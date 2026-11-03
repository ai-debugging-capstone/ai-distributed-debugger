"""PostgreSQL storage abstraction used by both OTLP and REST handlers."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .config import Settings
from .models import (
    RESERVED_EXPERIMENT_RUN_ID,
    ExperimentRunRequest,
    ExperimentRunUpdate,
    FaultInjectionRequest,
    FeedbackRequest,
    GroundTruthLabelRequest,
)

LOGGER = logging.getLogger(__name__)


class StorageError(RuntimeError):
    """Raised when a storage operation cannot be completed."""


class StorageValidationError(StorageError):
    """Raised when a write conflicts with persisted experiment state."""


@dataclass(slots=True)
class IngestResult:
    accepted: dict[str, int] = field(default_factory=dict)
    inserted: dict[str, int] = field(default_factory=dict)
    duplicates: dict[str, int] = field(default_factory=dict)
    rejected: int = 0


class Storage(Protocol):
    async def open(self) -> None: ...

    async def close(self) -> None: ...

    async def ping(self) -> bool: ...

    async def ingest(
        self,
        records: dict[str, list[dict[str, Any]]],
        rejected: list[dict[str, Any]],
    ) -> IngestResult: ...

    async def write_rejections(self, rejected: list[dict[str, Any]]) -> None: ...

    async def upsert_experiment(self, run: ExperimentRunRequest) -> dict[str, Any]: ...

    async def get_experiment(self, run_id: str) -> dict[str, Any] | None: ...

    async def get_experiment_capture_summary(self, run_id: str) -> dict[str, Any]: ...

    async def update_experiment(
        self, run_id: str, update: ExperimentRunUpdate
    ) -> dict[str, Any] | None: ...

    async def create_fault_injection(
        self, run_id: str, fault: FaultInjectionRequest
    ) -> dict[str, Any]: ...

    async def upsert_ground_truth_label(
        self, run_id: str, label: GroundTruthLabelRequest
    ) -> dict[str, Any]: ...

    async def list_incidents(
        self, *, status: str | None, limit: int, offset: int
    ) -> list[dict[str, Any]]: ...

    async def get_incident(self, incident_id: str) -> dict[str, Any] | None: ...

    async def get_candidates(self, incident_id: str) -> list[dict[str, Any]]: ...

    async def get_evidence(self, incident_id: str) -> list[dict[str, Any]]: ...

    async def get_recommendations(self, incident_id: str) -> list[dict[str, Any]]: ...

    async def get_graph(self, incident_id: str) -> dict[str, Any]: ...

    async def create_feedback(
        self, incident_id: str, feedback: FeedbackRequest
    ) -> dict[str, Any]: ...


SPAN_INSERT = """
    INSERT INTO spans (
        trace_id, span_id, parent_span_id, service_name, service_namespace,
        service_instance_id, operation_name, span_kind, start_time, end_time,
        duration_ms, status_code, status_message, attributes,
        resource_attributes, scope_name, scope_version, run_id, observed_at, raw
    ) VALUES (
        %(trace_id)s, %(span_id)s, %(parent_span_id)s, %(service_name)s,
        %(service_namespace)s, %(service_instance_id)s, %(operation_name)s,
        %(span_kind)s, %(start_time)s, %(end_time)s, %(duration_ms)s,
        %(status_code)s, %(status_message)s, %(attributes)s,
        %(resource_attributes)s, %(scope_name)s, %(scope_version)s,
        %(run_id)s, %(observed_at)s, %(raw)s
    ) ON CONFLICT (trace_id, span_id) DO NOTHING
"""

METRIC_INSERT = """
    INSERT INTO metric_points (
        idempotency_key, service_name, metric_name, metric_type, time_unix,
        start_time, value, count, sum, min, max, attributes,
        resource_attributes, exemplars, run_id, observed_at, raw
    ) VALUES (
        %(idempotency_key)s, %(service_name)s, %(metric_name)s, %(metric_type)s,
        %(time_unix)s, %(start_time)s, %(value)s, %(count)s, %(sum)s, %(min)s,
        %(max)s, %(attributes)s, %(resource_attributes)s, %(exemplars)s,
        %(run_id)s, %(observed_at)s, %(raw)s
    ) ON CONFLICT (idempotency_key) DO NOTHING
"""

LOG_INSERT = """
    INSERT INTO log_records (
        idempotency_key, service_name, time_unix, observed_time,
        severity_number, severity_text, body, trace_id, span_id, attributes,
        resource_attributes, run_id, raw
    ) VALUES (
        %(idempotency_key)s, %(service_name)s, %(time_unix)s,
        %(observed_time)s, %(severity_number)s, %(severity_text)s, %(body)s,
        %(trace_id)s, %(span_id)s, %(attributes)s, %(resource_attributes)s,
        %(run_id)s, %(raw)s
    ) ON CONFLICT (idempotency_key) DO NOTHING
"""

REJECT_INSERT = """
    INSERT INTO rejected_records (signal_type, reason, payload, received_at)
    VALUES (%(signal_type)s, %(reason)s, %(payload)s, %(received_at)s)
"""

# Telemetry can arrive before the experiment controller has persisted its full
# manifest (collector queues and restarts make that ordering normal).  Create a
# clearly marked placeholder so the raw-signal foreign keys never turn that race
# into data loss.  The experiment importer can enrich the same row later.
EXPERIMENT_RUN_PLACEHOLDER = """
    INSERT INTO experiment_runs (id, name, status, configuration)
    VALUES (%s, %s, 'running', '{"auto_registered":true}'::jsonb)
    ON CONFLICT (id) DO NOTHING
"""


JSON_COLUMNS = {
    "attributes",
    "resource_attributes",
    "exemplars",
    "raw",
    "payload",
    "metadata",
}


def _reject_reserved_run_id(run_id: object) -> None:
    if str(run_id) == str(RESERVED_EXPERIMENT_RUN_ID):
        raise StorageValidationError(
            "the reserved live-telemetry experiment cannot be mutated"
        )


def _adapt_json(record: dict[str, Any]) -> dict[str, Any]:
    return {
        key: Jsonb(value) if key in JSON_COLUMNS and value is not None else value
        for key, value in record.items()
    }


class PsycopgStorage:
    """Async, pooled PostgreSQL implementation.

    The API never creates or migrates schema at runtime.  Migrations are owned by
    the deployment, which makes startup deterministic and avoids multi-replica
    migration races.
    """

    def __init__(self, settings: Settings):
        self._settings = settings
        self._pool = AsyncConnectionPool(
            conninfo=settings.database_url,
            min_size=settings.database_min_pool_size,
            max_size=settings.database_max_pool_size,
            timeout=settings.database_connect_timeout_seconds,
            open=False,
            kwargs={
                "row_factory": dict_row,
                "connect_timeout": max(1, int(settings.database_connect_timeout_seconds)),
                "options": (
                    "-c statement_timeout="
                    f"{int(settings.database_command_timeout_seconds * 1000)}"
                ),
            },
        )

    async def open(self) -> None:
        try:
            await self._pool.open(
                wait=True, timeout=self._settings.database_connect_timeout_seconds
            )
        except Exception as exc:
            raise StorageError("could not connect to PostgreSQL") from exc

    async def close(self) -> None:
        await self._pool.close()

    async def ping(self) -> bool:
        try:
            async with self._pool.connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        """
                        SELECT ARRAY[
                            to_regclass('spans'),
                            to_regclass('metric_points'),
                            to_regclass('log_records'),
                            to_regclass('rejected_records'),
                            to_regclass('experiment_runs'),
                            to_regclass('fault_injections'),
                            to_regclass('ground_truth_labels'),
                            to_regclass('incidents'),
                            to_regclass('candidate_rankings'),
                            to_regclass('evidence_items')
                        ] AS required_tables,
                        COALESCE(
                            (
                                SELECT value::integer >= 4
                                FROM schema_metadata
                                WHERE key = 'schema_version'
                            ),
                            false
                        ) AS schema_current
                        """
                    )
                    row = await cursor.fetchone()
            return bool(
                row and all(row["required_tables"]) and row["schema_current"]
            )
        except Exception:
            LOGGER.exception("PostgreSQL readiness check failed")
            return False

    async def ingest(
        self,
        records: dict[str, list[dict[str, Any]]],
        rejected: list[dict[str, Any]],
    ) -> IngestResult:
        result = IngestResult(
            accepted={signal: len(records.get(signal, [])) for signal in ("span", "metric", "log")},
            inserted={"span": 0, "metric": 0, "log": 0},
            duplicates={"span": 0, "metric": 0, "log": 0},
            rejected=len(rejected),
        )
        statements = {"span": SPAN_INSERT, "metric": METRIC_INSERT, "log": LOG_INSERT}
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        run_ids = sorted(
                            {
                                str(row["run_id"])
                                for signal in ("span", "metric", "log")
                                for row in records.get(signal, [])
                                if row.get("run_id") is not None
                            }
                        )
                        if run_ids:
                            await cursor.executemany(
                                EXPERIMENT_RUN_PLACEHOLDER,
                                [(run_id, f"telemetry-{run_id}") for run_id in run_ids],
                            )
                        for signal, statement in statements.items():
                            batch = records.get(signal, [])
                            if not batch:
                                continue
                            await cursor.executemany(statement, [_adapt_json(row) for row in batch])
                            inserted = max(0, cursor.rowcount)
                            result.inserted[signal] = inserted
                            result.duplicates[signal] = len(batch) - inserted
                        if rejected:
                            await cursor.executemany(
                                REJECT_INSERT, [_adapt_json(row) for row in rejected]
                            )
            return result
        except Exception as exc:
            LOGGER.exception("Telemetry persistence failed")
            raise StorageError("telemetry could not be persisted") from exc

    async def write_rejections(self, rejected: list[dict[str, Any]]) -> None:
        if not rejected:
            return
        try:
            async with self._pool.connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.executemany(
                        REJECT_INSERT, [_adapt_json(row) for row in rejected]
                    )
                await connection.commit()
        except Exception as exc:
            LOGGER.exception("Rejected-record persistence failed")
            raise StorageError("rejected record could not be persisted") from exc

    async def upsert_experiment(self, run: ExperimentRunRequest) -> dict[str, Any]:
        _reject_reserved_run_id(run.id)
        values = run.model_dump(mode="python")
        # ``status`` has a useful create default, but that default must not
        # regress an already-running placeholder (or a completed run) when a
        # controller retries an otherwise partial/idempotent registration.
        # Pydantic retains whether the caller actually supplied the field.
        status_supplied = "status" in run.model_fields_set
        try:
            async with self._pool.connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        """
                        INSERT INTO experiment_runs (
                            id, name, source_repository, source_version, source_sha,
                            seed, workload, status, split, started_at, ended_at,
                            configuration, environment
                        ) VALUES (
                            %(id)s, %(name)s, %(source_repository)s,
                            %(source_version)s, %(source_sha)s, %(seed)s,
                            %(workload)s, %(status)s, %(split)s, %(started_at)s,
                            %(ended_at)s, %(configuration)s, %(environment)s
                        )
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            source_repository = COALESCE(
                                EXCLUDED.source_repository,
                                experiment_runs.source_repository
                            ),
                            source_version = COALESCE(
                                EXCLUDED.source_version,
                                experiment_runs.source_version
                            ),
                            source_sha = COALESCE(EXCLUDED.source_sha, experiment_runs.source_sha),
                            seed = COALESCE(EXCLUDED.seed, experiment_runs.seed),
                            workload = COALESCE(EXCLUDED.workload, experiment_runs.workload),
                            status = CASE
                                WHEN NOT %(status_supplied)s THEN experiment_runs.status
                                WHEN experiment_runs.status IN ('completed', 'failed')
                                    THEN experiment_runs.status
                                WHEN (
                                    CASE EXCLUDED.status
                                        WHEN 'created' THEN 0
                                        WHEN 'warming' THEN 1
                                        WHEN 'running' THEN 2
                                        WHEN 'recovering' THEN 3
                                        WHEN 'completed' THEN 4
                                        WHEN 'failed' THEN 4
                                    END
                                ) >= (
                                    CASE experiment_runs.status
                                        WHEN 'created' THEN 0
                                        WHEN 'warming' THEN 1
                                        WHEN 'running' THEN 2
                                        WHEN 'recovering' THEN 3
                                        WHEN 'completed' THEN 4
                                        WHEN 'failed' THEN 4
                                    END
                                ) THEN EXCLUDED.status
                                ELSE experiment_runs.status
                            END,
                            split = COALESCE(EXCLUDED.split, experiment_runs.split),
                            started_at = COALESCE(EXCLUDED.started_at, experiment_runs.started_at),
                            ended_at = COALESCE(EXCLUDED.ended_at, experiment_runs.ended_at),
                            configuration =
                                (experiment_runs.configuration - 'auto_registered')
                                || EXCLUDED.configuration,
                            environment = experiment_runs.environment || EXCLUDED.environment
                        WHERE (
                            COALESCE(EXCLUDED.ended_at, experiment_runs.ended_at) IS NULL
                            OR COALESCE(EXCLUDED.started_at, experiment_runs.started_at) IS NULL
                            OR COALESCE(EXCLUDED.ended_at, experiment_runs.ended_at)
                                >= COALESCE(EXCLUDED.started_at, experiment_runs.started_at)
                        )
                          AND (
                              experiment_runs.configuration ->> 'auto_registered' = 'true'
                              OR (
                                  experiment_runs.name = EXCLUDED.name
                                  AND (
                                      EXCLUDED.source_repository IS NULL
                                      OR experiment_runs.source_repository IS NULL
                                      OR experiment_runs.source_repository
                                          = EXCLUDED.source_repository
                                  )
                                  AND (
                                      EXCLUDED.source_version IS NULL
                                      OR experiment_runs.source_version IS NULL
                                      OR experiment_runs.source_version = EXCLUDED.source_version
                                  )
                                  AND (
                                      EXCLUDED.source_sha IS NULL
                                      OR experiment_runs.source_sha IS NULL
                                      OR experiment_runs.source_sha = EXCLUDED.source_sha
                                  )
                                  AND (
                                      EXCLUDED.seed IS NULL
                                      OR experiment_runs.seed IS NULL
                                      OR experiment_runs.seed = EXCLUDED.seed
                                  )
                                  AND (
                                      EXCLUDED.workload IS NULL
                                      OR experiment_runs.workload IS NULL
                                      OR experiment_runs.workload = EXCLUDED.workload
                                  )
                                  AND (
                                      EXCLUDED.split IS NULL
                                      OR experiment_runs.split IS NULL
                                      OR experiment_runs.split = EXCLUDED.split
                                  )
                                  AND (
                                      EXCLUDED.started_at IS NULL
                                      OR experiment_runs.started_at IS NULL
                                      OR experiment_runs.started_at = EXCLUDED.started_at
                                  )
                                  AND (
                                      EXCLUDED.ended_at IS NULL
                                      OR experiment_runs.ended_at IS NULL
                                      OR experiment_runs.ended_at = EXCLUDED.ended_at
                                  )
                                  AND (
                                      experiment_runs.configuration || EXCLUDED.configuration
                                  ) @> experiment_runs.configuration
                                  AND (
                                      experiment_runs.environment || EXCLUDED.environment
                                  ) @> experiment_runs.environment
                              )
                          )
                          AND (
                              experiment_runs.status NOT IN ('completed', 'failed')
                              OR (
                                  experiment_runs.name = EXCLUDED.name
                                  AND (
                                      EXCLUDED.source_repository IS NULL
                                      OR experiment_runs.source_repository
                                          IS NOT DISTINCT FROM EXCLUDED.source_repository
                                  )
                                  AND (
                                      EXCLUDED.source_version IS NULL
                                      OR experiment_runs.source_version
                                          IS NOT DISTINCT FROM EXCLUDED.source_version
                                  )
                                  AND (
                                      EXCLUDED.source_sha IS NULL
                                      OR experiment_runs.source_sha
                                          IS NOT DISTINCT FROM EXCLUDED.source_sha
                                  )
                                  AND (
                                      EXCLUDED.seed IS NULL
                                      OR experiment_runs.seed IS NOT DISTINCT FROM EXCLUDED.seed
                                  )
                                  AND (
                                      EXCLUDED.workload IS NULL
                                      OR experiment_runs.workload
                                          IS NOT DISTINCT FROM EXCLUDED.workload
                                  )
                                  AND (
                                      EXCLUDED.split IS NULL
                                      OR experiment_runs.split IS NOT DISTINCT FROM EXCLUDED.split
                                  )
                                  AND (
                                      EXCLUDED.started_at IS NULL
                                      OR experiment_runs.started_at
                                          IS NOT DISTINCT FROM EXCLUDED.started_at
                                  )
                                  AND (
                                      EXCLUDED.ended_at IS NULL
                                      OR experiment_runs.ended_at
                                          IS NOT DISTINCT FROM EXCLUDED.ended_at
                                  )
                                  AND experiment_runs.configuration @> EXCLUDED.configuration
                                  AND experiment_runs.environment @> EXCLUDED.environment
                              )
                          )
                        RETURNING *
                        """,
                        {
                            **values,
                            "status_supplied": status_supplied,
                            "configuration": Jsonb(values["configuration"]),
                            "environment": Jsonb(values["environment"]),
                        },
                    )
                    row = await cursor.fetchone()
                await connection.commit()
            if row is None:
                raise StorageValidationError(
                    "experiment conflicts with the persisted immutable or terminal run state"
                )
            return row
        except StorageError:
            raise
        except Exception as exc:
            LOGGER.exception("Experiment persistence failed")
            raise StorageError("experiment could not be persisted") from exc

    async def get_experiment(self, run_id: str) -> dict[str, Any] | None:
        rows = await self._fetch_all(
            "SELECT * FROM experiment_runs WHERE id::text = %s LIMIT 1", (run_id,)
        )
        return rows[0] if rows else None

    async def get_experiment_capture_summary(self, run_id: str) -> dict[str, Any]:
        """Return the persisted signals and RCA output scoped to one capture.

        This deliberately reads normalized rows, rather than gateway counters,
        so the experiment runner only accepts telemetry that PostgreSQL can
        actually export and train from.
        """

        count_rows = await self._fetch_all(
            """
            SELECT
                (SELECT count(*) FROM spans WHERE run_id = %s::uuid) AS spans,
                (SELECT count(*) FROM metric_points WHERE run_id = %s::uuid)
                    AS metric_points,
                (SELECT count(*) FROM log_records WHERE run_id = %s::uuid)
                    AS log_records,
                (SELECT count(*) FROM feature_buckets WHERE run_id = %s::uuid)
                    AS feature_buckets,
                (SELECT count(*) FROM dependency_edges WHERE run_id = %s::uuid)
                    AS dependency_edges
            """,
            (run_id, run_id, run_id, run_id, run_id),
        )
        counts = count_rows[0]
        incidents = await self._fetch_all(
            """
            SELECT id, status, started_at, detected_at, ended_at, severity,
                   summary, trigger_entity, max_anomaly, affected_entities,
                   model_version
            FROM incidents
            WHERE run_id = %s::uuid
            ORDER BY started_at ASC, id ASC
            """,
            (run_id,),
        )
        candidates = await self._fetch_all(
            """
            SELECT cr.incident_id, cr.rank, cr.entity_type, cr.entity_id,
                   cr.score, cr.probability, cr.model_version
            FROM candidate_rankings AS cr
            JOIN incidents AS i ON i.id = cr.incident_id
            WHERE i.run_id = %s::uuid
            ORDER BY i.started_at ASC, cr.rank ASC, cr.score DESC
            """,
            (run_id,),
        )
        return {
            "run_id": run_id,
            "telemetry": {
                "spans": int(counts["spans"]),
                "metric_points": int(counts["metric_points"]),
                "log_records": int(counts["log_records"]),
            },
            "derived": {
                "feature_buckets": int(counts["feature_buckets"]),
                "dependency_edges": int(counts["dependency_edges"]),
            },
            "incidents": incidents,
            "candidates": candidates,
        }

    async def update_experiment(
        self, run_id: str, update: ExperimentRunUpdate
    ) -> dict[str, Any] | None:
        _reject_reserved_run_id(run_id)
        values = update.model_dump(mode="python")
        try:
            async with self._pool.connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        """
                        WITH patch AS (
                            SELECT
                                %(status)s::text AS status,
                                %(split)s::text AS split,
                                %(workload)s::text AS workload,
                                %(started_at)s::timestamptz AS started_at,
                                %(ended_at)s::timestamptz AS ended_at,
                                %(configuration)s::jsonb AS configuration,
                                %(environment)s::jsonb AS environment
                        )
                        UPDATE experiment_runs SET
                            status = CASE
                                WHEN patch.status IS NULL THEN experiment_runs.status
                                WHEN experiment_runs.status IN ('completed', 'failed')
                                    THEN experiment_runs.status
                                WHEN (
                                    CASE patch.status
                                        WHEN 'created' THEN 0
                                        WHEN 'warming' THEN 1
                                        WHEN 'running' THEN 2
                                        WHEN 'recovering' THEN 3
                                        WHEN 'completed' THEN 4
                                        WHEN 'failed' THEN 4
                                    END
                                ) >= (
                                    CASE experiment_runs.status
                                        WHEN 'created' THEN 0
                                        WHEN 'warming' THEN 1
                                        WHEN 'running' THEN 2
                                        WHEN 'recovering' THEN 3
                                        WHEN 'completed' THEN 4
                                        WHEN 'failed' THEN 4
                                    END
                                ) THEN patch.status
                                ELSE experiment_runs.status
                            END,
                            split = COALESCE(patch.split, experiment_runs.split),
                            workload = COALESCE(patch.workload, experiment_runs.workload),
                            started_at = COALESCE(
                                patch.started_at, experiment_runs.started_at
                            ),
                            ended_at = COALESCE(patch.ended_at, experiment_runs.ended_at),
                            configuration =
                                experiment_runs.configuration || patch.configuration,
                            environment = experiment_runs.environment || patch.environment
                        FROM patch
                        WHERE experiment_runs.id = %(run_id)s::uuid
                          AND (
                              COALESCE(patch.ended_at, experiment_runs.ended_at) IS NULL
                              OR COALESCE(
                                  patch.started_at, experiment_runs.started_at
                              ) IS NULL
                              OR COALESCE(patch.ended_at, experiment_runs.ended_at)
                                  >= COALESCE(
                                      patch.started_at, experiment_runs.started_at
                                  )
                          )
                          AND (
                              patch.split IS NULL
                              OR experiment_runs.split IS NULL
                              OR experiment_runs.split = patch.split
                          )
                          AND (
                              patch.workload IS NULL
                              OR experiment_runs.workload IS NULL
                              OR experiment_runs.workload = patch.workload
                          )
                          AND (
                              patch.started_at IS NULL
                              OR experiment_runs.started_at IS NULL
                              OR experiment_runs.started_at = patch.started_at
                          )
                          AND (
                              patch.ended_at IS NULL
                              OR experiment_runs.ended_at IS NULL
                              OR experiment_runs.ended_at = patch.ended_at
                          )
                          AND (
                              experiment_runs.configuration || patch.configuration
                          ) @> experiment_runs.configuration
                          AND (
                              experiment_runs.environment || patch.environment
                          ) @> experiment_runs.environment
                          AND (
                              experiment_runs.status NOT IN ('completed', 'failed')
                              OR (
                                  (
                                      patch.split IS NULL
                                      OR experiment_runs.split
                                          IS NOT DISTINCT FROM patch.split
                                  )
                                  AND (
                                      patch.workload IS NULL
                                      OR experiment_runs.workload
                                          IS NOT DISTINCT FROM patch.workload
                                  )
                                  AND (
                                      patch.started_at IS NULL
                                      OR experiment_runs.started_at
                                          IS NOT DISTINCT FROM patch.started_at
                                  )
                                  AND (
                                      patch.ended_at IS NULL
                                      OR experiment_runs.ended_at
                                          IS NOT DISTINCT FROM patch.ended_at
                                  )
                                  AND experiment_runs.configuration
                                      @> patch.configuration
                                  AND experiment_runs.environment @> patch.environment
                              )
                          )
                        RETURNING experiment_runs.*
                        """,
                        {
                            **values,
                            "run_id": run_id,
                            "configuration": Jsonb(values.get("configuration") or {}),
                            "environment": Jsonb(values.get("environment") or {}),
                        },
                    )
                    row = await cursor.fetchone()
                    if row is None:
                        await cursor.execute(
                            "SELECT 1 FROM experiment_runs WHERE id::text = %s",
                            (run_id,),
                        )
                        if await cursor.fetchone() is not None:
                            raise StorageValidationError(
                                "experiment update conflicts with immutable or terminal run state"
                            )
                await connection.commit()
            return row
        except StorageError:
            raise
        except Exception as exc:
            LOGGER.exception("Experiment update failed")
            raise StorageError("experiment could not be updated") from exc

    async def create_fault_injection(
        self, run_id: str, fault: FaultInjectionRequest
    ) -> dict[str, Any]:
        _reject_reserved_run_id(run_id)
        values = fault.model_dump(mode="python")
        try:
            async with self._pool.connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        """
                        INSERT INTO fault_injections (
                            id, run_id, flag_name, variant, injector,
                            causal_entity_type, causal_entity_id,
                            causal_edge_source, causal_edge_target, mechanism,
                            impacted_sli, injected_at, cleared_at,
                            expected_runbook_key, metadata
                        ) VALUES (
                            %(id)s, %(run_id)s::uuid, %(flag_name)s, %(variant)s,
                            %(injector)s, %(causal_entity_type)s,
                            %(causal_entity_id)s, %(causal_edge_source)s,
                            %(causal_edge_target)s, %(mechanism)s,
                            %(impacted_sli)s, %(injected_at)s, %(cleared_at)s,
                            %(expected_runbook_key)s, %(metadata)s
                        )
                        ON CONFLICT (id) DO UPDATE SET
                            cleared_at = COALESCE(
                                EXCLUDED.cleared_at,
                                fault_injections.cleared_at
                            ),
                            metadata = fault_injections.metadata || EXCLUDED.metadata
                        WHERE fault_injections.run_id = EXCLUDED.run_id
                          AND fault_injections.flag_name = EXCLUDED.flag_name
                          AND fault_injections.variant IS NOT DISTINCT FROM EXCLUDED.variant
                          AND fault_injections.injector = EXCLUDED.injector
                          AND fault_injections.causal_entity_type = EXCLUDED.causal_entity_type
                          AND fault_injections.causal_entity_id = EXCLUDED.causal_entity_id
                          AND fault_injections.causal_edge_source
                              IS NOT DISTINCT FROM EXCLUDED.causal_edge_source
                          AND fault_injections.causal_edge_target
                              IS NOT DISTINCT FROM EXCLUDED.causal_edge_target
                          AND fault_injections.mechanism = EXCLUDED.mechanism
                          AND fault_injections.impacted_sli = EXCLUDED.impacted_sli
                          AND fault_injections.expected_runbook_key
                              IS NOT DISTINCT FROM EXCLUDED.expected_runbook_key
                          AND fault_injections.injected_at = EXCLUDED.injected_at
                          AND (
                              EXCLUDED.cleared_at IS NULL
                              OR fault_injections.cleared_at IS NULL
                              OR EXCLUDED.cleared_at >= fault_injections.cleared_at
                          )
                        RETURNING *
                        """,
                        {**values, "run_id": run_id, "metadata": Jsonb(values["metadata"])},
                    )
                    row = await cursor.fetchone()
                await connection.commit()
            if row is None:
                raise StorageValidationError(
                    "fault injection id conflicts with an existing fault definition"
                )
            return row
        except StorageError:
            raise
        except Exception as exc:
            LOGGER.exception("Fault-injection persistence failed")
            raise StorageError("fault injection could not be persisted") from exc

    async def upsert_ground_truth_label(
        self, run_id: str, label: GroundTruthLabelRequest
    ) -> dict[str, Any]:
        _reject_reserved_run_id(run_id)
        values = label.model_dump(mode="python")
        try:
            async with self._pool.connection() as connection:
                async with connection.cursor() as cursor:
                    if values.get("incident_id") is not None:
                        await cursor.execute(
                            """
                            SELECT 1 FROM incidents
                            WHERE id = %(incident_id)s AND run_id = %(run_id)s::uuid
                            """,
                            {**values, "run_id": run_id},
                        )
                        if await cursor.fetchone() is None:
                            raise StorageValidationError(
                                "ground-truth incident does not belong to the experiment"
                            )
                    await cursor.execute(
                        """
                        INSERT INTO ground_truth_labels (
                            run_id, incident_id, entity_type, entity_id,
                            fault_class, relevance, is_primary
                        ) VALUES (
                            %(run_id)s::uuid, %(incident_id)s,
                            %(entity_type)s, %(entity_id)s, %(fault_class)s,
                            %(relevance)s, %(is_primary)s
                        )
                        ON CONFLICT (
                            run_id, incident_id, entity_type, entity_id, fault_class
                        )
                        DO UPDATE SET
                            relevance = EXCLUDED.relevance,
                            is_primary = EXCLUDED.is_primary
                        RETURNING *
                        """,
                        {**values, "run_id": run_id},
                    )
                    row = await cursor.fetchone()
                await connection.commit()
            if row is None:  # pragma: no cover
                raise StorageError("ground-truth upsert returned no row")
            return row
        except StorageError:
            raise
        except Exception as exc:
            LOGGER.exception("Ground-truth persistence failed")
            raise StorageError("ground-truth label could not be persisted") from exc

    async def list_incidents(
        self, *, status: str | None, limit: int, offset: int
    ) -> list[dict[str, Any]]:
        if status:
            query = "SELECT * FROM incidents WHERE status = %s ORDER BY started_at DESC LIMIT %s OFFSET %s"
            parameters: tuple[Any, ...] = (status, limit, offset)
        else:
            query = "SELECT * FROM incidents ORDER BY started_at DESC LIMIT %s OFFSET %s"
            parameters = (limit, offset)
        return await self._fetch_all(query, parameters)

    async def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        rows = await self._fetch_all(
            "SELECT * FROM incidents WHERE id::text = %s LIMIT 1", (incident_id,)
        )
        return rows[0] if rows else None

    async def get_candidates(self, incident_id: str) -> list[dict[str, Any]]:
        return await self._fetch_all(
            """
            SELECT * FROM candidate_rankings
            WHERE incident_id::text = %s
            ORDER BY rank ASC, score DESC
            """,
            (incident_id,),
        )

    async def get_evidence(self, incident_id: str) -> list[dict[str, Any]]:
        return await self._fetch_all(
            """
            SELECT * FROM evidence_items
            WHERE incident_id::text = %s
            ORDER BY id ASC
            """,
            (incident_id,),
        )

    async def get_recommendations(self, incident_id: str) -> list[dict[str, Any]]:
        return await self._fetch_all(
            """
            SELECT
                r.id,
                r.incident_id,
                r.candidate_id,
                r.compatibility_score,
                r.rationale,
                cr.rank AS candidate_rank,
                cr.entity_type AS candidate_type,
                cr.entity_id AS candidate_entity,
                rb.runbook_key,
                rb.title,
                rb.description,
                rb.diagnostic_steps,
                rb.remediation_steps,
                rb.verification_steps,
                rb.rollback_steps,
                rb.version AS runbook_version
            FROM recommendations AS r
            JOIN runbooks AS rb ON rb.id = r.runbook_id
            LEFT JOIN candidate_rankings AS cr ON cr.id = r.candidate_id
            WHERE r.incident_id::text = %s
            ORDER BY cr.rank NULLS LAST, r.compatibility_score DESC NULLS LAST, r.id
            """,
            (incident_id,),
        )

    async def _table_columns(self, table_name: str) -> set[str]:
        rows = await self._fetch_all(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            """,
            (table_name,),
        )
        return {row["column_name"] for row in rows}

    async def get_graph(self, incident_id: str) -> dict[str, Any]:
        columns = await self._table_columns("dependency_edges")
        if "incident_id" in columns:
            edges = await self._fetch_all(
                "SELECT * FROM dependency_edges WHERE incident_id::text = %s ORDER BY id ASC",
                (incident_id,),
            )
            graph_scope = "incident"
        elif {"run_id", "bucket_start"}.issubset(columns):
            # The canonical schema stores time-bucketed topology globally. Scope
            # it to the incident's run and time window without requiring a second
            # denormalized incident_id column.
            edges = await self._fetch_all(
                """
                SELECT de.*
                FROM dependency_edges AS de
                JOIN incidents AS i ON i.id::text = %s
                WHERE de.run_id IS NOT DISTINCT FROM i.run_id
                  AND de.bucket_start >= i.started_at - interval '2 minutes'
                  AND de.bucket_start <= COALESCE(i.ended_at, now()) + interval '2 minutes'
                ORDER BY de.bucket_start ASC, de.id ASC
                LIMIT 2000
                """,
                (incident_id,),
            )
            graph_scope = "incident_window"
        else:
            # A global live-topology table is also supported. The response marks
            # this explicitly so clients do not mistake it for incident-scoped data.
            edges = await self._fetch_all(
                "SELECT * FROM dependency_edges ORDER BY id DESC LIMIT 2000", ()
            )
            graph_scope = "current_topology"

        source_key = next(
            (key for key in ("source_service", "source_service_name", "source") if key in columns),
            None,
        )
        target_key = next(
            (key for key in ("target_service", "target_service_name", "target") if key in columns),
            None,
        )
        node_names: set[str] = set()
        if source_key and target_key:
            for edge in edges:
                if edge.get(source_key):
                    node_names.add(str(edge[source_key]))
                if edge.get(target_key):
                    node_names.add(str(edge[target_key]))
        return {
            "incident_id": incident_id,
            "scope": graph_scope,
            "nodes": [{"id": name, "label": name} for name in sorted(node_names)],
            "edges": edges,
        }

    async def create_feedback(
        self, incident_id: str, feedback: FeedbackRequest
    ) -> dict[str, Any]:
        values = feedback.model_dump(mode="python")
        try:
            async with self._pool.connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        """
                        INSERT INTO developer_feedback (
                            incident_id, candidate_id, verdict, comment,
                            user_name, metadata
                        ) VALUES (
                            %s::uuid, %s::bigint, %s, %s, %s, %s
                        ) RETURNING *
                        """,
                        (
                            incident_id,
                            values["candidate_id"],
                            values["verdict"],
                            values["comment"],
                            values["user_name"],
                            Jsonb(values["metadata"]),
                        ),
                    )
                    row = await cursor.fetchone()
                await connection.commit()
            if row is None:  # pragma: no cover - PostgreSQL RETURNING always returns.
                raise StorageError("feedback insert returned no row")
            return row
        except StorageError:
            raise
        except Exception as exc:
            LOGGER.exception("Feedback persistence failed")
            raise StorageError("feedback could not be persisted") from exc

    async def _fetch_all(
        self, query: str, parameters: tuple[Any, ...]
    ) -> list[dict[str, Any]]:
        try:
            async with self._pool.connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query, parameters)
                    rows = await cursor.fetchall()
            return list(rows)
        except Exception as exc:
            LOGGER.exception("PostgreSQL query failed")
            raise StorageError("query could not be completed") from exc
