"""PostgreSQL polling worker for live feature, incident, and RCA materialization."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import groupby
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from .domain import AnomalyResult, DependencyEdge, FeatureBucket, Incident
from .features import StreamingAnomalyScorer, aggregate_feature_buckets, floor_time
from .graph import DirectedDependencyGraph, build_dependency_edges
from .incidents import IncidentDetector, IncidentEvent
from .online_ranking import (
    ChampionRerankingRootCauseRanker,
    load_champion_artifact,
    online_candidate_record,
)
from .ranking import HeuristicRootCauseRanker

LOGGER = logging.getLogger("rca.worker")
TERMINAL_RUN_STATUSES = frozenset({"completed", "failed"})


@dataclass(frozen=True)
class RunLifecycle:
    status: str
    ended_at: datetime | None = None


def incident_processing_mode(
    run_id: str | None,
    lifecycle: RunLifecycle | None,
    *,
    has_active_incident: bool,
) -> str:
    """Return ``detect``, ``recover``, or ``observe`` for one run bucket.

    A missing run id is legacy/unscoped live telemetry and retains the former
    always-on behavior.  A non-null id without a matching lifecycle fails
    closed: PostgreSQL normally prevents that state through the foreign key,
    but opening an incident is the unsafe fallback if integrity is ever lost.
    """

    if run_id is None:
        return "detect"
    if lifecycle is None:
        return "observe"
    if lifecycle.status == "running":
        return "detect"
    if has_active_incident and lifecycle.status in {"recovering", *TERMINAL_RUN_STATUSES}:
        return "recover"
    return "observe"


@dataclass(frozen=True)
class WorkerSettings:
    database_url: str
    poll_seconds: float = 5.0
    bucket_seconds: int = 10
    lookback_buckets: int = 30
    min_history_buckets: int = 10
    min_incident_buckets: int = 2
    model_path: str | None = None

    @classmethod
    def from_env(cls) -> WorkerSettings:
        database_url = os.environ.get("DATABASE_URL", "")
        return cls(
            database_url=database_url,
            poll_seconds=float(os.environ.get("RCA_POLL_SECONDS", "5")),
            bucket_seconds=int(os.environ.get("RCA_BUCKET_SECONDS", "10")),
            lookback_buckets=int(os.environ.get("RCA_LOOKBACK_BUCKETS", "30")),
            min_history_buckets=int(os.environ.get("RCA_MIN_HISTORY_BUCKETS", "10")),
            min_incident_buckets=int(os.environ.get("RCA_MIN_INCIDENT_BUCKETS", "2")),
            model_path=os.environ.get("RCA_MODEL_PATH") or None,
        )


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)


def is_unprocessed_bucket(
    run_id: str | None,
    bucket_start: datetime,
    watermarks: Mapping[str | None, datetime],
) -> bool:
    """Per-run watermark check; a new historical run must never be skipped."""

    return run_id not in watermarks or bucket_start > watermarks[run_id]


class RCAWorker:
    def __init__(self, settings: WorkerSettings) -> None:
        if not settings.database_url:
            raise ValueError("DATABASE_URL or --database-url is required")
        if settings.lookback_buckets < settings.min_history_buckets:
            raise ValueError("lookback must be at least min_history")
        self.settings = settings
        self.scorer = StreamingAnomalyScorer(
            lookback=settings.lookback_buckets,
            min_history=settings.min_history_buckets,
        )
        self.detector = IncidentDetector(min_consecutive=settings.min_incident_buckets)
        baseline_ranker = HeuristicRootCauseRanker()
        champion = load_champion_artifact(settings.model_path, logger=LOGGER)
        self.ranker = (
            ChampionRerankingRootCauseRanker(
                champion,
                baseline=baseline_ranker,
                logger=LOGGER,
            )
            if champion is not None
            else baseline_ranker
        )
        self._primed = False
        self._recent_anomalies: dict[str | None, list[AnomalyResult]] = defaultdict(list)
        self._recent_edges: dict[str | None, list[DependencyEdge]] = defaultdict(list)
        self._stopping = False

    @staticmethod
    def _connect(database_url: str):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - depends on deployment extra
            raise RuntimeError("PostgreSQL worker requires psycopg; install requirements-worker.txt") from exc
        return psycopg.connect(database_url, row_factory=dict_row)

    def check(self) -> None:
        with self._connect(self.settings.database_url) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1 AS ok")
            row = cursor.fetchone()
            if not row or row["ok"] != 1:
                raise RuntimeError("database readiness check failed")

    def stop(self, *_: Any) -> None:
        self._stopping = True

    def run(self, *, once: bool = False) -> None:
        while not self._stopping:
            try:
                processed = self.run_once()
                LOGGER.info("RCA cycle complete: %d closed buckets processed", processed)
            except Exception:
                LOGGER.exception("RCA worker cycle failed")
                if once:
                    raise
            if once:
                return
            end = time.monotonic() + self.settings.poll_seconds
            while not self._stopping and time.monotonic() < end:
                time.sleep(min(0.25, end - time.monotonic()))

    def run_once(self) -> int:
        with self._connect(self.settings.database_url) as connection:
            # Keep watermark discovery and raw-data fetches on one snapshot.  Under
            # READ COMMITTED, a historical run committed between _raw_start() and
            # _fetch_raw() can be seen only through the already-selected live-data
            # window, causing its newest bucket to become a watermark that skips
            # every older bucket permanently.
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            # Avoid two replicas materializing the same interval concurrently.
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_xact_lock(%s) AS acquired", (0x52434131,))
                if not cursor.fetchone()["acquired"]:
                    LOGGER.info("another RCA worker owns the advisory lock")
                    return 0
            watermarks = self._watermarks(connection)
            if not self._primed:
                self._prime_history(connection)
                self._primed = True
            self._resolve_terminal_active_incidents(connection)
            cutoff = floor_time(datetime.now(timezone.utc), self.settings.bucket_seconds)
            start = self._raw_start(connection, watermarks)
            if start is None or start >= cutoff:
                connection.commit()
                return 0
            spans, metrics, logs = self._fetch_raw(connection, start, cutoff)
            buckets = aggregate_feature_buckets(
                spans, metrics, logs, bucket_seconds=self.settings.bucket_seconds
            )
            edges = build_dependency_edges(spans, bucket_seconds=self.settings.bucket_seconds)
            buckets = [
                bucket
                for bucket in buckets
                if is_unprocessed_bucket(bucket.run_id, bucket.bucket_start, watermarks)
            ]
            edges = [
                edge
                for edge in edges
                if is_unprocessed_bucket(edge.run_id, edge.bucket_start, watermarks)
            ]
            for edge in edges:
                self._write_edge(connection, edge)
            edges_by_bucket: dict[
                tuple[str | None, datetime], list[DependencyEdge]
            ] = defaultdict(list)
            for edge in edges:
                edges_by_bucket[(edge.run_id, edge.bucket_start)].append(edge)
            lifecycles = self._run_lifecycles(
                connection, {bucket.run_id for bucket in buckets if bucket.run_id is not None}
            )
            processed_times: set[datetime] = set()
            ordered = sorted(buckets, key=lambda item: (item.run_id or "", item.bucket_start, item.entity_id))
            for (run_id, bucket_start), grouped in groupby(ordered, key=lambda item: (item.run_id, item.bucket_start)):
                current = list(grouped)
                graph = self._dependency_graph_at(
                    run_id,
                    bucket_start,
                    edges_by_bucket.get((run_id, bucket_start), ()),
                )
                results = [self.scorer.score(bucket) for bucket in current]
                processed_times.add(bucket_start)
                for result in results:
                    self._write_feature(connection, result)
                lifecycle = lifecycles.get(run_id)
                mode, events = self._incident_events_for_lifecycle(results, lifecycle)
                for event in events:
                    incident = self._persist_incident(connection, event)
                    if mode == "detect" and event.action in {"opened", "updated"}:
                        decision = self.ranker.rank(
                            incident,
                            self._recent_anomalies[run_id],
                            graph,
                        )
                        self._replace_rankings(connection, incident, decision.rankings)
            connection.commit()
            return len(processed_times)

    def _dependency_graph_at(
        self,
        run_id: str | None,
        bucket_start: datetime,
        current_edges: Iterable[DependencyEdge],
    ) -> DirectedDependencyGraph:
        """Build a causal, rolling graph without future-bucket leakage."""

        window_start = bucket_start - timedelta(
            seconds=self.settings.lookback_buckets * self.settings.bucket_seconds
        )
        recent = [
            edge
            for edge in self._recent_edges[run_id]
            if window_start <= edge.bucket_start <= bucket_start
        ]
        recent.extend(
            edge
            for edge in current_edges
            if edge.run_id == run_id and edge.bucket_start == bucket_start
        )
        self._recent_edges[run_id] = recent
        return DirectedDependencyGraph(recent)

    def _incident_events_for_lifecycle(
        self,
        results: Sequence[AnomalyResult],
        lifecycle: RunLifecycle | None,
    ) -> tuple[str, list[IncidentEvent]]:
        """Apply lifecycle policy after feature scoring but before persistence."""

        if not results:
            return "observe", []
        run_id = results[0].bucket.run_id
        bucket_start = results[0].bucket.bucket_start
        mode = incident_processing_mode(
            run_id,
            lifecycle,
            has_active_incident=self.detector.has_active_incident(run_id),
        )
        if mode == "observe":
            # Warm-up data is intentionally scored and persisted so it trains
            # the rolling baseline, but it must never leak into the evidence
            # window used after fault activation.
            return mode, []
        if mode == "detect":
            recent = self._recent_anomalies[run_id]
            recent.extend(results)
            window_start = bucket_start - timedelta(
                seconds=self.settings.lookback_buckets * self.settings.bucket_seconds
            )
            self._recent_anomalies[run_id] = [
                result
                for result in recent
                if result.bucket.bucket_start >= window_start
            ]
            return mode, self.detector.update(results)
        # Recovery telemetry may close the incident, but cannot create one or
        # add unrelated late anomalies to its scope.
        return mode, self.detector.update(
            results, allow_open=False, allow_growth=False
        )

    def _run_lifecycles(
        self, connection: Any, run_ids: Iterable[str]
    ) -> dict[str, RunLifecycle]:
        ids = sorted(set(run_ids))
        if not ids:
            return {}
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT id::text AS run_id, status, ended_at
                   FROM experiment_runs
                   WHERE id = ANY(%s::uuid[])""",
                (ids,),
            )
            return {
                str(row["run_id"]): RunLifecycle(
                    status=str(row["status"]), ended_at=row.get("ended_at")
                )
                for row in cursor.fetchall()
            }

    def _resolve_terminal_active_incidents(self, connection: Any) -> None:
        active = self.detector.active_incidents
        run_ids = {incident.run_id for incident in active if incident.run_id is not None}
        lifecycles = self._run_lifecycles(connection, run_ids)
        now = datetime.now(timezone.utc)
        for incident in active:
            lifecycle = lifecycles.get(incident.run_id) if incident.run_id is not None else None
            if lifecycle is None or lifecycle.status not in TERMINAL_RUN_STATUSES:
                continue
            event = self.detector.resolve_active(
                incident.run_id, lifecycle.ended_at or now
            )
            if event is not None:
                self._persist_incident(connection, event)

    def _watermarks(self, connection: Any) -> dict[str | None, datetime]:
        with connection.cursor() as cursor:
            cursor.execute("SELECT run_id, MAX(bucket_start) AS watermark FROM feature_buckets GROUP BY run_id")
            return {
                str(row["run_id"]) if row.get("run_id") else None: row["watermark"]
                for row in cursor.fetchall()
            }

    def _raw_start(
        self, connection: Any, watermarks: Mapping[str | None, datetime]
    ) -> datetime | None:
        query = """
            WITH watermarks AS (
              SELECT run_id, MAX(bucket_start) AS watermark
              FROM feature_buckets GROUP BY run_id
            )
            SELECT MIN(next_ts) AS start_at FROM (
              SELECT MIN(s.start_time) AS next_ts
              FROM spans s LEFT JOIN watermarks w ON w.run_id IS NOT DISTINCT FROM s.run_id
              WHERE w.watermark IS NULL OR s.start_time >= w.watermark + %s
              UNION ALL
              SELECT MIN(COALESCE(m.time_unix, m.observed_at))
              FROM metric_points m LEFT JOIN watermarks w ON w.run_id IS NOT DISTINCT FROM m.run_id
              WHERE w.watermark IS NULL OR COALESCE(m.time_unix, m.observed_at) >= w.watermark + %s
              UNION ALL
              SELECT MIN(COALESCE(l.time_unix, l.observed_time))
              FROM log_records l LEFT JOIN watermarks w ON w.run_id IS NOT DISTINCT FROM l.run_id
              WHERE w.watermark IS NULL OR COALESCE(l.time_unix, l.observed_time) >= w.watermark + %s
            ) starts
        """
        step = timedelta(seconds=self.settings.bucket_seconds)
        with connection.cursor() as cursor:
            cursor.execute(query, (step, step, step))
            value = cursor.fetchone()["start_at"]
        return floor_time(value, self.settings.bucket_seconds) if value else None

    def _prime_history(self, connection: Any) -> None:
        query = """
            WITH ranked AS (
              SELECT bucket_start, entity_type, entity_id, run_id,
                   request_count, error_count, error_rate,
                   latency_ms_p50, latency_ms_p95, latency_ms_p99,
                   cpu_percent, memory_bytes, log_error_count,
                   features, missingness,
                   ROW_NUMBER() OVER (
                     PARTITION BY run_id, entity_type, entity_id ORDER BY bucket_start DESC
                   ) AS history_rank
              FROM feature_buckets
            )
            SELECT * FROM ranked WHERE history_rank <= %s
            ORDER BY run_id NULLS FIRST, bucket_start, entity_id
        """
        with connection.cursor() as cursor:
            cursor.execute(query, (self.settings.lookback_buckets,))
            for row in cursor.fetchall():
                features = dict(row.get("features") or {})
                for name in (
                    "request_count", "error_count", "error_rate", "latency_ms_p50",
                    "latency_ms_p95", "latency_ms_p99", "cpu_percent", "memory_bytes",
                    "log_error_count",
                ):
                    if row.get(name) is not None:
                        features[name] = float(row[name])
                bucket = FeatureBucket(
                    bucket_start=row["bucket_start"],
                    bucket_end=row["bucket_start"] + timedelta(seconds=self.settings.bucket_seconds),
                    entity_type=row["entity_type"],
                    entity_id=row["entity_id"],
                    run_id=str(row["run_id"]) if row.get("run_id") else None,
                    features=features,
                    missingness=dict(row.get("missingness") or {}),
                )
                self.scorer.score(bucket)

    def _fetch_raw(
        self, connection: Any, start: datetime, cutoff: datetime
    ) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]], list[Mapping[str, Any]]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT trace_id,span_id,parent_span_id,service_name,operation_name,span_kind,
                          start_time,end_time,duration_ms,status_code,attributes,resource_attributes,
                          run_id,observed_at
                   FROM spans WHERE start_time >= %s AND start_time < %s""",
                (start, cutoff),
            )
            spans = cursor.fetchall()
            cursor.execute(
                """SELECT service_name,metric_name,metric_type,time_unix,value,count,sum,min,max,
                          attributes,resource_attributes,run_id,observed_at
                   FROM metric_points
                   WHERE COALESCE(time_unix, observed_at) >= %s
                     AND COALESCE(time_unix, observed_at) < %s""",
                (start, cutoff),
            )
            metrics = cursor.fetchall()
            cursor.execute(
                """SELECT service_name,time_unix,observed_time,severity_number,severity_text,body,
                          trace_id,span_id,attributes,resource_attributes,run_id
                   FROM log_records
                   WHERE COALESCE(time_unix, observed_time) >= %s
                     AND COALESCE(time_unix, observed_time) < %s""",
                (start, cutoff),
            )
            logs = cursor.fetchall()
        return list(spans), list(metrics), list(logs)

    def _write_edge(self, connection: Any, edge: Any) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """DELETE FROM dependency_edges
                   WHERE bucket_start=%s AND source_service=%s AND target_service=%s
                     AND protocol=%s AND run_id IS NOT DISTINCT FROM %s""",
                (edge.bucket_start, edge.source, edge.target, edge.protocol, edge.run_id),
            )
            cursor.execute(
                """INSERT INTO dependency_edges
                   (bucket_start,source_service,target_service,protocol,request_count,error_count,
                    error_rate,latency_ms_p50,latency_ms_p95,latency_ms_p99,run_id,attributes)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
                (
                    edge.bucket_start, edge.source, edge.target, edge.protocol, edge.request_count,
                    edge.error_count, edge.error_rate, edge.latency_ms_p50, edge.latency_ms_p95,
                    edge.latency_ms_p99, edge.run_id, _json(edge.attributes),
                ),
            )

    def _write_feature(self, connection: Any, result: AnomalyResult) -> None:
        bucket = result.bucket
        with connection.cursor() as cursor:
            cursor.execute(
                """DELETE FROM feature_buckets
                   WHERE bucket_start=%s AND entity_type=%s AND entity_id=%s
                     AND run_id IS NOT DISTINCT FROM %s""",
                (bucket.bucket_start, bucket.entity_type, bucket.entity_id, bucket.run_id),
            )
            cursor.execute(
                """INSERT INTO feature_buckets
                   (bucket_start,entity_type,entity_id,run_id,request_count,error_count,error_rate,
                    latency_ms_p50,latency_ms_p95,latency_ms_p99,cpu_percent,memory_bytes,
                    log_error_count,anomaly_score,onset_score,features,missingness)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)""",
                (
                    bucket.bucket_start, bucket.entity_type, bucket.entity_id, bucket.run_id,
                    bucket.value("request_count"), bucket.value("error_count"), bucket.value("error_rate"),
                    bucket.value("latency_ms_p50"), bucket.value("latency_ms_p95"),
                    bucket.value("latency_ms_p99"), bucket.value("cpu_percent"),
                    bucket.value("memory_bytes"), bucket.value("log_error_count"), result.score,
                    result.onset_score, _json(bucket.features), _json(bucket.missingness),
                ),
            )

    def _persist_incident(self, connection: Any, event: IncidentEvent) -> Incident:
        incident = event.incident
        # A replay/restart must identify the same incident, not generate duplicates.
        incident.id = str(uuid5(NAMESPACE_URL, f"rca:{incident.run_id}:{incident.started_at.isoformat()}:{incident.trigger_entity}"))
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO incidents
                   (id,run_id,status,started_at,detected_at,ended_at,severity,summary,trigger_entity,
                    max_anomaly,affected_entities,metadata)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                   ON CONFLICT (id) DO UPDATE SET
                     run_id=EXCLUDED.run_id, status=EXCLUDED.status, ended_at=EXCLUDED.ended_at, severity=EXCLUDED.severity,
                     summary=EXCLUDED.summary, max_anomaly=EXCLUDED.max_anomaly,
                     affected_entities=EXCLUDED.affected_entities, metadata=EXCLUDED.metadata""",
                (
                    incident.id, incident.run_id, incident.status, incident.started_at, incident.detected_at,
                    incident.ended_at, incident.severity, incident.summary, incident.trigger_entity,
                    incident.max_anomaly, incident.affected_entities, _json({**incident.metadata, "run_id": incident.run_id}),
                ),
            )
        return incident

    def _replace_rankings(self, connection: Any, incident: Incident, rankings: Iterable[Any]) -> None:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM evidence_items WHERE incident_id=%s", (incident.id,))
            cursor.execute("DELETE FROM candidate_rankings WHERE incident_id=%s", (incident.id,))
            inserted: list[tuple[Any, int]] = []
            for ranking in rankings:
                online_features = dict(ranking.online_features) or dict(
                    online_candidate_record(incident, ranking)["features"]
                )
                cursor.execute(
                    """INSERT INTO candidate_rankings
                       (incident_id,rank,entity_type,entity_id,score,probability,fault_class,confidence,
                        evidence_summary,model_version,online_features,created_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW()) RETURNING id""",
                    (
                        incident.id, ranking.rank, ranking.entity_type, ranking.entity_id,
                        ranking.score, ranking.probability, ranking.fault_class, ranking.confidence,
                        ranking.evidence_summary, ranking.model_version, _json(online_features),
                    ),
                )
                candidate_id = cursor.fetchone()["id"]
                inserted.append((ranking, candidate_id))
                for evidence in ranking.evidence:
                    cursor.execute(
                        """INSERT INTO evidence_items
                           (incident_id,candidate_id,evidence_type,entity_id,occurred_at,signal_name,
                            value,baseline,contribution,details)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
                        (
                            incident.id, candidate_id, evidence.evidence_type, evidence.entity_id,
                            evidence.occurred_at, evidence.signal_name,
                            evidence.value if isinstance(evidence.value, (int, float)) else None,
                            evidence.baseline if isinstance(evidence.baseline, (int, float)) else None,
                            evidence.contribution, _json(evidence.details),
                        ),
                    )
            self._write_recommendation(cursor, incident, inserted)

    @staticmethod
    def _write_recommendation(
        cursor: Any, incident: Incident, inserted: Sequence[tuple[Any, int]]
    ) -> None:
        actionable = next(
            ((ranking, candidate_id) for ranking, candidate_id in inserted if ranking.entity_id != "inconclusive"),
            None,
        )
        if actionable is None:
            return
        ranking, candidate_id = actionable
        runbook_entity_type = {
            "dependency_edge": "edge",
            "service": "service",
        }.get(ranking.entity_type, ranking.entity_type)
        cursor.execute(
            """SELECT id, title, entity_type FROM runbooks
               WHERE enabled=true AND fault_class=%s
               ORDER BY CASE WHEN entity_type=%s THEN 0 WHEN entity_type='any' THEN 1 ELSE 2 END,
                        version DESC
               LIMIT 1""",
            (ranking.fault_class, runbook_entity_type),
        )
        runbook = cursor.fetchone()
        if not runbook:
            return
        rationale = (
            f"Selected {runbook['title']} for rank-{ranking.rank} {ranking.entity_type} "
            f"{ranking.entity_id}; class={ranking.fault_class}, probability={ranking.probability:.3f}."
        )
        cursor.execute(
            """INSERT INTO recommendations
               (incident_id,candidate_id,runbook_id,compatibility_score,rationale)
               VALUES (%s,%s,%s,%s,%s)
               ON CONFLICT (incident_id,candidate_id,runbook_id) DO UPDATE SET
                 compatibility_score=EXCLUDED.compatibility_score,
                 rationale=EXCLUDED.rationale,
                 created_at=NOW()""",
            (incident.id, candidate_id, runbook["id"], ranking.probability, rationale),
        )


def main(argv: Sequence[str] | None = None) -> int:
    defaults = WorkerSettings.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=defaults.database_url)
    parser.add_argument("--poll-seconds", type=float, default=defaults.poll_seconds)
    parser.add_argument("--bucket-seconds", type=int, default=defaults.bucket_seconds)
    parser.add_argument("--lookback-buckets", type=int, default=defaults.lookback_buckets)
    parser.add_argument("--min-history-buckets", type=int, default=defaults.min_history_buckets)
    parser.add_argument("--min-incident-buckets", type=int, default=defaults.min_incident_buckets)
    parser.add_argument("--model-path", default=defaults.model_path)
    parser.add_argument("--once", action="store_true", default=os.environ.get("RCA_ONCE") == "1")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    worker = RCAWorker(
        WorkerSettings(
            database_url=args.database_url,
            poll_seconds=args.poll_seconds,
            bucket_seconds=args.bucket_seconds,
            lookback_buckets=args.lookback_buckets,
            min_history_buckets=args.min_history_buckets,
            min_incident_buckets=args.min_incident_buckets,
            model_path=args.model_path,
        )
    )
    if args.check:
        worker.check()
        return 0
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    worker.run(once=args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
