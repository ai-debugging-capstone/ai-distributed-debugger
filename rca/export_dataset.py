"""Export leakage-safe candidate-ranking rows from PostgreSQL for model training.

Ground truth is used only to create labels and evaluation metadata. Every value in
``features`` comes from the online candidate/evidence records that existed when
the incident was analyzed; fault flags, injected entity names, and expected
runbooks never enter the model feature vector.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from .candidate_features import (
    candidate_feature_map,
    candidate_key,
    canonical_entity,
    canonical_type,
)


class DatasetExportError(RuntimeError):
    pass


RESERVED_RUN_ID = "00000000-0000-0000-0000-000000000000"


def normalize_include_run_ids(run_ids: Sequence[str] | None) -> tuple[str, ...]:
    """Validate and deterministically order an explicit experiment allowlist."""

    normalized: set[str] = set()
    for value in run_ids or ():
        try:
            run_id = str(UUID(str(value)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise DatasetExportError(f"invalid --include-run UUID: {value!r}") from exc
        if run_id == RESERVED_RUN_ID:
            raise DatasetExportError("the reserved live-telemetry run cannot be exported")
        normalized.add(run_id)
    return tuple(sorted(normalized))


def selection_manifest(
    *,
    include_run_ids: Sequence[str] | None = None,
    source_repository: str | None = None,
    source_version: str | None = None,
    source_sha: str | None = None,
    include_active: bool = False,
) -> dict[str, Any]:
    """Freeze the exact selection policy alongside the exported run provenance."""

    return {
        "include_run_ids": list(normalize_include_run_ids(include_run_ids)),
        "source_repository": source_repository,
        "source_version": source_version,
        "source_sha": source_sha,
        "include_active": bool(include_active),
        "training_eligible_only": True,
        "reserved_run_excluded": True,
    }


def freeze_run_provenance(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return a stable, JSON-safe manifest for the exact exported runs."""

    frozen: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
                item[str(key)] = aware.astimezone(timezone.utc).isoformat().replace(
                    "+00:00", "Z"
                )
            else:
                item[str(key)] = value
        frozen.append(item)
    frozen.sort(key=lambda item: str(item.get("run_id") or ""))
    return frozen


def provenance_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    payload = json.dumps(
        list(rows), sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_candidate_records(
    candidates: Sequence[Mapping[str, Any]],
    labels: Sequence[Mapping[str, Any]],
    evidence: Sequence[Mapping[str, Any]],
    *,
    eligible_incidents: Sequence[Mapping[str, Any]] | None = None,
    require_positive_per_incident: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build model rows while keeping labels outside the feature dictionary."""

    labels_by_run: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for label in labels:
        if int(label.get("relevance") or 0) > 0:
            labels_by_run[str(label["run_id"])].append(label)

    evidence_by_candidate: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for item in evidence:
        if item.get("candidate_id") is not None:
            evidence_by_candidate[str(item["candidate_id"])].append(item)

    records: list[dict[str, Any]] = []
    positives_by_incident: dict[str, int] = defaultdict(int)
    candidates_by_incident: dict[str, int] = defaultdict(int)
    if eligible_incidents is not None:
        # Initialize before reading candidate rows so a labeled incident whose
        # generator produced zero candidates is visible to the fail-closed
        # check instead of silently disappearing from the artifact.
        for incident in eligible_incidents:
            candidates_by_incident[str(incident["incident_id"])] = 0
    skipped_unlabelled = 0
    skipped_abstention = 0
    for candidate in candidates:
        run_id = str(candidate.get("run_id") or "")
        run_labels = labels_by_run.get(run_id, [])
        if not run_labels:
            skipped_unlabelled += 1
            continue
        incident_id = str(candidate["incident_id"])
        candidate_entity = canonical_entity(candidate.get("entity_id"))
        candidate_type = canonical_type(candidate.get("entity_type"))
        if candidate_type == "system" and candidate_entity == "inconclusive":
            # Abstention is evaluated as a separate system decision. It is not
            # an entity candidate and the online learned reranker never scores
            # it, so including it here would create training-serving skew.
            skipped_abstention += 1
            continue
        applicable = [
            label
            for label in run_labels
            if not label.get("incident_id")
            or str(label.get("incident_id")) == incident_id
        ]
        if not applicable:
            # An explicitly incident-scoped label must not turn every other
            # incident in the same run into an all-negative training example.
            skipped_unlabelled += 1
            continue
        matching = [
            label
            for label in applicable
            if canonical_entity(label.get("entity_id")) == candidate_entity
            and canonical_type(label.get("entity_type")) == candidate_type
        ]
        primary = next((label for label in applicable if label.get("is_primary")), None)
        representative = primary or (applicable[0] if applicable else run_labels[0])
        label_value = int(bool(matching))
        candidates_by_incident[incident_id] += 1
        positives_by_incident[incident_id] += label_value

        entity_type = canonical_type(candidate.get("entity_type"))
        model_version = str(candidate.get("model_version") or "unknown")
        stored_features = candidate.get("online_features")
        if not stored_features and not model_version.casefold().startswith("heuristic-"):
            raise DatasetExportError(
                "learned candidate row is missing its captured online feature vector: "
                + str(candidate.get("candidate_row_id") or "unknown")
            )
        if stored_features and not isinstance(stored_features, Mapping):
            raise DatasetExportError(
                "candidate online feature snapshot is not an object: "
                + str(candidate.get("candidate_row_id") or "unknown")
            )
        features = candidate_feature_map(
            candidate,
            evidence_by_candidate.get(str(candidate.get("candidate_row_id")), []),
            stored_features=stored_features,
        )
        records.append(
            {
                "schema_version": 2,
                "run_id": run_id,
                "incident_id": incident_id,
                "candidate_id": candidate_key(candidate_type, candidate_entity),
                "candidate_type": entity_type,
                "candidate_entity_id": str(candidate.get("entity_id")),
                "split": candidate.get("dataset_split"),
                "label": label_value,
                "label_relevance": max(
                    (int(label.get("relevance") or 0) for label in matching),
                    default=0,
                ),
                "fault_type": str(representative.get("fault_class") or "unknown"),
                "model_version": model_version,
                "features": features,
            }
        )

    unmatched = sorted(
        incident_id
        for incident_id in candidates_by_incident
        if positives_by_incident[incident_id] == 0
    )
    if require_positive_per_incident and unmatched:
        raise DatasetExportError(
            "ground truth was not present in the generated candidate set for incident(s): "
            + ", ".join(unmatched)
        )
    records.sort(key=lambda row: (row["run_id"], row["incident_id"], row["candidate_id"]))
    summary = {
        "records": len(records),
        "runs": len({row["run_id"] for row in records}),
        "incidents": len({row["incident_id"] for row in records}),
        "positive_records": sum(int(row["label"]) for row in records),
        "unmatched_incidents": unmatched,
        "eligible_incidents": len(candidates_by_incident),
        "skipped_unlabelled_candidates": skipped_unlabelled,
        "skipped_abstention_candidates": skipped_abstention,
    }
    return records, summary


def fetch_dataset_inputs(
    database_url: str,
    *,
    include_active: bool = False,
    include_run_ids: Sequence[str] | None = None,
    source_repository: str | None = None,
    source_version: str | None = None,
    source_sha: str | None = None,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - deployment dependency
        raise DatasetExportError("install rca/requirements.txt to export from PostgreSQL") from exc

    normalized_run_ids = normalize_include_run_ids(include_run_ids)
    status_clause = (
        "er.status IN ('warming', 'running', 'recovering', 'completed')"
        if include_active
        else "er.status = 'completed'"
    )
    selector_clauses: list[str] = []
    selector_parameters: list[Any] = []
    if normalized_run_ids:
        selector_clauses.append("er.id = ANY(%s::uuid[])")
        selector_parameters.append(list(normalized_run_ids))
    for column, value in (
        ("source_repository", source_repository),
        ("source_version", source_version),
        ("source_sha", source_sha),
    ):
        if value is not None:
            selector_clauses.append(f"er.{column} = %s")
            selector_parameters.append(value)
    selector_clause = "".join(f"\n        AND {clause}" for clause in selector_clauses)
    eligibility_clause = f"""
        {status_clause}
        AND er.id <> '{RESERVED_RUN_ID}'::uuid
        AND COALESCE(er.configuration ->> 'training_eligible', 'true') <> 'false'
        {selector_clause}
    """
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                # Candidate rows, evidence, labels, and lineage must describe one
                # database state even while the worker atomically reranks an incident.
                cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                cursor.execute(
                    f"""
                    SELECT DISTINCT g.run_id::text AS run_id
                    FROM ground_truth_labels AS g
                    JOIN experiment_runs AS er ON er.id = g.run_id
                    WHERE g.relevance > 0
                      AND {eligibility_clause}
                      AND NOT EXISTS (
                          SELECT 1
                          FROM incidents AS i
                          WHERE i.run_id = g.run_id
                            AND (g.incident_id IS NULL OR i.id = g.incident_id)
                      )
                    ORDER BY g.run_id::text
                    """,
                    selector_parameters,
                )
                missing_incident_runs = [str(row["run_id"]) for row in cursor.fetchall()]
                if missing_incident_runs:
                    raise DatasetExportError(
                        "positive ground truth has no matching incident for completed run(s): "
                        + ", ".join(missing_incident_runs)
                    )

                cursor.execute(
                    f"""
                    SELECT
                        cr.id AS candidate_row_id,
                        cr.incident_id::text AS incident_id,
                        i.run_id::text AS run_id,
                        cr.entity_type,
                        cr.entity_id,
                        cr.rank,
                        cr.score,
                        cr.probability,
                        cr.confidence,
                        cr.fault_class,
                        cr.model_version,
                        cr.online_features,
                        er.split AS dataset_split
                    FROM candidate_rankings AS cr
                    JOIN incidents AS i ON i.id = cr.incident_id
                    JOIN experiment_runs AS er ON er.id = i.run_id
                    WHERE i.run_id IS NOT NULL AND {eligibility_clause}
                    ORDER BY i.run_id, cr.incident_id, cr.rank
                    """,
                    selector_parameters,
                )
                candidates = list(cursor.fetchall())
                cursor.execute(
                    f"""
                    SELECT g.run_id::text AS run_id, g.incident_id::text AS incident_id,
                           g.entity_type, g.entity_id, g.fault_class, g.relevance,
                           g.is_primary
                    FROM ground_truth_labels AS g
                    JOIN experiment_runs AS er ON er.id = g.run_id
                    WHERE g.relevance > 0 AND {eligibility_clause}
                    ORDER BY g.run_id, g.is_primary DESC, g.relevance DESC, g.id
                    """,
                    selector_parameters,
                )
                labels = list(cursor.fetchall())
                cursor.execute(
                    f"""
                    SELECT i.id::text AS incident_id, i.run_id::text AS run_id
                    FROM incidents AS i
                    JOIN experiment_runs AS er ON er.id = i.run_id
                    WHERE i.run_id IS NOT NULL
                      AND {eligibility_clause}
                      AND EXISTS (
                          SELECT 1
                          FROM ground_truth_labels AS g
                          WHERE g.run_id = i.run_id
                            AND g.relevance > 0
                            AND (g.incident_id IS NULL OR g.incident_id = i.id)
                      )
                    ORDER BY i.run_id, i.id
                    """,
                    selector_parameters,
                )
                eligible_incidents = list(cursor.fetchall())
                cursor.execute(
                    f"""
                    SELECT ei.candidate_id, ei.evidence_type, ei.signal_name,
                           ei.value, ei.baseline, ei.contribution, ei.details
                    FROM evidence_items AS ei
                    JOIN candidate_rankings AS cr ON cr.id = ei.candidate_id
                    JOIN incidents AS i ON i.id = cr.incident_id
                    JOIN experiment_runs AS er ON er.id = i.run_id
                    WHERE i.run_id IS NOT NULL AND {eligibility_clause}
                    ORDER BY ei.candidate_id, ei.id
                    """,
                    selector_parameters,
                )
                evidence = list(cursor.fetchall())
                cursor.execute(
                    f"""
                    SELECT er.id::text AS run_id, er.name, er.source_repository,
                           er.source_version, er.source_sha, er.seed, er.workload,
                           er.split, er.started_at, er.ended_at,
                           er.configuration, er.environment
                    FROM experiment_runs AS er
                    WHERE {eligibility_clause}
                      AND EXISTS (
                          SELECT 1 FROM ground_truth_labels AS g
                          WHERE g.run_id = er.id AND g.relevance > 0
                      )
                    ORDER BY er.id
                    """,
                    selector_parameters,
                )
                provenance = list(cursor.fetchall())
    return candidates, labels, evidence, eligible_incidents, provenance


def write_dataset(
    records: Sequence[Mapping[str, Any]], output: str | Path, summary: Mapping[str, Any]
) -> tuple[Path, Path, str]:
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(
        json.dumps(record, sort_keys=True, separators=(",", ":"), default=str) + "\n"
        for record in records
    )
    temporary = target.with_suffix(target.suffix + ".tmp")
    encoded = body.encode("utf-8")
    checksum = hashlib.sha256(encoded).hexdigest()
    if target.exists():
        if hashlib.sha256(target.read_bytes()).hexdigest() != checksum:
            raise DatasetExportError(
                f"refusing to overwrite immutable dataset artifact with different bytes: {target}"
            )
    else:
        temporary.write_bytes(encoded)
        temporary.replace(target)
    metadata_path = target.with_suffix(target.suffix + ".metadata.json")
    metadata_core = {
        "schema_version": 2,
        "artifact": str(target),
        "manifest_uri": str(metadata_path.resolve()),
        "sha256": checksum,
        **dict(summary),
    }
    if metadata_path.exists():
        try:
            existing_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DatasetExportError(f"existing dataset manifest is unreadable: {metadata_path}") from exc
        comparable = {
            key: value for key, value in existing_metadata.items() if key != "created_at"
        }
        if comparable != metadata_core:
            raise DatasetExportError(
                f"refusing to overwrite immutable dataset manifest: {metadata_path}"
            )
    else:
        metadata = {
            **metadata_core,
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        metadata_temporary = metadata_path.with_suffix(metadata_path.suffix + ".tmp")
        metadata_temporary.write_text(
            json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8"
        )
        metadata_temporary.replace(metadata_path)
    return target, metadata_path, checksum


def register_snapshot(
    database_url: str,
    *,
    name: str,
    version: str,
    artifact: Path,
    checksum: str,
    summary: Mapping[str, Any],
    source_sha: str | None = None,
) -> None:
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover
        raise DatasetExportError("install rca/requirements.txt to register a snapshot") from exc
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO dataset_snapshots (
                    name, version, source_sha, manifest_uri, checksum,
                    run_count, statistics
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (name, version) DO NOTHING
                RETURNING checksum, source_sha, manifest_uri, run_count, statistics
                """,
                (
                    name,
                    version,
                    source_sha,
                    str(artifact.resolve()),
                    checksum,
                    int(summary.get("runs") or 0),
                    Jsonb(dict(summary)),
                ),
            )
            inserted = cursor.fetchone()
            if inserted is None:
                cursor.execute(
                    """
                    SELECT checksum, source_sha, manifest_uri, run_count, statistics
                    FROM dataset_snapshots
                    WHERE name = %s AND version = %s
                    """,
                    (name, version),
                )
                existing = cursor.fetchone()
                expected = (
                    checksum,
                    source_sha,
                    str(artifact.resolve()),
                    int(summary.get("runs") or 0),
                    dict(summary),
                )
                if existing is None or tuple(existing) != expected:
                    raise DatasetExportError(
                        f"dataset snapshot {name}:{version} already exists with different bytes or provenance"
                    )
        connection.commit()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="PostgreSQL URL (defaults to DATABASE_URL)",
    )
    parser.add_argument(
        "--output",
        help=(
            "Immutable JSONL path; defaults to a snapshot-versioned file under "
            "data/exports"
        ),
    )
    parser.add_argument("--include-active", action="store_true")
    parser.add_argument("--allow-unmatched", action="store_true")
    parser.add_argument(
        "--include-run",
        action="append",
        default=[],
        metavar="UUID",
        help="Export only this experiment UUID; repeat to select multiple runs",
    )
    parser.add_argument(
        "--source-repository",
        help="Require an exact experiment source_repository value",
    )
    parser.add_argument(
        "--source-version",
        help="Require an exact experiment source_version value",
    )
    parser.add_argument(
        "--source-sha",
        help="Require an exact experiment source_sha value",
    )
    parser.add_argument("--snapshot-name", default="candidate-ranking")
    parser.add_argument(
        "--snapshot-version",
        default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
    )
    parser.add_argument("--no-register-snapshot", action="store_true")
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")

    selected_run_allowlist = normalize_include_run_ids(args.include_run)
    selector = selection_manifest(
        include_run_ids=selected_run_allowlist,
        source_repository=args.source_repository,
        source_version=args.source_version,
        source_sha=args.source_sha,
        include_active=args.include_active,
    )
    candidates, labels, evidence, eligible_incidents, provenance_rows = fetch_dataset_inputs(
        args.database_url,
        include_active=args.include_active,
        include_run_ids=selected_run_allowlist,
        source_repository=args.source_repository,
        source_version=args.source_version,
        source_sha=args.source_sha,
    )
    eligible_run_ids = {str(row["run_id"]) for row in provenance_rows}
    missing_selected_runs = sorted(set(selected_run_allowlist) - eligible_run_ids)
    if missing_selected_runs:
        raise DatasetExportError(
            "explicitly included run(s) were not eligible and positively labeled under "
            "the requested selector: " + ", ".join(missing_selected_runs)
        )
    records, summary = build_candidate_records(
        candidates,
        labels,
        evidence,
        eligible_incidents=eligible_incidents,
        require_positive_per_incident=not args.allow_unmatched,
    )
    if not records:
        raise DatasetExportError("no labeled candidate records were available to export")
    output = args.output or (
        f"data/exports/candidate-ranking-{args.snapshot_version}.jsonl"
    )
    selected_run_ids = {str(record["run_id"]) for record in records}
    provenance_by_run = {
        str(row["run_id"]): row for row in provenance_rows if str(row["run_id"]) in selected_run_ids
    }
    missing_provenance = sorted(selected_run_ids - provenance_by_run.keys())
    if missing_provenance:
        raise DatasetExportError(
            "exported records are missing frozen experiment provenance for run(s): "
            + ", ".join(missing_provenance)
        )
    run_provenance = freeze_run_provenance(list(provenance_by_run.values()))
    source_shas = sorted(
        {str(row["source_sha"]) for row in run_provenance if row.get("source_sha")}
    )
    output_path = Path(output)
    summary = {
        **summary,
        "source_shas": source_shas,
        "selection": selector,
        "run_provenance": run_provenance,
        "provenance_sha256": provenance_sha256(run_provenance),
        "manifest_uri": str(
            output_path.with_suffix(output_path.suffix + ".metadata.json").resolve()
        ),
    }
    artifact, metadata, checksum = write_dataset(records, output, summary)
    if not args.no_register_snapshot:
        register_snapshot(
            args.database_url,
            name=args.snapshot_name,
            version=args.snapshot_version,
            artifact=artifact,
            checksum=checksum,
            summary=summary,
            source_sha=source_shas[0] if len(source_shas) == 1 else None,
        )
    print(
        json.dumps(
            {
                "artifact": str(artifact),
                "metadata": str(metadata),
                "sha256": checksum,
                "snapshot": None
                if args.no_register_snapshot
                else f"{args.snapshot_name}:{args.snapshot_version}",
                **summary,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
