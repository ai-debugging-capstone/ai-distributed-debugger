"""Offline champion training and run-level evaluation command."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .dataset import DatasetSplit, split_by_run
from .evaluation import (
    RankingExample,
    brier_score,
    evaluate_rankings,
    expected_calibration_error,
    risk_coverage,
)
from .models import ChampionRanker

FEATURE_PROFILES: dict[str, tuple[str, ...] | None] = {
    "full": None,
    # For smoke-scale datasets, constrain the linear fallback to stable
    # candidate-level signals. High-dimensional evidence contributions require
    # substantially more independent incidents or they overfit individual
    # telemetry magnitudes and service mixes.
    "compact": (
        "baseline_inverse_rank",
        "baseline_probability",
        "baseline_score",
        "candidate_is_dependency_edge",
        "candidate_is_service",
    ),
}


def apply_feature_profile(
    records: Sequence[Mapping[str, Any]], profile: str
) -> tuple[Mapping[str, Any], ...]:
    """Return immutable training rows projected onto a declared feature profile."""

    if profile not in FEATURE_PROFILES:
        raise ValueError(
            f"unsupported feature profile {profile!r}; expected one of "
            f"{sorted(FEATURE_PROFILES)}"
        )
    selected = FEATURE_PROFILES[profile]
    if selected is None:
        return tuple(records)
    projected: list[Mapping[str, Any]] = []
    for row in records:
        values = row.get("features") or {}
        if not isinstance(values, Mapping):
            raise TypeError("features must be a mapping")
        projected.append(
            {
                **row,
                "features": {
                    name: float(values.get(name, 0.0)) for name in selected
                },
            }
        )
    return tuple(projected)


def load_records(path: str | Path) -> list[Mapping[str, Any]]:
    source = Path(path)
    if source.suffix.lower() in {".jsonl", ".ndjson"}:
        return [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines() if line.strip()]
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("dataset JSON must be an array or JSONL records")
    return payload


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_dataset_checksum(path: str | Path) -> str:
    """Verify the exporter sidecar when present and return the byte checksum."""

    source = Path(path)
    checksum = file_sha256(source)
    metadata_path = source.with_suffix(source.suffix + ".metadata.json")
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        expected = str(metadata.get("sha256") or "")
        if not expected or expected != checksum:
            raise ValueError(
                f"dataset checksum does not match metadata sidecar: {metadata_path}"
            )
    return checksum


def _candidate_identity(row: Mapping[str, Any]) -> str:
    """Use an ontology-qualified identity, including for legacy datasets."""

    candidate_id = str(row["candidate_id"])
    candidate_type = str(row.get("candidate_type") or "").strip().casefold()
    if not candidate_type or candidate_id.casefold().startswith(f"{candidate_type}:"):
        return candidate_id
    return f"{candidate_type}:{candidate_id}"


def partition_records(
    records: Sequence[Mapping[str, Any]], *, seed: int = 42
) -> tuple[DatasetSplit, tuple[Mapping[str, Any], ...]]:
    """Honor declared run splits, randomly assigning only undeclared runs.

    ``holdout`` records remain completely outside fitting, validation, and the
    ordinary test report.  They are returned separately for the final untouched
    evaluation.  A run may never declare two different split values.
    """

    by_run: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    declared_by_run: dict[str, str | None] = {}
    allowed = {"train", "validation", "test", "holdout"}
    for row in records:
        run_id = str(row.get("run_id") or "")
        if not run_id:
            raise ValueError("every record must have a non-empty run_id")
        by_run[run_id].append(row)
    for run_id, rows in by_run.items():
        values = {
            str(row.get("split")).strip().casefold()
            for row in rows
            if row.get("split") not in (None, "")
        }
        unknown = values - allowed
        if unknown:
            raise ValueError(f"run {run_id} has unsupported split(s): {sorted(unknown)}")
        if len(values) > 1:
            raise ValueError(f"run {run_id} has conflicting declared splits: {sorted(values)}")
        declared_by_run[run_id] = next(iter(values), None)

    if not any(declared_by_run.values()):
        return split_by_run(records, seed=seed), ()

    undeclared = [
        row
        for run_id in sorted(by_run)
        if declared_by_run[run_id] is None
        for row in by_run[run_id]
    ]
    random_split = split_by_run(undeclared, seed=seed)
    train_runs = set(random_split.train_runs)
    validation_runs = set(random_split.validation_runs)
    test_runs = set(random_split.test_runs)
    holdout_runs: set[str] = set()
    destinations = {
        "train": train_runs,
        "validation": validation_runs,
        "test": test_runs,
        "holdout": holdout_runs,
    }
    for run_id, declared in declared_by_run.items():
        if declared is not None:
            destinations[declared].add(run_id)

    def rows_for(run_ids: set[str]) -> tuple[Mapping[str, Any], ...]:
        return tuple(row for run_id in sorted(run_ids) for row in by_run[run_id])

    result = DatasetSplit(
        train=rows_for(train_runs),
        validation=rows_for(validation_runs),
        test=rows_for(test_runs),
        train_runs=frozenset(train_runs),
        validation_runs=frozenset(validation_runs),
        test_runs=frozenset(test_runs),
    )
    result.assert_disjoint()
    return result, rows_for(holdout_runs)


def evaluate_model(
    model: ChampionRanker, records: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    if not records:
        return {**evaluate_rankings([]), "brier": 0.0, "ece": 0.0, "examples": 0}
    grouped_rows: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    grouped_scores: dict[str, list[float]] = defaultdict(list)
    scores = model.predict_scores(records)
    for row, score in zip(records, scores):
        incident_id = str(row["incident_id"])
        grouped_rows[incident_id].append(row)
        grouped_scores[incident_id].append(score)
    examples: list[RankingExample] = []
    confidences: list[float] = []
    outcomes: list[bool] = []
    for incident_id in sorted(grouped_rows):
        pairs = sorted(
            zip(grouped_rows[incident_id], grouped_scores[incident_id]),
            key=lambda item: (-item[1], str(item[0]["candidate_id"])),
        )
        relevant = frozenset(_candidate_identity(row) for row, _ in pairs if bool(row.get("label")))
        ranked = tuple(_candidate_identity(row) for row, _ in pairs)
        probabilities = tuple(float(score) for _, score in pairs)
        examples.append(RankingExample(relevant, ranked, probabilities))
        confidences.append(probabilities[0])
        outcomes.append(bool(ranked and ranked[0] in relevant))
    return {
        **evaluate_rankings(examples),
        "brier": brier_score(confidences, outcomes),
        "ece": expected_calibration_error(confidences, outcomes),
        "risk_coverage": risk_coverage(confidences, outcomes, (0.3, 0.5, 0.7, 0.9)),
        "examples": len(examples),
    }


def train_pipeline(
    records: Sequence[Mapping[str, Any]],
    *,
    prefer_sklearn: bool = True,
    seed: int = 42,
    feature_profile: str = "full",
) -> tuple[ChampionRanker, DatasetSplit, dict[str, Any]]:
    profiled_records = apply_feature_profile(records, feature_profile)
    split, holdout = partition_records(profiled_records, seed=seed)
    if not split.train:
        raise ValueError("run-level split produced no training records")
    model = ChampionRanker(prefer_sklearn=prefer_sklearn, random_state=seed).fit(split.train)
    report = {
        "backend": model.backend,
        "seed": seed,
        "feature_profile": feature_profile,
        "feature_schema": model.schema,
        "run_counts": {
            "train": len(split.train_runs),
            "validation": len(split.validation_runs),
            "test": len(split.test_runs),
            "holdout": len({str(row["run_id"]) for row in holdout}),
        },
        "validation": evaluate_model(model, split.validation),
        "test": evaluate_model(model, split.test),
        "holdout": evaluate_model(model, holdout),
        "run_membership": {
            "train": sorted(split.train_runs),
            "validation": sorted(split.validation_runs),
            "test": sorted(split.test_runs),
            "holdout": sorted({str(row["run_id"]) for row in holdout}),
        },
    }
    return model, split, report


def registered_model_matches(
    registered: Mapping[str, Any] | None,
    *,
    artifact_checksum: str,
    dataset_snapshot_id: Any,
) -> bool:
    """Compare immutable identity; registry lifecycle status is intentionally ignored."""

    return bool(
        registered is not None
        and registered["artifact_checksum"] == artifact_checksum
        and registered["dataset_snapshot_id"] == dataset_snapshot_id
    )


def register_trained_model(
    database_url: str,
    *,
    dataset_path: Path,
    dataset_checksum: str,
    artifact: Path,
    artifact_checksum: str,
    name: str,
    version: str,
    model_kind: str,
    configuration: Mapping[str, Any],
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    """Register an immutable challenger linked to its exported dataset snapshot."""

    try:
        import psycopg
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - deployment dependency
        raise RuntimeError("model registration requires rca/requirements.txt") from exc

    dataset_uri = str(dataset_path.resolve())
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, name, version, manifest_uri, checksum
                FROM dataset_snapshots
                WHERE checksum = %s
                ORDER BY CASE WHEN manifest_uri = %s THEN 0 ELSE 1 END,
                         created_at DESC
                LIMIT 1
                """,
                (dataset_checksum, dataset_uri),
            )
            snapshot = cursor.fetchone()
            if snapshot is None:
                raise ValueError(
                    "no registered dataset snapshot matches the training artifact checksum"
                )
            cursor.execute(
                """
                INSERT INTO model_versions (
                    name, version, model_kind, status, artifact_uri,
                    artifact_checksum, dataset_snapshot_id, configuration, metrics
                ) VALUES (%s, %s, %s, 'challenger', %s, %s, %s, %s, %s)
                ON CONFLICT (name, version) DO NOTHING
                RETURNING id, name, version, status, dataset_snapshot_id
                """,
                (
                    name,
                    version,
                    model_kind,
                    str(artifact.resolve()),
                    artifact_checksum,
                    snapshot["id"],
                    Jsonb(dict(configuration)),
                    Jsonb(dict(metrics)),
                ),
            )
            registered = cursor.fetchone()
            if registered is None:
                cursor.execute(
                    """
                    SELECT id, name, version, status, dataset_snapshot_id,
                           artifact_checksum
                    FROM model_versions
                    WHERE name = %s AND version = %s
                    """,
                    (name, version),
                )
                registered = cursor.fetchone()
                if not registered_model_matches(
                    registered,
                    artifact_checksum=artifact_checksum,
                    dataset_snapshot_id=snapshot["id"],
                ):
                    raise ValueError(
                        f"model version {name}:{version} already identifies a different artifact"
                    )
        connection.commit()
    return {
        "id": str(registered["id"]),
        "name": str(registered["name"]),
        "version": str(registered["version"]),
        "status": str(registered["status"]),
        "dataset_snapshot_id": str(registered["dataset_snapshot_id"]),
        "dataset_snapshot": f"{snapshot['name']}:{snapshot['version']}",
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", help="Candidate-row JSON or JSONL dataset")
    parser.add_argument("--output-dir", default="artifacts/champion")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--portable", action="store_true", help="Force dependency-free linear fallback")
    parser.add_argument(
        "--feature-profile",
        choices=sorted(FEATURE_PROFILES),
        default="full",
        help="Feature set used for fitting; compact is safer for smoke-scale datasets",
    )
    parser.add_argument(
        "--database-url",
        help="Optionally register the artifact as a PostgreSQL challenger",
    )
    parser.add_argument("--model-name", default="candidate-ranker")
    parser.add_argument("--model-version")
    args = parser.parse_args(argv)
    dataset_path = Path(args.dataset)
    dataset_checksum = verify_dataset_checksum(dataset_path)
    records = load_records(args.dataset)
    model, _, report = train_pipeline(
        records,
        prefer_sklearn=not args.portable,
        seed=args.seed,
        feature_profile=args.feature_profile,
    )
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    suffix = ".json" if model.backend == "portable-linear" else ".pkl"
    descriptor, temporary_name = tempfile.mkstemp(prefix=".model-", suffix=suffix, dir=output)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        temporary.unlink()
        model.save(temporary)
        artifact_checksum = file_sha256(temporary)
        artifact = output / f"model-{artifact_checksum[:16]}{suffix}"
        if artifact.exists():
            if file_sha256(artifact) != artifact_checksum:
                raise ValueError(f"model artifact checksum collision at {artifact}")
            temporary.unlink()
        else:
            temporary.replace(artifact)
    finally:
        temporary.unlink(missing_ok=True)

    version = args.model_version or f"sha256-{artifact_checksum[:16]}"
    evaluation = output / f"evaluation-{artifact_checksum[:16]}.json"
    evaluation.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    registered_model = None
    if args.database_url:
        registered_model = register_trained_model(
            args.database_url,
            dataset_path=dataset_path,
            dataset_checksum=dataset_checksum,
            artifact=artifact,
            artifact_checksum=artifact_checksum,
            name=args.model_name,
            version=version,
            model_kind=model.backend,
            configuration={
                "seed": args.seed,
                "portable": bool(args.portable),
                "feature_profile": args.feature_profile,
                "feature_schema": list(model.schema),
                "dataset_sha256": dataset_checksum,
                "evaluation_uri": str(evaluation.resolve()),
            },
            metrics=report,
        )
    print(
        json.dumps(
            {
                "artifact": str(artifact),
                "artifact_sha256": artifact_checksum,
                "deployment_alias": None,
                "promotion_required": True,
                "model_name": args.model_name,
                "model_version": version,
                "registered_model": registered_model,
                **report,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
