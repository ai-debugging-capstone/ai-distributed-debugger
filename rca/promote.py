"""Explicitly promote a registered RCA challenger and publish the worker alias."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .train import file_sha256


class PromotionError(RuntimeError):
    pass


def stage_deployment_alias(artifact: Path, output_dir: Path) -> tuple[Path, Path]:
    """Durably stage a copy without changing the live alias."""

    output_dir.mkdir(parents=True, exist_ok=True)
    alias = output_dir / "model.json"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".model-promotion-", suffix=".tmp", dir=output_dir
    )
    temporary = Path(temporary_name)
    try:
        with artifact.open("rb") as reader, os.fdopen(descriptor, "wb") as writer:
            descriptor = -1
            shutil.copyfileobj(reader, writer)
            writer.flush()
            os.fsync(writer.fileno())
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise
    return temporary, alias


def activate_deployment_alias(staged: Path, alias: Path) -> Path:
    """Atomically replace the stable worker path after registry commit."""

    os.replace(staged, alias)
    return alias


def validate_registry_artifact(model: Mapping[str, Any]) -> tuple[Path, str]:
    status = str(model.get("status") or "")
    if status not in {"challenger", "champion"}:
        raise PromotionError(
            f"model status {status or 'unknown'} is not promotable; expected challenger"
        )
    checksum = str(model.get("artifact_checksum") or "")
    uri = str(model.get("artifact_uri") or "")
    if not checksum or not uri:
        raise PromotionError("registered model is missing its artifact URI or checksum")
    artifact = Path(uri)
    if not artifact.is_file():
        raise PromotionError(f"registered model artifact does not exist: {artifact}")
    actual = file_sha256(artifact)
    if actual != checksum:
        raise PromotionError(
            f"registered model artifact checksum mismatch: expected {checksum}, got {actual}"
        )
    return artifact, checksum


def _compensate_registry(
    connection: Any,
    *,
    promoted_id: Any,
    previous_champion_id: Any | None,
) -> bool:
    """Best-effort rollback when alias publication fails after registry commit."""

    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE model_versions
                SET status = 'challenger'
                WHERE id = %s AND status = 'champion'
                """,
                (promoted_id,),
            )
            if cursor.rowcount != 1:
                return False
            if previous_champion_id is not None:
                cursor.execute(
                    """
                    UPDATE model_versions
                    SET status = 'champion'
                    WHERE id = %s AND status = 'retired'
                    """,
                    (previous_champion_id,),
                )
                if cursor.rowcount != 1:
                    raise PromotionError(
                        "could not restore the previous champion during compensation"
                    )
    return True


def promote_registered_model(
    database_url: str,
    *,
    name: str,
    version: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Commit registry lifecycle first, then atomically publish the validated bytes."""

    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - deployment dependency
        raise PromotionError("model promotion requires rca/requirements.txt") from exc

    staged: Path | None = None
    alias: Path | None = None
    model: Mapping[str, Any] | None = None
    previous_champion_id: Any | None = None
    registry_changed = False
    checksum = ""
    lock_key = f"rca-model-promotion:{name}"
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as connection:
            with connection.cursor() as lock_cursor:
                lock_cursor.execute(
                    "SELECT pg_advisory_lock(hashtext(%s))", (lock_key,)
                )
            # Session locks survive transaction commit and serialize the
            # registry-to-filesystem handoff for all versions of this model.
            connection.commit()
            try:
                with connection.transaction():
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            SELECT id, name, version, status, artifact_uri,
                                   artifact_checksum, dataset_snapshot_id
                            FROM model_versions
                            WHERE name = %s AND version = %s
                            FOR UPDATE
                            """,
                            (name, version),
                        )
                        model = cursor.fetchone()
                        if model is None:
                            raise PromotionError(f"registered model {name}:{version} was not found")
                        artifact, checksum = validate_registry_artifact(model)
                        staged, alias = stage_deployment_alias(artifact, Path(output_dir))
                        if file_sha256(staged) != checksum:
                            raise PromotionError(
                                "registered model artifact changed while it was staged"
                            )

                        if model["status"] == "challenger":
                            cursor.execute(
                                """
                                SELECT id
                                FROM model_versions
                                WHERE name = %s AND status = 'champion' AND id <> %s
                                FOR UPDATE
                                """,
                                (name, model["id"]),
                            )
                            previous = cursor.fetchone()
                            previous_champion_id = previous["id"] if previous else None
                            if previous_champion_id is not None:
                                cursor.execute(
                                    """
                                    UPDATE model_versions SET status = 'retired'
                                    WHERE id = %s AND status = 'champion'
                                    """,
                                    (previous_champion_id,),
                                )
                                if cursor.rowcount != 1:
                                    raise PromotionError("the current champion changed during promotion")
                            cursor.execute(
                                """
                                UPDATE model_versions SET status = 'champion'
                                WHERE id = %s AND status = 'challenger'
                                """,
                                (model["id"],),
                            )
                            if cursor.rowcount != 1:
                                raise PromotionError("the challenger changed during promotion")
                            registry_changed = True

                assert staged is not None and alias is not None and model is not None
                try:
                    activate_deployment_alias(staged, alias)
                    staged = None
                except Exception as publish_error:
                    if registry_changed:
                        try:
                            compensated = _compensate_registry(
                                connection,
                                promoted_id=model["id"],
                                previous_champion_id=previous_champion_id,
                            )
                        except Exception as compensation_error:
                            raise PromotionError(
                                "alias publication failed and registry compensation also failed"
                            ) from compensation_error
                        if not compensated:
                            raise PromotionError(
                                "alias publication failed after another registry change; manual recovery is required"
                            ) from publish_error
                    raise PromotionError(
                        "alias publication failed; registry lifecycle was restored"
                    ) from publish_error

                return {
                    "id": str(model["id"]),
                    "name": str(model["name"]),
                    "version": str(model["version"]),
                    "status": "champion",
                    "artifact_sha256": checksum,
                    "deployment_alias": str(alias),
                    "previous_champion_id": (
                        str(previous_champion_id) if previous_champion_id is not None else None
                    ),
                    "registry_changed": registry_changed,
                }
            finally:
                with connection.cursor() as lock_cursor:
                    lock_cursor.execute(
                        "SELECT pg_advisory_unlock(hashtext(%s))", (lock_key,)
                    )
                connection.commit()
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="PostgreSQL URL (defaults to DATABASE_URL)",
    )
    parser.add_argument("--model-name", default="candidate-ranker")
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--output-dir", default="artifacts/champion")
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    result = promote_registered_model(
        args.database_url,
        name=args.model_name,
        version=args.model_version,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
