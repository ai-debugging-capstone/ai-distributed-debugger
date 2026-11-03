from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from telemetry_api.config import Settings
from telemetry_api.main import create_app
from telemetry_api.models import (
    ExperimentRunRequest,
    ExperimentRunUpdate,
    FaultInjectionRequest,
    FeedbackRequest,
    GroundTruthLabelRequest,
)
from telemetry_api.storage import IngestResult, StorageValidationError


class FakeStorage:
    def __init__(self) -> None:
        self.opened = False
        self.closed = False
        self.ready = True
        self.batches: list[dict[str, list[dict[str, Any]]]] = []
        self.rejections: list[dict[str, Any]] = []
        self.feedback: list[dict[str, Any]] = []
        self.experiments: dict[str, dict[str, Any]] = {}
        self.faults: list[dict[str, Any]] = []
        self.labels: list[dict[str, Any]] = []
        self.incidents = [
            {
                "id": "970c8464-87bf-42d2-a25f-cff3b01f1002",
                "run_id": None,
                "status": "open",
                "title": "Checkout failures",
            }
        ]

    async def open(self) -> None:
        self.opened = True

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> bool:
        return self.ready

    async def ingest(self, records, rejected) -> IngestResult:
        self.batches.append(records)
        self.rejections.extend(rejected)
        accepted = {key: len(records.get(key, [])) for key in ("span", "metric", "log")}
        return IngestResult(
            accepted=accepted,
            inserted=dict(accepted),
            duplicates={"span": 0, "metric": 0, "log": 0},
            rejected=len(rejected),
        )

    async def write_rejections(self, rejected) -> None:
        self.rejections.extend(rejected)

    async def upsert_experiment(self, run: ExperimentRunRequest):
        record = run.model_dump(mode="json")
        key = record["id"]
        existing = self.experiments.get(key)
        if existing is not None:
            placeholder = bool((existing.get("configuration") or {}).get("auto_registered"))
            terminal = existing.get("status") in {"completed", "failed"}
            immutable = (
                "name",
                "source_repository",
                "source_version",
                "source_sha",
                "seed",
                "workload",
                "split",
                "started_at",
                "ended_at",
            )
            for field in immutable:
                incoming = record.get(field)
                current = existing.get(field)
                if incoming is None:
                    continue
                if terminal and incoming != current:
                    raise StorageValidationError(
                        "experiment conflicts with the persisted immutable or terminal run state"
                    )
                if not placeholder and current is not None and incoming != current:
                    raise StorageValidationError(
                        "experiment conflicts with the persisted immutable or terminal run state"
                    )
            for field in ("configuration", "environment"):
                incoming_map = record.get(field) or {}
                current_map = existing.get(field) or {}
                if any(
                    name in current_map and current_map[name] != value
                    for name, value in incoming_map.items()
                ) or (terminal and any(name not in current_map for name in incoming_map)):
                    raise StorageValidationError(
                        "experiment conflicts with the persisted immutable or terminal run state"
                    )
            old_status = str(existing["status"])
            new_status = str(record["status"])
            order = {
                "created": 0,
                "warming": 1,
                "running": 2,
                "recovering": 3,
                "completed": 4,
                "failed": 4,
            }
            if (
                "status" not in run.model_fields_set
                or old_status in {"completed", "failed"}
                or order[new_status] < order[old_status]
            ):
                record.pop("status", None)
        started_at = record.get("started_at") or (existing or {}).get("started_at")
        ended_at = record.get("ended_at") or (existing or {}).get("ended_at")
        if started_at and ended_at and ended_at < started_at:
            raise StorageValidationError(
                "experiment timestamps conflict with the persisted run interval"
            )
        merged = dict(existing or {})
        for field, value in record.items():
            if field in {"configuration", "environment"}:
                values = dict(merged.get(field) or {})
                if field == "configuration":
                    values.pop("auto_registered", None)
                values.update(value or {})
                merged[field] = values
            elif value is not None:
                merged[field] = value
        self.experiments[key] = merged
        return self.experiments[key]

    async def get_experiment(self, run_id: str):
        return self.experiments.get(run_id)

    async def get_experiment_capture_summary(self, run_id: str):
        incidents = [item for item in self.incidents if item.get("run_id") == run_id]
        return {
            "run_id": run_id,
            "telemetry": {"spans": 3, "metric_points": 2, "log_records": 1},
            "derived": {"feature_buckets": 2, "dependency_edges": 1},
            "incidents": incidents,
            "candidates": [
                {
                    "incident_id": item["id"],
                    "rank": 1,
                    "entity_type": "service",
                    "entity_id": "payment",
                    "score": 0.9,
                    "probability": 0.8,
                    "model_version": "heuristic-v1",
                }
                for item in incidents
            ],
        }

    async def update_experiment(self, run_id: str, update: ExperimentRunUpdate):
        record = self.experiments.get(run_id)
        if record is None:
            return None
        values = update.model_dump(mode="json", exclude_unset=True)
        terminal = record.get("status") in {"completed", "failed"}
        for field in ("split", "workload", "started_at", "ended_at"):
            incoming = values.get(field)
            current = record.get(field)
            if incoming is not None and current is not None and incoming != current:
                raise StorageValidationError(
                    "experiment update conflicts with immutable or terminal run state"
                )
            if terminal and incoming is not None and incoming != current:
                raise StorageValidationError(
                    "experiment update conflicts with immutable or terminal run state"
                )
        for field in ("configuration", "environment"):
            incoming_map = values.get(field) or {}
            current_map = record.get(field) or {}
            if any(
                name in current_map and current_map[name] != value
                for name, value in incoming_map.items()
            ) or (terminal and any(name not in current_map for name in incoming_map)):
                raise StorageValidationError(
                    "experiment update conflicts with immutable or terminal run state"
                )
        old_status = str(record["status"])
        new_status = values.get("status")
        order = {
            "created": 0,
            "warming": 1,
            "running": 2,
            "recovering": 3,
            "completed": 4,
            "failed": 4,
        }
        if new_status and (
            old_status in {"completed", "failed"}
            or order[str(new_status)] < order[old_status]
        ):
            values.pop("status")
        started_at = values.get("started_at") or record.get("started_at")
        ended_at = values.get("ended_at") or record.get("ended_at")
        if started_at and ended_at and ended_at < started_at:
            raise StorageValidationError(
                "experiment timestamps conflict with the persisted run interval"
            )
        for field, value in values.items():
            if field in {"configuration", "environment"}:
                merged = dict(record.get(field) or {})
                merged.update(value or {})
                record[field] = merged
            elif value is not None:
                record[field] = value
        return record

    async def create_fault_injection(self, run_id: str, fault: FaultInjectionRequest):
        record = {"run_id": run_id, **fault.model_dump(mode="json")}
        existing = next((item for item in self.faults if item["id"] == record["id"]), None)
        if existing is not None:
            immutable = (
                "run_id",
                "flag_name",
                "variant",
                "injector",
                "causal_entity_type",
                "causal_entity_id",
                "causal_edge_source",
                "causal_edge_target",
                "mechanism",
                "impacted_sli",
                "expected_runbook_key",
                "injected_at",
            )
            if any(existing.get(key) != record.get(key) for key in immutable):
                raise StorageValidationError(
                    "fault injection id conflicts with an existing fault definition"
                )
            current_clear = existing.get("cleared_at")
            incoming_clear = record.get("cleared_at")
            if current_clear and incoming_clear and incoming_clear < current_clear:
                raise StorageValidationError(
                    "fault injection id conflicts with an existing fault definition"
                )
            record["cleared_at"] = incoming_clear or current_clear
            existing.update(record)
            return existing
        self.faults.append(record)
        return record

    async def upsert_ground_truth_label(
        self, run_id: str, label: GroundTruthLabelRequest
    ):
        record = {"id": len(self.labels) + 1, "run_id": run_id, **label.model_dump(mode="json")}
        incident_id = record.get("incident_id")
        if incident_id and not any(
            item["id"] == incident_id and item.get("run_id") == run_id
            for item in self.incidents
        ):
            raise StorageValidationError(
                "ground-truth incident does not belong to the experiment"
            )
        existing = next(
            (
                item
                for item in self.labels
                if all(
                    item.get(key) == record.get(key)
                    for key in (
                        "run_id",
                        "incident_id",
                        "entity_type",
                        "entity_id",
                        "fault_class",
                    )
                )
            ),
            None,
        )
        if existing is not None:
            existing.update(record)
            return existing
        self.labels.append(record)
        return record

    async def list_incidents(self, *, status, limit, offset):
        records = self.incidents
        if status:
            records = [record for record in records if record["status"] == status]
        return records[offset : offset + limit]

    async def get_incident(self, incident_id):
        return next((item for item in self.incidents if item["id"] == incident_id), None)

    async def get_candidates(self, incident_id):
        return [{"incident_id": incident_id, "rank": 1, "candidate": "paymentservice"}]

    async def get_evidence(self, incident_id):
        return [{"incident_id": incident_id, "kind": "trace", "supports": True}]

    async def get_recommendations(self, incident_id):
        return [
            {
                "incident_id": incident_id,
                "candidate_rank": 1,
                "runbook_key": "dependency-unreachable-v1",
            }
        ]

    async def get_graph(self, incident_id):
        return {
            "incident_id": incident_id,
            "nodes": [{"id": "checkoutservice"}, {"id": "paymentservice"}],
            "edges": [{"source": "checkoutservice", "target": "paymentservice"}],
        }

    async def create_feedback(self, incident_id: str, feedback: FeedbackRequest):
        record = {
            "id": len(self.feedback) + 1,
            "incident_id": incident_id,
            **feedback.model_dump(mode="json"),
        }
        self.feedback.append(record)
        return record


@pytest.fixture
def fake_storage() -> FakeStorage:
    return FakeStorage()


@pytest.fixture
def client(fake_storage: FakeStorage):
    settings = Settings(
        database_url="postgresql://test:test@localhost:5432/test",
        max_request_body_bytes=1024 * 1024,
        max_batch_events=100,
    )
    app = create_app(settings=settings, storage=fake_storage)
    with TestClient(app) as test_client:
        yield test_client
