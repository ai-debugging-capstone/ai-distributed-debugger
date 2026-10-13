"""Actual API -> normalizer -> Psycopg -> PostgreSQL; payloads are synthetic OTLP."""

import base64
from datetime import UTC, datetime
from uuid import UUID

import pytest
from psycopg import sql

from tests.telemetry_samples import (
    ATTRIBUTES, RESOURCE_ATTRIBUTES, RUN_ID, SPAN_ID, TRACE_ID, make_sample,
)

pytestmark = pytest.mark.postgres


def post_sample(harness, sample):
    return harness.client.post(
        "/v1/" + sample.endpoint, content=sample.request.SerializeToString(),
        headers={"Content-Type": "application/x-protobuf"},
    )


def rows(harness, table):
    return harness.connection.execute(sql.SQL("SELECT * FROM {}").format(
        sql.Identifier(table)
    )).fetchall()


@pytest.mark.parametrize("signal", ["span", "metric", "log"])
def test_otlp_round_trip_preserves_metadata_and_deduplicates(postgres_api, signal):
    sample = make_sample(signal)
    first = post_sample(postgres_api, sample)
    assert first.status_code == 200, first.text
    assert first.headers["x-otlp-inserted"] == "1"
    stored = rows(postgres_api, sample.table)
    assert len(stored) == 1
    row = stored[0]
    assert row["service_name"] == "payment"
    assert row["resource_attributes"] == RESOURCE_ATTRIBUTES
    assert row["attributes"] == ATTRIBUTES
    assert row["run_id"] == UUID(RUN_ID)
    when = row["start_time"] if signal == "span" else row["time_unix"]
    assert when == datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)
    if signal in {"span", "log"}:
        assert (row["trace_id"], row["span_id"]) == (TRACE_ID, SPAN_ID)
    if signal == "span":
        assert row["duration_ms"] == 250.0
        assert row["parent_span_id"] == "8899aabbccddeeff"
        assert (row["scope_name"], row["scope_version"]) == ("m5-fixture", "1.0")
        assert row["end_time"] == when.replace(microsecond=250000)
    elif signal == "log":
        assert (row["severity_number"], row["severity_text"]) == (17, "ERROR")
        assert row["observed_time"] == when.replace(second=21)
        assert row["body"] == sample.item.body.string_value
    else:
        assert (row["metric_name"], row["value"]) == ("process.cpu.utilization", 0.5)
        assert row["exemplars"][0]["trace_id"] == base64.b64encode(
            bytes.fromhex(TRACE_ID)
        ).decode()
    assert row["raw"]
    assert rows(postgres_api, "rejected_records") == []
    second = post_sample(postgres_api, sample)
    assert second.status_code == 200
    assert second.headers["x-otlp-inserted"] == "0"
    assert second.headers["x-otlp-duplicates"] == "1"
    assert rows(postgres_api, sample.table) == stored
    # The real read API sees the placeholder and signal counts, not a fake response.
    summary = postgres_api.client.get(f"/api/v1/experiments/{RUN_ID}/capture-summary")
    assert summary.status_code == 200
    assert summary.json()["telemetry"][sample.table] == 1


@pytest.mark.parametrize("signal", ["span", "metric", "log"])
def test_malformed_sibling_is_quarantined_in_the_same_transaction(postgres_api, signal):
    sample = make_sample(signal)
    bad = sample.siblings.add()
    bad.CopyFrom(sample.item)
    if signal == "span":
        bad.parent_span_id = b"bad"
    elif signal == "log":
        bad.trace_id = b"bad"
    else:
        bad.ClearField("as_double")
    response = post_sample(postgres_api, sample)
    assert response.status_code == 200
    assert response.headers["x-otlp-inserted"] == "1"
    assert response.headers["x-otlp-rejected"] == "1"
    parsed = sample.response_type.FromString(response.content)
    assert getattr(parsed.partial_success, sample.partial_field) == 1
    assert len(rows(postgres_api, sample.table)) == 1
    rejected = rows(postgres_api, "rejected_records")
    assert len(rejected) == 1
    assert rejected[0]["signal_type"] == signal
    assert "normalization_error" in rejected[0]["reason"]
    assert rejected[0]["payload"]["service_name"] == "payment"


def test_malformed_protobuf_is_persisted_as_a_rejection(postgres_api):
    response = postgres_api.client.post(
        "/v1/logs", content=b"\xff\xff\xff",
        headers={"Content-Type": "application/x-protobuf"},
    )
    assert response.status_code == 400
    assert rows(postgres_api, "log_records") == []
    rejected = rows(postgres_api, "rejected_records")
    assert len(rejected) == 1
    assert rejected[0]["reason"] == "protobuf_decode_error"


def test_normalized_json_batch_round_trip_and_atomic_validation(postgres_api):
    batch = {"run_id": RUN_ID, "events": [{
        "signal": "log", "service_name": "payment", "time_unix": "2023-11-14T22:13:20Z",
        "trace_id": TRACE_ID.upper(), "span_id": SPAN_ID, "severity_number": 17,
        "body": "Synthetic normalized JSON fixture", "resource_attributes": RESOURCE_ATTRIBUTES,
        "attributes": ATTRIBUTES,
    }]}
    first = postgres_api.client.post("/api/v1/events/batch", json=batch)
    assert first.status_code == 200, first.text
    assert first.json()["inserted"]["log"] == 1
    row = rows(postgres_api, "log_records")[0]
    assert row["trace_id"] == TRACE_ID
    assert row["resource_attributes"] == RESOURCE_ATTRIBUTES
    assert row["attributes"] == ATTRIBUTES
    assert row["run_id"] == UUID(RUN_ID)
    second = postgres_api.client.post("/api/v1/events/batch", json=batch)
    assert second.json()["duplicates"]["log"] == 1
    batch["events"].append({**batch["events"][0], "trace_id": "bad"})
    invalid = postgres_api.client.post("/api/v1/events/batch", json=batch)
    assert invalid.status_code == 422
    assert len(rows(postgres_api, "log_records")) == 1


@pytest.mark.parametrize("signal", ["log", "metric"])
def test_retries_without_source_timestamps_are_deduplicated(postgres_api, signal):
    sample = make_sample(signal)
    sample.item.time_unix_nano = 0
    if signal == "log":
        sample.item.observed_time_unix_nano = 0
    assert post_sample(postgres_api, sample).headers["x-otlp-inserted"] == "1"
    second = post_sample(postgres_api, sample)
    assert second.status_code == 200
    assert second.headers["x-otlp-duplicates"] == "1"
    assert len(rows(postgres_api, sample.table)) == 1


def test_jsonb_storage_failure_rolls_back_valid_sibling_and_run_placeholder(postgres_api):
    # Characterize an unresolved defect explicitly: OTLP nonfinite attributes
    # reach JSONB and return 503 instead of per-record quarantine. This test
    # checks transaction atomicity, not successful malformed-record handling.
    sample = make_sample("log")
    bad = sample.siblings.add()
    bad.CopyFrom(sample.item)
    bad.attributes.add(key="nonfinite").value.double_value = float("nan")
    response = post_sample(postgres_api, sample)
    assert response.status_code == 503
    assert response.json()["detail"] == "telemetry could not be persisted"
    assert rows(postgres_api, "log_records") == []
    assert rows(postgres_api, "rejected_records") == []
    assert postgres_api.connection.execute(
        "SELECT id FROM experiment_runs WHERE id = %s", (RUN_ID,)
    ).fetchone() is None
    # The database/API remain usable after rollback.
    del sample.siblings[1:]
    assert post_sample(postgres_api, sample).headers["x-otlp-inserted"] == "1"
