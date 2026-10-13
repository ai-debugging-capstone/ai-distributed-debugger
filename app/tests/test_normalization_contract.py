"""M5 contracts not covered by the original normalization examples."""

import base64
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from telemetry_api.models import NormalizedBatch
from telemetry_api.normalization import normalized_batch_to_records
from tests.telemetry_samples import (
    ATTRIBUTES, NANOS, RESOURCE_ATTRIBUTES, RUN_ID, SPAN_ID, TRACE_ID,
    add_attributes, make_sample, set_value,
)


@pytest.mark.parametrize("signal", ["span", "metric", "log"])
def test_metadata_and_recursive_attribute_types_survive_normalization(signal):
    sample = make_sample(signal)
    rows, rejected = sample.normalize(sample.request)
    assert not rejected
    row = rows[0]
    assert row["service_name"] == "payment"
    assert row["resource_attributes"] == RESOURCE_ATTRIBUTES
    assert row["attributes"] == ATTRIBUTES
    assert row["run_id"] == UUID(RUN_ID)
    when = row["start_time"] if signal == "span" else row["time_unix"]
    assert when.tzinfo == UTC
    if signal == "span":
        assert (row["scope_name"], row["scope_version"]) == ("m5-fixture", "1.0")
        assert row["raw"]["start_time_unix_nano"] == str(NANOS)
    elif signal == "log":
        assert (row["trace_id"], row["span_id"]) == (TRACE_ID, SPAN_ID)
        assert (row["severity_number"], row["severity_text"]) == (17, "ERROR")
        assert row["raw"]["time_unix_nano"] == str(NANOS)


@pytest.mark.parametrize("metric_type", [
    "gauge", "sum", "histogram", "exponential_histogram", "summary",
])
def test_every_supported_metric_type_retains_its_measurements(metric_type):
    sample = make_sample("metric", metric_type=metric_type)
    rows, rejected = sample.normalize(sample.request)
    assert not rejected
    row = rows[0]
    assert row["metric_type"] == metric_type
    assert row["raw"]["metric"]["unit"] == "1"
    if metric_type != "summary":
        exemplar = row["exemplars"][0]
        assert exemplar["trace_id"] == base64.b64encode(bytes.fromhex(TRACE_ID)).decode()
        assert exemplar["span_id"] == base64.b64encode(bytes.fromhex(SPAN_ID)).decode()
        assert exemplar["time_unix_nano"] == str(NANOS)
    if metric_type in {"gauge", "sum"}:
        assert row["value"] == 0.5
    else:
        assert (row["count"], row["sum"]) == (2, 4.0)
        if metric_type != "summary":
            assert (row["min"], row["max"]) == (1.0, 3.0)
        point = row["raw"]["data_point"]
        if metric_type == "histogram":
            assert point["bucket_counts"] == ["1", "1"]
            assert point["explicit_bounds"] == [2.0]
        elif metric_type == "exponential_histogram":
            assert point["positive"]["bucket_counts"] == ["1", "1"]
        else:
            assert point["quantile_values"] == [{"quantile": 0.5, "value": 2.0}]


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("trace_id", b"short", "trace_id"),
    ("span_id", b"short", "span_id"),
    ("parent_span_id", b"short", "parent_span_id"),
    ("start_time_unix_nano", 0, "timestamp"),
    ("end_time_unix_nano", 0, "timestamp"),
    ("end_time_unix_nano", NANOS - 1_000_000_000, "precedes"),
])
def test_bad_span_is_rejected_but_valid_sibling_survives(field, value, reason):
    sample = make_sample("span")
    bad = sample.siblings.add()
    bad.CopyFrom(sample.item)
    setattr(bad, field, value)
    rows, rejected = sample.normalize(sample.request)
    assert len(rows) == len(rejected) == 1
    assert rows[0]["trace_id"] == TRACE_ID
    assert reason in rejected[0]["reason"]
    assert rejected[0]["signal_type"] == "span"
    assert rejected[0]["payload"]["service_name"] == "payment"


@pytest.mark.parametrize("field", ["trace_id", "span_id"])
def test_bad_log_correlation_id_is_quarantined(field):
    sample = make_sample("log")
    bad = sample.siblings.add()
    bad.CopyFrom(sample.item)
    setattr(bad, field, b"short")
    rows, rejected = sample.normalize(sample.request)
    assert len(rows) == len(rejected) == 1
    assert field in rejected[0]["reason"]


@pytest.mark.parametrize("signal", ["span", "metric", "log"])
def test_missing_resource_identity_and_invalid_run_id_have_safe_defaults(signal):
    sample = make_sample(signal)
    sample.resource.ClearField("attributes")
    add_attributes(sample.resource.attributes, {
        "service.name": {"unexpected": True}, "experiment.run_id": "not-a-uuid",
    })
    rows, rejected = sample.normalize(sample.request)
    assert not rejected
    assert rows[0]["service_name"] == "unknown_service"
    assert rows[0]["run_id"] is None
    assert rows[0]["resource_attributes"]["service.name"] == {"unexpected": True}


def test_empty_log_body_and_optional_correlation_ids_are_accepted():
    sample = make_sample("log")
    for field in ("body", "trace_id", "span_id", "severity_text"):
        sample.item.ClearField(field)
    sample.item.severity_number = 0
    rows, rejected = sample.normalize(sample.request)
    assert not rejected
    assert (rows[0]["body"], rows[0]["trace_id"], rows[0]["span_id"]) == ("", None, None)
    assert (rows[0]["severity_number"], rows[0]["severity_text"]) == (0, None)


def test_duplicate_attribute_keys_and_structured_log_body_are_deterministic():
    sample = make_sample("log")
    add_attributes(sample.item.attributes, {"attempts": 8})
    sample.item.body.Clear()
    set_value(sample.item.body, {"z": [False, None], "a": 3})
    first, rejected = sample.normalize(sample.request)
    second, _ = sample.normalize(sample.request)
    assert not rejected
    assert first[0]["attributes"]["attempts"] == 8
    assert json.loads(first[0]["body"]) == {"a": 3, "z": [False, None]}
    assert first[0]["idempotency_key"] == second[0]["idempotency_key"]


@pytest.mark.parametrize(("signal", "event_time", "observed_time"), [
    ("log", 0, 0), ("log", NANOS, 0), ("metric", 0, None),
])
def test_timestamp_fallback_does_not_change_retry_identity(
    signal, event_time, observed_time, monkeypatch,
):
    sample = make_sample(signal)
    sample.item.time_unix_nano = event_time
    if observed_time is not None:
        sample.item.observed_time_unix_nano = observed_time
    clock = datetime(2026, 1, 1, tzinfo=UTC)
    monkeypatch.setattr("telemetry_api.normalization.utc_now", lambda: clock)
    first, rejected = sample.normalize(sample.request)
    clock += timedelta(hours=1)
    second, _ = sample.normalize(sample.request)
    assert not rejected
    assert first[0]["idempotency_key"] == second[0]["idempotency_key"]
    if event_time == 0:
        assert first[0]["time_unix"] == datetime(2026, 1, 1, tzinfo=UTC)
        assert second[0]["time_unix"] == clock


def test_empty_gauge_is_rejected_without_losing_valid_point():
    sample = make_sample("metric")
    bad = sample.siblings.add()
    bad.CopyFrom(sample.item)
    bad.ClearField("as_double")
    rows, rejected = sample.normalize(sample.request)
    assert len(rows) == len(rejected) == 1
    assert "value" in rejected[0]["reason"]


@pytest.mark.parametrize("signal", ["span", "metric", "log"])
def test_duplicate_normalization_does_not_discard_records_before_storage(signal):
    sample = make_sample(signal)
    sample.siblings.add().CopyFrom(sample.item)
    rows, rejected = sample.normalize(sample.request)
    assert not rejected
    assert len(rows) == 2  # Storage, not normalization, owns deduplication.
    assert rows[0] == rows[1]


@pytest.mark.parametrize("signal", ["span", "metric", "log"])
def test_otlp_and_normalized_json_share_the_semantic_row_contract(signal):
    sample = make_sample(signal)
    rows, rejected = sample.normalize(sample.request)
    assert not rejected
    row = rows[0]
    event = {key: value for key, value in row.items()
             if key not in {"observed_at", "idempotency_key", "duration_ms"}}
    batch = NormalizedBatch.model_validate({"events": [{"signal": signal, **event}]})
    converted, rejected = normalized_batch_to_records(batch)
    assert not rejected
    comparable = {key for key in row if key not in {"observed_at", "idempotency_key"}}
    assert {key: converted[signal][0][key] for key in comparable} == {
        key: row[key] for key in comparable
    }


@pytest.mark.parametrize(("signal", "missing_field"), [
    ("span", "service_name"), ("span", "start_time"),
    ("metric", "time_unix"), ("log", "time_unix"),
])
def test_normalized_json_requires_its_documented_metadata(signal, missing_field):
    sample = make_sample(signal)
    row = sample.normalize(sample.request)[0][0]
    event = {key: value for key, value in row.items()
             if key not in {"observed_at", "idempotency_key", "duration_ms", missing_field}}
    with pytest.raises(ValidationError):
        NormalizedBatch.model_validate({"events": [{"signal": signal, **event}]})
