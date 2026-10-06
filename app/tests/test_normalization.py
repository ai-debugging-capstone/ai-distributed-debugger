from __future__ import annotations

from datetime import UTC, datetime

import pytest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from telemetry_api.normalization import (
    normalize_log_request,
    normalize_metric_request,
    normalize_trace_request,
)


def _add_string_attribute(attributes, key: str, value: str) -> None:
    item = attributes.add()
    item.key = key
    item.value.string_value = value


def test_trace_normalization_preserves_correlation_and_resource_identity():
    request = ExportTraceServiceRequest()
    resource_spans = request.resource_spans.add()
    _add_string_attribute(resource_spans.resource.attributes, "service.name", "checkoutservice")
    _add_string_attribute(
        resource_spans.resource.attributes,
        "experiment.run_id",
        "970c8464-87bf-42d2-a25f-cff3b01f1001",
    )
    scope_spans = resource_spans.scope_spans.add()
    scope_spans.scope.name = "otel-test"
    scope_spans.scope.version = "1.0"
    span = scope_spans.spans.add()
    span.trace_id = bytes.fromhex("00112233445566778899aabbccddeeff")
    span.span_id = bytes.fromhex("0011223344556677")
    span.parent_span_id = bytes.fromhex("8899aabbccddeeff")
    span.name = "POST /checkout"
    span.kind = 2
    span.start_time_unix_nano = 1_700_000_000_000_000_000
    span.end_time_unix_nano = 1_700_000_000_125_000_000
    span.status.code = 2
    span.status.message = "dependency timeout"
    _add_string_attribute(span.attributes, "error.type", "DeadlineExceeded")

    records, rejected = normalize_trace_request(request)

    assert rejected == []
    assert len(records) == 1
    record = records[0]
    assert record["service_name"] == "checkoutservice"
    assert record["trace_id"] == "00112233445566778899aabbccddeeff"
    assert record["span_kind"] == "SERVER"
    assert record["status_code"] == "ERROR"
    assert record["duration_ms"] == 125.0
    assert str(record["run_id"]) == "970c8464-87bf-42d2-a25f-cff3b01f1001"


def test_metric_normalization_expands_each_data_point_and_hashes_it():
    request = ExportMetricsServiceRequest()
    resource_metrics = request.resource_metrics.add()
    _add_string_attribute(resource_metrics.resource.attributes, "service.name", "frontend")
    metric = resource_metrics.scope_metrics.add().metrics.add()
    metric.name = "http.server.active_requests"
    point = metric.gauge.data_points.add()
    point.time_unix_nano = 1_700_000_000_000_000_000
    point.as_int = 7
    _add_string_attribute(point.attributes, "http.route", "/")

    records, rejected = normalize_metric_request(request)

    assert rejected == []
    assert len(records) == 1
    assert records[0]["metric_type"] == "gauge"
    assert records[0]["value"] == 7.0
    assert len(records[0]["idempotency_key"]) == 64


def _one_gauge_metric(
    resource_attributes: dict[str, str],
    point_attributes: dict[str, str] | None = None,
):
    request = ExportMetricsServiceRequest()
    resource_metrics = request.resource_metrics.add()
    for key, value in resource_attributes.items():
        _add_string_attribute(resource_metrics.resource.attributes, key, value)
    metric = resource_metrics.scope_metrics.add().metrics.add()
    metric.name = "test.metric"
    point = metric.gauge.data_points.add()
    point.time_unix_nano = 1_700_000_000_000_000_000
    point.as_int = 1
    for key, value in (point_attributes or {}).items():
        _add_string_attribute(point.attributes, key, value)
    records, rejected = normalize_metric_request(request)
    assert rejected == []
    assert len(records) == 1
    return records[0]


def test_metric_identity_prefers_explicit_service_name_over_container_name():
    record = _one_gauge_metric(
        {"service.name": "checkout", "container.name": "payment"}
    )

    assert record["service_name"] == "checkout"


def test_metric_identity_uses_container_name_and_removes_docker_slash_prefix():
    record = _one_gauge_metric({"container.name": " /payment "})

    assert record["service_name"] == "payment"


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("k8s.deployment.name", "checkout"),
        ("server.address", "astronomy-db"),
        ("postgresql.database.name", "astronomy"),
        ("kafka.cluster.alias", "kafka"),
        ("host.name", "demo-host"),
    ],
)
def test_metric_identity_uses_stable_resource_semantic_conventions(
    attribute: str, value: str
):
    record = _one_gauge_metric({attribute: value})

    assert record["service_name"] == value


def test_metric_identity_uses_point_semantics_only_without_resource_identity():
    record = _one_gauge_metric(
        {}, {"service.name": "ad", "host.name": "collector-host"}
    )

    assert record["service_name"] == "ad"


def test_metric_identity_resource_entity_wins_over_point_entity():
    record = _one_gauge_metric(
        {"container.name": "payment"}, {"service.name": "incorrect-label"}
    )

    assert record["service_name"] == "payment"


def test_log_normalization_uses_observed_time_when_event_time_is_missing():
    request = ExportLogsServiceRequest()
    resource_logs = request.resource_logs.add()
    _add_string_attribute(resource_logs.resource.attributes, "service.name", "paymentservice")
    record = resource_logs.scope_logs.add().log_records.add()
    record.observed_time_unix_nano = 1_700_000_000_000_000_000
    record.severity_number = 17
    record.severity_text = "ERROR"
    record.body.string_value = "payment timed out"

    records, rejected = normalize_log_request(request)

    assert rejected == []
    assert records[0]["time_unix"] == datetime.fromtimestamp(
        1_700_000_000, tz=UTC
    )
    assert records[0]["body"] == "payment timed out"
    assert records[0]["severity_number"] == 17


def test_invalid_span_is_quarantined_without_losing_valid_sibling():
    request = ExportTraceServiceRequest()
    resource_spans = request.resource_spans.add()
    _add_string_attribute(resource_spans.resource.attributes, "service.name", "frontend")
    spans = resource_spans.scope_spans.add().spans
    invalid = spans.add()
    invalid.name = "invalid"
    valid = spans.add()
    valid.trace_id = bytes.fromhex("00112233445566778899aabbccddeeff")
    valid.span_id = bytes.fromhex("0011223344556677")
    valid.name = "valid"
    valid.start_time_unix_nano = 1_700_000_000_000_000_000
    valid.end_time_unix_nano = 1_700_000_001_000_000_000

    records, rejected = normalize_trace_request(request)

    assert [record["operation_name"] for record in records] == ["valid"]
    assert len(rejected) == 1
    assert rejected[0]["signal_type"] == "span"
