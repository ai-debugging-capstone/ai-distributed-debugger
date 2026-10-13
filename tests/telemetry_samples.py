"""Small, explicitly synthetic OTLP messages shared by unit and database tests."""

from dataclasses import dataclass
from typing import Any, Callable

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
    ExportLogsServiceResponse,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)

from telemetry_api.normalization import (
    normalize_log_request,
    normalize_metric_request,
    normalize_trace_request,
)

NANOS = 1_700_000_000_000_000_000
TRACE_ID = "00112233445566778899aabbccddeeff"
SPAN_ID = "0011223344556677"
RUN_ID = "970c8464-87bf-42d2-a25f-cff3b01f1001"
RESOURCE_ATTRIBUTES = {
    "service.name": "payment",
    "host.name": "m5-test-host",
    "service.version": "1.2.3",
    "service.namespace": "capstone",
    "service.instance.id": "fixture-1",
    "experiment.run_id": RUN_ID,
    "deployment.labels": {"team": 4, "local": True},
}
ATTRIBUTES = {
    "http.route": "/m5",
    "attempts": 3,
    "cached": False,
    "ratio": 0.25,
    "payload": {"items": ["one", 2, True, None], "bytes": "0102"},
}


def set_value(target, value):
    if value is None:
        return
    if isinstance(value, bool):
        target.bool_value = value
    elif isinstance(value, str):
        target.string_value = value
    elif isinstance(value, int):
        target.int_value = value
    elif isinstance(value, float):
        target.double_value = value
    elif isinstance(value, bytes):
        target.bytes_value = value
    elif isinstance(value, list):
        for item in value:
            set_value(target.array_value.values.add(), item)
    elif isinstance(value, dict):
        add_attributes(target.kvlist_value.values, value)
    else:
        raise TypeError(type(value))


def add_attributes(target, attributes):
    for key, value in attributes.items():
        item = target.add(key=key)
        set_value(item.value, value)


@dataclass
class Sample:
    request: Any
    resource: Any
    item: Any
    siblings: Any
    normalize: Callable
    endpoint: str
    table: str
    response_type: Any
    partial_field: str


def make_sample(signal, *, metric_type="gauge") -> Sample:
    if signal == "span":
        request = ExportTraceServiceRequest()
        group = request.resource_spans.add()
        scope = group.scope_spans.add()
        scope.scope.name = "m5-fixture"
        scope.scope.version = "1.0"
        siblings = scope.spans
        item = siblings.add(
            trace_id=bytes.fromhex(TRACE_ID), span_id=bytes.fromhex(SPAN_ID),
            parent_span_id=bytes.fromhex("8899aabbccddeeff"), name="POST /m5",
            kind=2, start_time_unix_nano=NANOS, end_time_unix_nano=NANOS + 250_000_000,
        )
        item.status.code = 2
        sample = Sample(request, group.resource, item, siblings, normalize_trace_request,
                        "traces", "spans", ExportTraceServiceResponse, "rejected_spans")
    elif signal == "metric":
        request = ExportMetricsServiceRequest()
        group = request.resource_metrics.add()
        metric = group.scope_metrics.add().metrics.add(
            name="process.cpu.utilization", unit="1", description="Synthetic test metric"
        )
        data = getattr(metric, metric_type)
        siblings = data.data_points
        item = siblings.add(time_unix_nano=NANOS, start_time_unix_nano=NANOS - 1_000_000_000)
        if metric_type in {"gauge", "sum"}:
            item.as_double = 0.5
        else:
            item.count = 2
            item.sum = 4.0
            if metric_type in {"histogram", "exponential_histogram"}:
                item.min = 1.0
                item.max = 3.0
            if metric_type == "histogram":
                item.explicit_bounds.append(2.0)
                item.bucket_counts.extend([1, 1])
            elif metric_type == "exponential_histogram":
                item.positive.bucket_counts.extend([1, 1])
            else:
                item.quantile_values.add(quantile=0.5, value=2.0)
        if hasattr(item, "exemplars"):
            item.exemplars.add(
                trace_id=bytes.fromhex(TRACE_ID), span_id=bytes.fromhex(SPAN_ID),
                time_unix_nano=NANOS, as_double=0.5,
            )
        sample = Sample(request, group.resource, item, siblings, normalize_metric_request,
                        "metrics", "metric_points", ExportMetricsServiceResponse,
                        "rejected_data_points")
    elif signal == "log":
        request = ExportLogsServiceRequest()
        group = request.resource_logs.add()
        siblings = group.scope_logs.add().log_records
        item = siblings.add(
            time_unix_nano=NANOS, observed_time_unix_nano=NANOS + 1_000_000_000,
            severity_number=17, severity_text="ERROR",
            trace_id=bytes.fromhex(TRACE_ID), span_id=bytes.fromhex(SPAN_ID),
        )
        item.body.string_value = "Synthetic M5 test log; not captured demo traffic"
        sample = Sample(request, group.resource, item, siblings, normalize_log_request,
                        "logs", "log_records", ExportLogsServiceResponse, "rejected_log_records")
    else:
        raise ValueError(signal)
    add_attributes(group.resource.attributes, RESOURCE_ATTRIBUTES)
    add_attributes(item.attributes, {**ATTRIBUTES, "payload": {
        "items": ["one", 2, True, None], "bytes": b"\x01\x02",
    }})
    return sample
