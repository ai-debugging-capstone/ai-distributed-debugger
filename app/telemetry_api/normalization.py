"""Translate OTLP protobuf messages into the platform's relational records."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message

from .models import NormalizedBatch, NormalizedLog, NormalizedMetric, NormalizedSpan

SPAN_KINDS = {
    0: "UNSPECIFIED",
    1: "INTERNAL",
    2: "SERVER",
    3: "CLIENT",
    4: "PRODUCER",
    5: "CONSUMER",
}

STATUS_CODES = {0: "UNSET", 1: "OK", 2: "ERROR"}

UNKNOWN_SERVICE = "unknown_service"

# Receiver-produced infrastructure metrics often do not carry service.name.
# These resource attributes identify the entity being measured without relying
# on metric values or high-cardinality identifiers. Keep container.name first:
# in the Docker Compose demo it matches the application service names used by
# traces (for example, ``payment`` and ``checkout``), which lets the RCA engine
# correlate container pressure with the correct service.
RESOURCE_ENTITY_NAME_KEYS = (
    "container.name",
    "k8s.container.name",
    "k8s.deployment.name",
    "k8s.statefulset.name",
    "k8s.daemonset.name",
    "k8s.job.name",
    "k8s.cronjob.name",
    "k8s.pod.name",
    "faas.name",
    "server.address",
    "postgresql.database.name",
    "kafka.cluster.alias",
    "db.system.name",
    "db.system",
    "messaging.system",
    "host.name",
)

# Some bridges encode identifying resource labels on each data point. This is
# a lower-priority fallback: a real OTLP resource identity always wins.
METRIC_POINT_ENTITY_NAME_KEYS = (
    "service.name",
    *RESOURCE_ENTITY_NAME_KEYS,
    "process.executable.name",
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def nanos_to_datetime(value: int, *, fallback: datetime | None = None) -> datetime:
    """Convert an OTLP nanosecond timestamp to an aware UTC datetime."""

    if not value:
        if fallback is None:
            raise ValueError("missing OTLP timestamp")
        return fallback
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC)


def bytes_to_hex(value: bytes) -> str | None:
    return value.hex() if value else None


def any_value_to_python(value: Any) -> Any:
    """Convert a protobuf ``AnyValue`` recursively without losing types."""

    kind = value.WhichOneof("value")
    if kind is None:
        return None
    if kind == "array_value":
        return [any_value_to_python(item) for item in value.array_value.values]
    if kind == "kvlist_value":
        return key_values_to_dict(value.kvlist_value.values)
    if kind == "bytes_value":
        return value.bytes_value.hex()
    return getattr(value, kind)


def key_values_to_dict(values: Iterable[Any]) -> dict[str, Any]:
    # Later duplicate keys intentionally win, matching common SDK behavior.
    return {item.key: any_value_to_python(item.value) for item in values}


def _message_to_dict(message: Message) -> dict[str, Any]:
    return MessageToDict(message, preserving_proto_field_name=True)


def _scope_fields(scope: Any) -> tuple[str | None, str | None]:
    name = getattr(scope, "name", "") or None
    version = getattr(scope, "version", "") or None
    return name, version


def _identity_attribute(
    attributes: dict[str, Any], keys: Iterable[str]
) -> str | None:
    """Return the first valid, stable entity name in ``keys``.

    OpenTelemetry semantic-convention identity fields are strings. Ignoring
    arrays, maps, booleans, and numbers prevents malformed telemetry from
    creating surprising service buckets such as ``"True"`` or a serialized
    attribute map. Docker APIs sometimes prefix container names with ``/``;
    stripping only that transport artifact makes them line up with trace
    service names while preserving all other values exactly.
    """

    for key in keys:
        value = attributes.get(key)
        if not isinstance(value, str):
            continue
        candidate = value.strip()
        if key in {"container.name", "k8s.container.name"}:
            candidate = candidate.lstrip("/")
        if candidate:
            return candidate
    return None


def _resource_identity(attributes: dict[str, Any]) -> tuple[str, str | None, str | None]:
    service_name = _identity_attribute(attributes, ("service.name",))
    namespace = attributes.get("service.namespace")
    instance_id = attributes.get("service.instance.id")
    return (
        service_name or UNKNOWN_SERVICE,
        str(namespace) if namespace is not None else None,
        str(instance_id) if instance_id is not None else None,
    )


def _metric_service_name(
    resource_attributes: dict[str, Any], point_attributes: dict[str, Any]
) -> str:
    resource_service_name = _identity_attribute(resource_attributes, ("service.name",))
    if resource_service_name is not None:
        return resource_service_name

    resource_entity_name = _identity_attribute(
        resource_attributes, RESOURCE_ENTITY_NAME_KEYS
    )
    if resource_entity_name is not None:
        return resource_entity_name

    return _identity_attribute(point_attributes, METRIC_POINT_ENTITY_NAME_KEYS) or UNKNOWN_SERVICE


def _extract_run_id(*attribute_sets: dict[str, Any]) -> UUID | None:
    for attributes in attribute_sets:
        for key in ("experiment.run_id", "telemetry.run_id", "run.id"):
            candidate = attributes.get(key)
            if candidate is None:
                continue
            try:
                return UUID(str(candidate))
            except (ValueError, TypeError, AttributeError):
                continue
    return None


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, UUID)):
        return str(value)
    raise TypeError(f"cannot encode {type(value)!r}")


def idempotency_key(record: dict[str, Any]) -> str:
    excluded = {"idempotency_key", "observed_at"}
    stable = {key: value for key, value in record.items() if key not in excluded}
    encoded = json.dumps(
        stable,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _rejection(signal_type: str, reason: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal_type": signal_type,
        "reason": reason[:4000],
        "payload": payload,
        "received_at": utc_now(),
    }


def normalize_trace_request(request: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    observed_at = utc_now()

    for resource_spans in request.resource_spans:
        resource_attributes = key_values_to_dict(resource_spans.resource.attributes)
        service_name, namespace, instance_id = _resource_identity(resource_attributes)
        for scope_spans in resource_spans.scope_spans:
            scope_name, scope_version = _scope_fields(scope_spans.scope)
            for span in scope_spans.spans:
                try:
                    trace_id = bytes_to_hex(span.trace_id)
                    span_id = bytes_to_hex(span.span_id)
                    if trace_id is None or len(trace_id) != 32:
                        raise ValueError("trace_id must contain 16 bytes")
                    if span_id is None or len(span_id) != 16:
                        raise ValueError("span_id must contain 8 bytes")
                    start_time = nanos_to_datetime(span.start_time_unix_nano)
                    end_time = nanos_to_datetime(span.end_time_unix_nano)
                    if end_time < start_time:
                        raise ValueError("span end time precedes start time")
                    attributes = key_values_to_dict(span.attributes)
                    parent_span_id = bytes_to_hex(span.parent_span_id)
                    record = {
                        "trace_id": trace_id,
                        "span_id": span_id,
                        "parent_span_id": parent_span_id,
                        "service_name": service_name,
                        "service_namespace": namespace,
                        "service_instance_id": instance_id,
                        "operation_name": span.name or "unnamed",
                        "span_kind": SPAN_KINDS.get(int(span.kind), f"UNKNOWN_{int(span.kind)}"),
                        "start_time": start_time,
                        "end_time": end_time,
                        "duration_ms": (end_time - start_time).total_seconds() * 1000.0,
                        "status_code": STATUS_CODES.get(
                            int(span.status.code), f"UNKNOWN_{int(span.status.code)}"
                        ),
                        "status_message": span.status.message or None,
                        "attributes": attributes,
                        "resource_attributes": resource_attributes,
                        "scope_name": scope_name,
                        "scope_version": scope_version,
                        "run_id": _extract_run_id(attributes, resource_attributes),
                        "observed_at": observed_at,
                        "raw": _message_to_dict(span),
                    }
                    records.append(record)
                except Exception as exc:  # One malformed span must not drop its siblings.
                    rejected.append(
                        _rejection(
                            "span",
                            f"normalization_error: {exc}",
                            {"record": _message_to_dict(span), "service_name": service_name},
                        )
                    )
    return records, rejected


def _optional_scalar(message: Any, field: str) -> float | None:
    if not hasattr(message, field):
        return None
    try:
        if not message.HasField(field):
            return None
    except ValueError:
        # Older protobuf definitions did not mark these scalar fields optional.
        pass
    return float(getattr(message, field))


def _number_value(point: Any) -> float | None:
    kind = point.WhichOneof("value")
    if kind == "as_double":
        return float(point.as_double)
    if kind == "as_int":
        return float(point.as_int)
    return None


def normalize_metric_request(request: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    observed_at = utc_now()

    for resource_metrics in request.resource_metrics:
        resource_attributes = key_values_to_dict(resource_metrics.resource.attributes)
        resource_service_name = _metric_service_name(resource_attributes, {})
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                metric_type = metric.WhichOneof("data")
                if metric_type is None:
                    rejected.append(
                        _rejection(
                            "metric",
                            "normalization_error: metric has no data type",
                            {
                                "metric": _message_to_dict(metric),
                                "service_name": resource_service_name,
                            },
                        )
                    )
                    continue
                data = getattr(metric, metric_type)
                for point in data.data_points:
                    try:
                        attributes = key_values_to_dict(point.attributes)
                        service_name = _metric_service_name(
                            resource_attributes, attributes
                        )
                        point_time = nanos_to_datetime(
                            int(point.time_unix_nano), fallback=observed_at
                        )
                        start_time = (
                            nanos_to_datetime(int(point.start_time_unix_nano))
                            if int(getattr(point, "start_time_unix_nano", 0))
                            else None
                        )
                        value: float | None = None
                        count: int | None = None
                        total: float | None = None
                        minimum: float | None = None
                        maximum: float | None = None
                        if metric_type in {"gauge", "sum"}:
                            value = _number_value(point)
                        elif metric_type in {"histogram", "exponential_histogram"}:
                            count = int(point.count)
                            total = _optional_scalar(point, "sum")
                            minimum = _optional_scalar(point, "min")
                            maximum = _optional_scalar(point, "max")
                        elif metric_type == "summary":
                            count = int(point.count)
                            total = float(point.sum)

                        exemplars = [
                            _message_to_dict(exemplar)
                            for exemplar in getattr(point, "exemplars", [])
                        ]
                        raw = {
                            "metric": {
                                "name": metric.name,
                                "description": metric.description,
                                "unit": metric.unit,
                                "type": metric_type,
                            },
                            "data_point": _message_to_dict(point),
                        }
                        record = {
                            "service_name": service_name,
                            "metric_name": metric.name or "unnamed_metric",
                            "metric_type": metric_type,
                            "time_unix": point_time,
                            "start_time": start_time,
                            "value": value,
                            "count": count,
                            "sum": total,
                            "min": minimum,
                            "max": maximum,
                            "attributes": attributes,
                            "resource_attributes": resource_attributes,
                            "exemplars": exemplars,
                            "run_id": _extract_run_id(attributes, resource_attributes),
                            "observed_at": observed_at,
                            "raw": raw,
                        }
                        record["idempotency_key"] = idempotency_key(record)
                        records.append(record)
                    except Exception as exc:
                        rejected.append(
                            _rejection(
                                "metric",
                                f"normalization_error: {exc}",
                                {
                                    "metric_name": metric.name,
                                    "data_point": _message_to_dict(point),
                                    "service_name": service_name,
                                },
                            )
                        )
    return records, rejected


def _body_to_text(body: Any) -> str:
    value = any_value_to_python(body)
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def normalize_log_request(request: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    received_at = utc_now()

    for resource_logs in request.resource_logs:
        resource_attributes = key_values_to_dict(resource_logs.resource.attributes)
        service_name, _, _ = _resource_identity(resource_attributes)
        for scope_logs in resource_logs.scope_logs:
            for log_record in scope_logs.log_records:
                try:
                    attributes = key_values_to_dict(log_record.attributes)
                    observed_time = nanos_to_datetime(
                        int(log_record.observed_time_unix_nano), fallback=received_at
                    )
                    time_unix = nanos_to_datetime(
                        int(log_record.time_unix_nano), fallback=observed_time
                    )
                    record = {
                        "service_name": service_name,
                        "time_unix": time_unix,
                        "observed_time": observed_time,
                        "severity_number": int(log_record.severity_number),
                        "severity_text": log_record.severity_text or None,
                        "body": _body_to_text(log_record.body),
                        "trace_id": bytes_to_hex(log_record.trace_id),
                        "span_id": bytes_to_hex(log_record.span_id),
                        "attributes": attributes,
                        "resource_attributes": resource_attributes,
                        "run_id": _extract_run_id(attributes, resource_attributes),
                        "raw": _message_to_dict(log_record),
                    }
                    record["idempotency_key"] = idempotency_key(record)
                    records.append(record)
                except Exception as exc:
                    rejected.append(
                        _rejection(
                            "log",
                            f"normalization_error: {exc}",
                            {"record": _message_to_dict(log_record), "service_name": service_name},
                        )
                    )
    return records, rejected


def normalized_batch_to_records(
    batch: NormalizedBatch,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Convert the synthetic runner's validated JSON batch into database rows."""

    rows: dict[str, list[dict[str, Any]]] = {"span": [], "metric": [], "log": []}
    observed_at = utc_now()
    for event in batch.events:
        run_id = event.run_id or batch.run_id
        if isinstance(event, NormalizedSpan):
            raw = event.model_dump(mode="python", exclude={"signal"})
            row = {
                **raw,
                "run_id": run_id,
                "duration_ms": (event.end_time - event.start_time).total_seconds() * 1000.0,
                "observed_at": observed_at,
            }
            rows["span"].append(row)
        elif isinstance(event, NormalizedMetric):
            raw = event.model_dump(mode="python", exclude={"signal"})
            row = {**raw, "run_id": run_id, "observed_at": observed_at}
            row["idempotency_key"] = idempotency_key(row)
            rows["metric"].append(row)
        elif isinstance(event, NormalizedLog):
            raw = event.model_dump(mode="python", exclude={"signal"})
            row = {
                **raw,
                "run_id": run_id,
                "observed_time": event.observed_time or event.time_unix,
            }
            row["idempotency_key"] = idempotency_key(row)
            rows["log"].append(row)
        else:  # pragma: no cover - discriminated union prevents this.
            raise TypeError(f"unsupported normalized event: {type(event)!r}")
    return rows, []
