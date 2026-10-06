from __future__ import annotations

import gzip

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)


def _trace_export() -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    resource_spans = request.resource_spans.add()
    service = resource_spans.resource.attributes.add()
    service.key = "service.name"
    service.value.string_value = "checkoutservice"
    span = resource_spans.scope_spans.add().spans.add()
    span.trace_id = bytes.fromhex("00112233445566778899aabbccddeeff")
    span.span_id = bytes.fromhex("0011223344556677")
    span.name = "POST /checkout"
    span.start_time_unix_nano = 1_700_000_000_000_000_000
    span.end_time_unix_nano = 1_700_000_000_100_000_000
    return request


def test_health_endpoints(client, fake_storage):
    assert client.get("/health/live").json()["status"] == "alive"
    assert client.get("/health/ready").status_code == 200
    fake_storage.ready = False
    assert client.get("/health/ready").status_code == 503


def test_otlp_trace_ingestion_returns_protobuf_response(client, fake_storage):
    payload = _trace_export().SerializeToString()

    response = client.post(
        "/otlp/v1/traces",
        content=payload,
        headers={"Content-Type": "application/x-protobuf"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-protobuf")
    assert response.headers["x-otlp-inserted"] == "1"
    parsed = ExportTraceServiceResponse()
    parsed.ParseFromString(response.content)
    assert parsed.partial_success.rejected_spans == 0
    row = fake_storage.batches[0]["span"][0]
    assert row["service_name"] == "checkoutservice"
    assert row["operation_name"] == "POST /checkout"


def test_canonical_otlp_alias_accepts_gzip(client, fake_storage):
    payload = gzip.compress(_trace_export().SerializeToString())

    response = client.post(
        "/v1/traces",
        content=payload,
        headers={
            "Content-Type": "application/x-protobuf",
            "Content-Encoding": "gzip",
        },
    )

    assert response.status_code == 200
    assert len(fake_storage.batches[0]["span"]) == 1


def test_malformed_protobuf_is_rejected_and_quarantined(client, fake_storage):
    response = client.post(
        "/otlp/v1/traces",
        content=b"\xff\xff\xff",
        headers={"Content-Type": "application/x-protobuf"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "invalid OTLP protobuf payload"
    assert fake_storage.rejections[0]["reason"] == "protobuf_decode_error"


def test_normalized_batch_and_query_workflow(client, fake_storage):
    incident_id = "970c8464-87bf-42d2-a25f-cff3b01f1002"
    response = client.post(
        "/api/v1/events/batch",
        json={
            "run_id": "970c8464-87bf-42d2-a25f-cff3b01f1001",
            "events": [
                {
                    "signal": "metric",
                    "service_name": "paymentservice",
                    "metric_name": "rpc.server.duration",
                    "metric_type": "gauge",
                    "time_unix": "2026-09-30T16:00:00Z",
                    "value": 1.25,
                }
            ],
        },
    )
    assert response.status_code == 200
    assert response.json()["inserted"]["metric"] == 1
    assert fake_storage.batches[0]["metric"][0]["service_name"] == "paymentservice"

    assert client.get("/api/v1/incidents").json()["count"] == 1
    assert client.get(f"/api/v1/incidents/{incident_id}").status_code == 200
    assert client.get("/api/v1/incidents/00000000-0000-0000-0000-000000000000").status_code == 404
    assert client.get(f"/api/v1/incidents/{incident_id}/candidates").json()["count"] == 1
    assert client.get(f"/api/v1/incidents/{incident_id}/evidence").json()["count"] == 1
    assert client.get(f"/api/v1/incidents/{incident_id}/recommendations").json()["count"] == 1
    assert len(client.get(f"/api/v1/incidents/{incident_id}/graph").json()["edges"]) == 1

    feedback = client.post(
        f"/api/v1/incidents/{incident_id}/feedback",
        json={
            "verdict": "confirmed",
            "candidate_id": 1,
            "comment": "Confirmed from payment logs",
        },
    )
    assert feedback.status_code == 201
    assert feedback.json()["verdict"] == "correct"


def test_batch_validation_rejects_bad_trace_identifier(client):
    response = client.post(
        "/api/v1/events/batch",
        json={
            "events": [
                {
                    "signal": "span",
                    "service_name": "frontend",
                    "trace_id": "not-a-trace-id",
                    "span_id": "0011223344556677",
                    "operation_name": "GET /",
                    "start_time": "2026-09-30T16:00:00Z",
                    "end_time": "2026-09-30T16:00:01Z"
                }
            ]
        },
    )
    assert response.status_code == 422
    assert response.json()["detail"]
