from __future__ import annotations


RUN_ID = "d507d090-d8cc-4a66-a483-2d677b77643f"
FAULT_ID = "c8e83639-53e1-4ca6-b590-9fb04e145cec"


def test_experiment_fault_and_label_lifecycle(client, fake_storage):
    created = client.post(
        "/api/v1/experiments",
        json={
            "id": RUN_ID,
            "name": "otel-demo-payment-failure",
            "source_repository": "https://github.com/open-telemetry/opentelemetry-demo.git",
            "source_version": "3.1.0",
            "source_sha": "dedc0178918e260823323b8d95005a8cb924b007",
            "seed": 42,
            "workload": "repeated-checkout",
            "status": "warming",
            "started_at": "2026-09-30T16:00:00Z",
            "configuration": {"profile": "full"},
        },
    )
    assert created.status_code == 201
    assert created.json()["id"] == RUN_ID
    assert client.get(f"/api/v1/experiments/{RUN_ID}").json()["status"] == "warming"

    fault = client.post(
        f"/api/v1/experiments/{RUN_ID}/faults",
        json={
            "id": FAULT_ID,
            "flag_name": "paymentFailure",
            "variant": "50%",
            "injector": "flagd",
            "causal_entity_type": "service",
            "causal_entity_id": "payment",
            "mechanism": "service_error",
            "impacted_sli": "checkout_success",
            "injected_at": "2026-09-30T16:01:00Z",
            "cleared_at": "2026-09-30T16:03:00Z",
            "expected_runbook_key": "service-error-v1",
        },
    )
    assert fault.status_code == 201
    assert fault.json()["run_id"] == RUN_ID
    assert fault.json()["flag_name"] == "paymentFailure"

    label = client.post(
        f"/api/v1/experiments/{RUN_ID}/labels",
        json={
            "entity_type": "service",
            "entity_id": "payment",
            "fault_class": "service_error",
            "relevance": 3,
            "is_primary": True,
        },
    )
    assert label.status_code == 201
    assert label.json()["entity_id"] == "payment"

    completed = client.patch(
        f"/api/v1/experiments/{RUN_ID}",
        json={
            "status": "completed",
            "ended_at": "2026-09-30T16:04:00Z",
            "environment": {"collector": "rca-otel-gateway"},
        },
    )
    assert completed.status_code == 200
    assert completed.json()["status"] == "completed"
    assert completed.json()["environment"]["collector"] == "rca-otel-gateway"
    assert len(fake_storage.faults) == 1
    assert len(fake_storage.labels) == 1


def test_experiment_children_require_a_registered_run(client):
    missing = "00000000-0000-0000-0000-000000000001"
    response = client.post(
        f"/api/v1/experiments/{missing}/labels",
        json={
            "entity_type": "service",
            "entity_id": "payment",
            "fault_class": "service_error",
        },
    )
    assert response.status_code == 404
    assert client.patch(f"/api/v1/experiments/{missing}", json={"status": "failed"}).status_code == 404


def test_capture_summary_is_scoped_to_registered_experiment(client, fake_storage):
    client.post("/api/v1/experiments", json={"id": RUN_ID, "name": "capture"})
    fake_storage.incidents.append(
        {
            "id": "8e94de49-4a63-47c4-abbb-92235e5a93d2",
            "run_id": RUN_ID,
            "status": "resolved",
        }
    )

    response = client.get(f"/api/v1/experiments/{RUN_ID}/capture-summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == RUN_ID
    assert payload["telemetry"] == {
        "spans": 3,
        "metric_points": 2,
        "log_records": 1,
    }
    assert payload["incidents"][0]["run_id"] == RUN_ID
    assert payload["candidates"][0]["entity_id"] == "payment"

    missing = "56b642de-ae9d-4a47-bbca-199a1bd79863"
    assert client.get(f"/api/v1/experiments/{missing}/capture-summary").status_code == 404


def test_experiment_validation_rejects_inconsistent_intervals_and_edges(client):
    invalid_run = client.post(
        "/api/v1/experiments",
        json={
            "id": RUN_ID,
            "name": "invalid",
            "started_at": "2026-09-30T16:05:00Z",
            "ended_at": "2026-09-30T16:00:00Z",
        },
    )
    assert invalid_run.status_code == 422

    client.post("/api/v1/experiments", json={"id": RUN_ID, "name": "valid"})
    invalid_fault = client.post(
        f"/api/v1/experiments/{RUN_ID}/faults",
        json={
            "flag_name": "paymentUnreachable",
            "variant": "on",
            "injector": "flagd",
            "causal_entity_type": "edge",
            "causal_entity_id": "checkout->payment",
            "causal_edge_source": "checkout",
            "mechanism": "dependency_unreachable",
            "impacted_sli": "checkout_success",
            "injected_at": "2026-09-30T16:01:00Z",
        },
    )
    assert invalid_fault.status_code == 422


def test_idempotent_registration_does_not_regress_status(client):
    created = client.post(
        "/api/v1/experiments",
        json={
            "id": RUN_ID,
            "name": "first",
            "status": "completed",
            "source_version": "3.1.0",
        },
    )
    assert created.status_code == 201

    retried = client.post(
        "/api/v1/experiments",
        json={"id": RUN_ID, "name": "first", "source_version": "3.1.0"},
    )
    assert retried.status_code == 201
    assert retried.json()["status"] == "completed"
    assert retried.json()["name"] == "first"

    explicit = client.post(
        "/api/v1/experiments",
        json={"id": RUN_ID, "name": "first", "status": "running"},
    )
    assert explicit.status_code == 201
    assert explicit.json()["status"] == "completed"

    conflicting = client.post(
        "/api/v1/experiments",
        json={"id": RUN_ID, "name": "reused-id", "source_version": "4.0.0"},
    )
    assert conflicting.status_code == 409


def test_terminal_experiment_metadata_is_frozen(client):
    assert client.post(
        "/api/v1/experiments",
        json={
            "id": RUN_ID,
            "name": "frozen",
            "status": "completed",
            "split": "train",
            "configuration": {"profile": "full"},
        },
    ).status_code == 201
    assert client.patch(
        f"/api/v1/experiments/{RUN_ID}",
        json={"split": "test"},
    ).status_code == 409
    assert client.patch(
        f"/api/v1/experiments/{RUN_ID}",
        json={"configuration": {"late": True}},
    ).status_code == 409


def test_cors_preflight_allows_experiment_patch(fake_storage):
    from fastapi.testclient import TestClient

    from telemetry_api.config import Settings
    from telemetry_api.main import create_app

    application = create_app(
        settings=Settings(
            database_url="postgresql://test:test@localhost:5432/test",
            cors_origins=["http://debugger.local"],
        ),
        storage=fake_storage,
    )
    with TestClient(application) as cors_client:
        response = cors_client.options(
            f"/api/v1/experiments/{RUN_ID}",
            headers={
                "Origin": "http://debugger.local",
                "Access-Control-Request-Method": "PATCH",
            },
        )
    assert response.status_code == 200
    assert "PATCH" in response.headers["access-control-allow-methods"]


def test_patch_validates_merged_interval_and_status_is_monotonic(client):
    client.post(
        "/api/v1/experiments",
        json={
            "id": RUN_ID,
            "name": "interval",
            "status": "running",
            "started_at": "2026-09-30T16:05:00Z",
        },
    )
    invalid = client.patch(
        f"/api/v1/experiments/{RUN_ID}",
        json={"ended_at": "2026-09-30T16:00:00Z"},
    )
    assert invalid.status_code == 409

    regressed = client.patch(
        f"/api/v1/experiments/{RUN_ID}", json={"status": "warming"}
    )
    assert regressed.status_code == 200
    assert regressed.json()["status"] == "running"


def test_labels_are_scoped_to_incident_and_run(client, fake_storage):
    other_run = "5f1cd498-2599-4f62-955f-667dc8eae491"
    incident_one = "f5a0bf6a-1a8c-4eb8-8c0d-250df63376ad"
    incident_two = "2b8fed75-06a4-49d0-8ddb-849ac6537390"
    for run_id in (RUN_ID, other_run):
        client.post("/api/v1/experiments", json={"id": run_id, "name": run_id})
    fake_storage.incidents.extend(
        [
            {"id": incident_one, "run_id": RUN_ID, "status": "open"},
            {"id": incident_two, "run_id": RUN_ID, "status": "open"},
        ]
    )
    payload = {
        "entity_type": "service",
        "entity_id": "payment",
        "fault_class": "service_error",
        "relevance": 3,
    }
    first = client.post(
        f"/api/v1/experiments/{RUN_ID}/labels",
        json={**payload, "incident_id": incident_one},
    )
    second = client.post(
        f"/api/v1/experiments/{RUN_ID}/labels",
        json={**payload, "incident_id": incident_two},
    )
    assert first.status_code == second.status_code == 201
    assert len(fake_storage.labels) == 2

    cross_run = client.post(
        f"/api/v1/experiments/{other_run}/labels",
        json={**payload, "incident_id": incident_one},
    )
    assert cross_run.status_code == 409


def test_fault_id_cannot_move_between_runs(client):
    other_run = "5f1cd498-2599-4f62-955f-667dc8eae491"
    for run_id in (RUN_ID, other_run):
        client.post("/api/v1/experiments", json={"id": run_id, "name": run_id})
    payload = {
        "id": FAULT_ID,
        "flag_name": "paymentFailure",
        "injector": "flagd",
        "causal_entity_type": "service",
        "causal_entity_id": "payment",
        "mechanism": "service_error",
        "impacted_sli": "checkout_success",
        "injected_at": "2026-09-30T16:01:00Z",
    }
    assert client.post(
        f"/api/v1/experiments/{RUN_ID}/faults", json=payload
    ).status_code == 201
    assert client.post(
        f"/api/v1/experiments/{other_run}/faults", json=payload
    ).status_code == 409
    assert client.post(
        f"/api/v1/experiments/{RUN_ID}/faults",
        json={**payload, "causal_entity_id": "checkout"},
    ).status_code == 409


def test_fault_retry_preserves_monotonic_event_times(client):
    client.post("/api/v1/experiments", json={"id": RUN_ID, "name": "fault-times"})
    payload = {
        "id": FAULT_ID,
        "flag_name": "paymentFailure",
        "injector": "flagd",
        "causal_entity_type": "service",
        "causal_entity_id": "payment",
        "mechanism": "service_error",
        "impacted_sli": "checkout_success",
        "injected_at": "2026-09-30T16:01:00Z",
    }
    cleared = client.post(
        f"/api/v1/experiments/{RUN_ID}/faults",
        json={**payload, "cleared_at": "2026-09-30T16:03:00Z"},
    )
    assert cleared.status_code == 201
    replay = client.post(f"/api/v1/experiments/{RUN_ID}/faults", json=payload)
    assert replay.status_code == 201
    assert replay.json()["cleared_at"] == "2026-09-30T16:03:00Z"
    assert client.post(
        f"/api/v1/experiments/{RUN_ID}/faults",
        json={**payload, "injected_at": "2026-09-30T16:01:01Z"},
    ).status_code == 409
    assert client.post(
        f"/api/v1/experiments/{RUN_ID}/faults",
        json={**payload, "cleared_at": "2026-09-30T16:02:00Z"},
    ).status_code == 409


def test_reserved_live_telemetry_run_cannot_be_mutated(client):
    reserved = "00000000-0000-0000-0000-000000000000"
    assert client.post(
        "/api/v1/experiments", json={"id": reserved, "name": "not-allowed"}
    ).status_code == 409
    assert client.patch(
        f"/api/v1/experiments/{reserved}", json={"status": "completed"}
    ).status_code == 409
    assert client.post(
        f"/api/v1/experiments/{reserved}/faults",
        json={
            "flag_name": "x",
            "injector": "test",
            "causal_entity_type": "service",
            "causal_entity_id": "x",
            "mechanism": "error",
            "impacted_sli": "availability",
            "injected_at": "2026-09-30T16:01:00Z",
        },
    ).status_code == 409
    assert client.post(
        f"/api/v1/experiments/{reserved}/labels",
        json={
            "entity_type": "service",
            "entity_id": "x",
            "fault_class": "error",
        },
    ).status_code == 409
