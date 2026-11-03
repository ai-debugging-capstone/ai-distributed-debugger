"""FastAPI application for live OTLP ingestion and incident exploration."""

from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import io
import logging
import logging.config
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from google.protobuf.message import DecodeError, Message
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

from . import __version__
from .config import Settings, get_settings
from .models import (
    RESERVED_EXPERIMENT_RUN_ID,
    ExperimentRunRequest,
    ExperimentRunUpdate,
    FaultInjectionRequest,
    FeedbackRequest,
    GroundTruthLabelRequest,
    IngestSummary,
    NormalizedBatch,
)
from .normalization import (
    normalize_log_request,
    normalize_metric_request,
    normalize_trace_request,
    normalized_batch_to_records,
    utc_now,
)
from .storage import PsycopgStorage, Storage, StorageError, StorageValidationError

LOGGER = logging.getLogger(__name__)

PROTOBUF_MEDIA_TYPES = {
    "application/x-protobuf",
    "application/protobuf",
    "application/octet-stream",
}


def _reject_reserved_experiment(run_id: UUID) -> None:
    """Keep the all-zero UUID as an internal live-telemetry namespace."""

    if run_id == RESERVED_EXPERIMENT_RUN_ID:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="the reserved live-telemetry experiment cannot be mutated",
        )

OTLP_CONFIG: dict[str, dict[str, Any]] = {
    "traces": {
        "request": ExportTraceServiceRequest,
        "response": ExportTraceServiceResponse,
        "normalize": normalize_trace_request,
        "record_key": "span",
        "partial_field": "rejected_spans",
    },
    "metrics": {
        "request": ExportMetricsServiceRequest,
        "response": ExportMetricsServiceResponse,
        "normalize": normalize_metric_request,
        "record_key": "metric",
        "partial_field": "rejected_data_points",
    },
    "logs": {
        "request": ExportLogsServiceRequest,
        "response": ExportLogsServiceResponse,
        "normalize": normalize_log_request,
        "record_key": "log",
        "partial_field": "rejected_log_records",
    },
}


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _gzip_decompress_limited(payload: bytes, limit: int) -> bytes:
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as stream:
            decoded = stream.read(limit + 1)
    except (OSError, EOFError) as exc:
        raise HTTPException(status_code=400, detail="invalid gzip request body") from exc
    if len(decoded) > limit:
        raise HTTPException(status_code=413, detail="decompressed request body is too large")
    return decoded


async def _otlp_body(request: Request, settings: Settings) -> bytes:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type and content_type not in PROTOBUF_MEDIA_TYPES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="OTLP/HTTP endpoints require protobuf content",
        )
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > settings.max_request_body_bytes:
                raise HTTPException(status_code=413, detail="request body is too large")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid Content-Length header") from exc

    payload = await request.body()
    if len(payload) > settings.max_request_body_bytes:
        raise HTTPException(status_code=413, detail="request body is too large")
    encoding = request.headers.get("content-encoding", "identity").lower().strip()
    if encoding == "gzip":
        return _gzip_decompress_limited(payload, settings.max_request_body_bytes)
    if encoding not in {"", "identity"}:
        raise HTTPException(status_code=415, detail=f"unsupported content encoding: {encoding}")
    return payload


def _decode_message(message_type: type[Message], payload: bytes) -> Message:
    message = message_type()
    try:
        message.ParseFromString(payload)
    except DecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid OTLP protobuf payload") from exc
    return message


def _decode_rejection(signal: str, payload: bytes, request: Request) -> dict[str, Any]:
    return {
        "signal_type": signal,
        "reason": "protobuf_decode_error",
        "payload": {
            "content_type": request.headers.get("content-type"),
            "content_encoding": request.headers.get("content-encoding"),
            "size_bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "prefix_base64": base64.b64encode(payload[:1024]).decode("ascii"),
        },
        "received_at": utc_now(),
    }


def create_app(
    *, settings: Settings | None = None, storage: Storage | None = None
) -> FastAPI:
    resolved_settings = settings or get_settings()
    resolved_storage: Storage = storage or PsycopgStorage(resolved_settings)
    _configure_logging(resolved_settings.log_level)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        application.state.storage = resolved_storage
        application.state.settings = resolved_settings
        try:
            await resolved_storage.open()
        except StorageError:
            # Keep liveness available while the pool retries; readiness and all
            # database-backed endpoints still correctly report unavailable.
            LOGGER.exception("Initial PostgreSQL connection failed; service is not ready")
        yield
        await resolved_storage.close()

    application = FastAPI(
        title="Dependency-Aware Debugging API",
        summary="OTLP ingestion and evidence-backed incident exploration",
        description=(
            "Receives OpenTelemetry protobuf signals, persists normalized records "
            "to PostgreSQL, and exposes incidents, root-cause candidates, evidence, "
            "dependency graphs, and developer feedback."
        ),
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    if resolved_settings.cors_origins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=resolved_settings.cors_origins,
            allow_credentials=False,
            allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
            allow_headers=["*"],
        )

    @application.middleware("http")
    async def request_context(request: Request, call_next: Callable[..., Any]) -> Response:
        supplied_request_id = request.headers.get("x-request-id", "")
        request_id = (
            supplied_request_id
            if supplied_request_id and len(supplied_request_id) <= 128
            else str(uuid4())
        )
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @application.exception_handler(StorageValidationError)
    async def storage_validation_error_handler(
        request: Request, exc: StorageValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": str(exc),
                "request_id": getattr(request.state, "request_id", None),
            },
        )

    @application.exception_handler(StorageError)
    async def storage_error_handler(request: Request, exc: StorageError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "detail": str(exc),
                "request_id": getattr(request.state, "request_id", None),
            },
            headers={"Retry-After": "5"},
        )

    @application.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "detail": jsonable_encoder(exc.errors()),
                "request_id": getattr(request.state, "request_id", None),
            },
        )

    def database(request: Request) -> Storage:
        return request.app.state.storage

    @application.get("/", tags=["service"])
    async def root() -> dict[str, str]:
        return {
            "service": "dependency-aware-debugging-api",
            "version": __version__,
            "documentation": "/docs",
        }

    @application.get("/health/live", tags=["health"])
    async def liveness() -> dict[str, str]:
        return {"status": "alive", "version": __version__}

    @application.get("/health/ready", tags=["health"])
    async def readiness(db: Storage = Depends(database)) -> JSONResponse:
        try:
            ready = await asyncio.wait_for(
                db.ping(), timeout=resolved_settings.database_connect_timeout_seconds + 1
            )
        except (asyncio.TimeoutError, StorageError):
            ready = False
        return JSONResponse(
            status_code=200 if ready else 503,
            content={"status": "ready" if ready else "not_ready", "database": ready},
        )

    async def receive_otlp(signal: str, request: Request, db: Storage) -> Response:
        config = OTLP_CONFIG[signal]
        payload = await _otlp_body(request, resolved_settings)
        try:
            message = _decode_message(config["request"], payload)
        except HTTPException:
            try:
                await db.write_rejections([_decode_rejection(signal, payload, request)])
            except StorageError:
                LOGGER.exception("Could not persist malformed OTLP request")
            raise

        normalized, rejected = config["normalize"](message)
        record_key = config["record_key"]
        records = {"span": [], "metric": [], "log": [], record_key: normalized}
        result = await db.ingest(records, rejected)

        response_message = config["response"]()
        if rejected:
            partial_success = response_message.partial_success
            setattr(partial_success, config["partial_field"], len(rejected))
            partial_success.error_message = (
                f"{len(rejected)} record(s) were rejected during normalization"
            )
        return Response(
            content=response_message.SerializeToString(),
            media_type="application/x-protobuf",
            headers={
                "X-OTLP-Accepted": str(result.accepted.get(record_key, 0)),
                "X-OTLP-Inserted": str(result.inserted.get(record_key, 0)),
                "X-OTLP-Duplicates": str(result.duplicates.get(record_key, 0)),
                "X-OTLP-Rejected": str(result.rejected),
            },
        )

    @application.post("/otlp/v1/traces", tags=["OTLP"], response_class=Response)
    @application.post(
        "/v1/traces", tags=["OTLP"], response_class=Response, include_in_schema=False
    )
    async def ingest_traces(request: Request, db: Storage = Depends(database)) -> Response:
        return await receive_otlp("traces", request, db)

    @application.post("/otlp/v1/metrics", tags=["OTLP"], response_class=Response)
    @application.post(
        "/v1/metrics", tags=["OTLP"], response_class=Response, include_in_schema=False
    )
    async def ingest_metrics(request: Request, db: Storage = Depends(database)) -> Response:
        return await receive_otlp("metrics", request, db)

    @application.post("/otlp/v1/logs", tags=["OTLP"], response_class=Response)
    @application.post(
        "/v1/logs", tags=["OTLP"], response_class=Response, include_in_schema=False
    )
    async def ingest_logs(request: Request, db: Storage = Depends(database)) -> Response:
        return await receive_otlp("logs", request, db)

    @application.post(
        "/api/v1/events/batch",
        response_model=IngestSummary,
        tags=["normalized telemetry"],
    )
    async def ingest_normalized_batch(
        batch: NormalizedBatch, db: Storage = Depends(database)
    ) -> IngestSummary:
        if len(batch.events) > resolved_settings.max_batch_events:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"batch contains {len(batch.events)} events; maximum is "
                    f"{resolved_settings.max_batch_events}"
                ),
            )
        records, rejected = normalized_batch_to_records(batch)
        result = await db.ingest(records, rejected)
        return IngestSummary(
            accepted=result.accepted,
            inserted=result.inserted,
            duplicates=result.duplicates,
            rejected=result.rejected,
        )

    @application.post(
        "/api/v1/experiments",
        status_code=status.HTTP_201_CREATED,
        tags=["experiments"],
    )
    async def upsert_experiment(
        run: ExperimentRunRequest, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        """Register a reproducible run before its stamped telemetry arrives."""

        _reject_reserved_experiment(run.id)
        return await db.upsert_experiment(run)

    @application.get("/api/v1/experiments/{run_id}", tags=["experiments"])
    async def get_experiment(
        run_id: UUID, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        run = await db.get_experiment(str(run_id))
        if run is None:
            raise HTTPException(status_code=404, detail="experiment not found")
        return run

    @application.get(
        "/api/v1/experiments/{run_id}/capture-summary", tags=["experiments"]
    )
    async def get_experiment_capture_summary(
        run_id: UUID, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        """Expose persisted, run-scoped evidence for the capture acceptance gate."""

        run_key = str(run_id)
        if await db.get_experiment(run_key) is None:
            raise HTTPException(status_code=404, detail="experiment not found")
        return await db.get_experiment_capture_summary(run_key)

    @application.patch("/api/v1/experiments/{run_id}", tags=["experiments"])
    async def update_experiment(
        run_id: UUID,
        update: ExperimentRunUpdate,
        db: Storage = Depends(database),
    ) -> dict[str, Any]:
        _reject_reserved_experiment(run_id)
        run = await db.update_experiment(str(run_id), update)
        if run is None:
            raise HTTPException(status_code=404, detail="experiment not found")
        return run

    @application.post(
        "/api/v1/experiments/{run_id}/faults",
        status_code=status.HTTP_201_CREATED,
        tags=["experiments"],
    )
    async def create_fault_injection(
        run_id: UUID,
        fault: FaultInjectionRequest,
        db: Storage = Depends(database),
    ) -> dict[str, Any]:
        _reject_reserved_experiment(run_id)
        run_key = str(run_id)
        if await db.get_experiment(run_key) is None:
            raise HTTPException(status_code=404, detail="experiment not found")
        return await db.create_fault_injection(run_key, fault)

    @application.post(
        "/api/v1/experiments/{run_id}/labels",
        status_code=status.HTTP_201_CREATED,
        tags=["experiments"],
    )
    async def upsert_ground_truth_label(
        run_id: UUID,
        label: GroundTruthLabelRequest,
        db: Storage = Depends(database),
    ) -> dict[str, Any]:
        _reject_reserved_experiment(run_id)
        run_key = str(run_id)
        if await db.get_experiment(run_key) is None:
            raise HTTPException(status_code=404, detail="experiment not found")
        return await db.upsert_ground_truth_label(run_key, label)

    @application.get("/api/v1/incidents", tags=["incidents"])
    async def list_incidents(
        incident_status: str | None = Query(default=None, alias="status", max_length=64),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        db: Storage = Depends(database),
    ) -> dict[str, Any]:
        items = await db.list_incidents(
            status=incident_status, limit=limit, offset=offset
        )
        return {"items": items, "count": len(items), "limit": limit, "offset": offset}

    @application.get("/api/v1/incidents/{incident_id}", tags=["incidents"])
    async def get_incident(
        incident_id: UUID, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        incident = await db.get_incident(str(incident_id))
        if incident is None:
            raise HTTPException(status_code=404, detail="incident not found")
        return incident

    @application.get("/api/v1/incidents/{incident_id}/candidates", tags=["analysis"])
    async def get_candidates(
        incident_id: UUID, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        incident_key = str(incident_id)
        items = await db.get_candidates(incident_key)
        return {"incident_id": incident_key, "items": items, "count": len(items)}

    @application.get("/api/v1/candidates", tags=["analysis"], include_in_schema=False)
    async def get_candidates_alias(
        incident_id: UUID, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        return await get_candidates(incident_id, db)

    @application.get("/api/v1/incidents/{incident_id}/evidence", tags=["analysis"])
    async def get_evidence(
        incident_id: UUID, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        incident_key = str(incident_id)
        items = await db.get_evidence(incident_key)
        return {"incident_id": incident_key, "items": items, "count": len(items)}

    @application.get("/api/v1/evidence", tags=["analysis"], include_in_schema=False)
    async def get_evidence_alias(
        incident_id: UUID, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        return await get_evidence(incident_id, db)

    @application.get(
        "/api/v1/incidents/{incident_id}/recommendations", tags=["analysis"]
    )
    async def get_recommendations(
        incident_id: UUID, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        incident_key = str(incident_id)
        items = await db.get_recommendations(incident_key)
        return {"incident_id": incident_key, "items": items, "count": len(items)}

    @application.get("/api/v1/incidents/{incident_id}/graph", tags=["analysis"])
    async def get_graph(
        incident_id: UUID, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        return await db.get_graph(str(incident_id))

    @application.get("/api/v1/graph", tags=["analysis"], include_in_schema=False)
    async def get_graph_alias(
        incident_id: UUID, db: Storage = Depends(database)
    ) -> dict[str, Any]:
        return await get_graph(incident_id, db)

    @application.post(
        "/api/v1/incidents/{incident_id}/feedback",
        status_code=status.HTTP_201_CREATED,
        tags=["feedback"],
    )
    async def create_feedback(
        incident_id: UUID,
        feedback: FeedbackRequest,
        db: Storage = Depends(database),
    ) -> dict[str, Any]:
        return await db.create_feedback(str(incident_id), feedback)

    @application.post(
        "/api/v1/feedback",
        status_code=status.HTTP_201_CREATED,
        tags=["feedback"],
        include_in_schema=False,
    )
    async def create_feedback_alias(
        incident_id: UUID,
        feedback: FeedbackRequest,
        db: Storage = Depends(database),
    ) -> dict[str, Any]:
        return await create_feedback(incident_id, feedback, db)

    return application


app = create_app()
