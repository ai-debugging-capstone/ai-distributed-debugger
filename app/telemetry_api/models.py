"""External request and response models for normalized telemetry ingestion."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator


HexTraceId = Annotated[str, StringConstraints(pattern=r"^[0-9a-fA-F]{32}$")]
HexSpanId = Annotated[str, StringConstraints(pattern=r"^[0-9a-fA-F]{16}$")]


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class SignalType(str, Enum):
    SPAN = "span"
    METRIC = "metric"
    LOG = "log"


class _EventBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: UUID | None = None
    service_name: str = Field(min_length=1, max_length=255)
    resource_attributes: dict[str, Any] = Field(default_factory=dict)
    attributes: dict[str, Any] = Field(default_factory=dict)


class NormalizedSpan(_EventBase):
    signal: Literal["span"]
    trace_id: HexTraceId
    span_id: HexSpanId
    parent_span_id: HexSpanId | None = None
    service_namespace: str | None = Field(default=None, max_length=255)
    service_instance_id: str | None = Field(default=None, max_length=512)
    operation_name: str = Field(min_length=1, max_length=1024)
    span_kind: str = Field(default="INTERNAL", max_length=64)
    start_time: datetime
    end_time: datetime
    status_code: str = Field(default="UNSET", max_length=64)
    status_message: str | None = Field(default=None, max_length=4096)
    scope_name: str | None = Field(default=None, max_length=512)
    scope_version: str | None = Field(default=None, max_length=128)
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("start_time", "end_time")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _ensure_aware(value)

    @field_validator("trace_id", "span_id", "parent_span_id")
    @classmethod
    def normalize_hex(cls, value: str | None) -> str | None:
        return value.lower() if value else value

    @model_validator(mode="after")
    def valid_time_range(self) -> "NormalizedSpan":
        if self.end_time < self.start_time:
            raise ValueError("end_time must be greater than or equal to start_time")
        return self


class NormalizedMetric(_EventBase):
    signal: Literal["metric"]
    metric_name: str = Field(min_length=1, max_length=512)
    metric_type: str = Field(default="gauge", max_length=64)
    time_unix: datetime
    start_time: datetime | None = None
    value: float | None = None
    count: int | None = Field(default=None, ge=0)
    sum: float | None = None
    min: float | None = None
    max: float | None = None
    exemplars: list[dict[str, Any]] = Field(default_factory=list)
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("time_unix", "start_time")
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        return _ensure_aware(value) if value is not None else None

    @model_validator(mode="after")
    def contains_a_value(self) -> "NormalizedMetric":
        if all(item is None for item in (self.value, self.count, self.sum, self.min, self.max)):
            raise ValueError("a metric event must contain value, count, sum, min, or max")
        return self


class NormalizedLog(_EventBase):
    signal: Literal["log"]
    time_unix: datetime
    observed_time: datetime | None = None
    severity_number: int = Field(default=0, ge=0)
    severity_text: str | None = Field(default=None, max_length=128)
    body: str = Field(default="", max_length=1_000_000)
    trace_id: HexTraceId | None = None
    span_id: HexSpanId | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("time_unix", "observed_time")
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        return _ensure_aware(value) if value is not None else None

    @field_validator("trace_id", "span_id")
    @classmethod
    def normalize_hex(cls, value: str | None) -> str | None:
        return value.lower() if value else value


NormalizedEvent = Annotated[
    NormalizedSpan | NormalizedMetric | NormalizedLog,
    Field(discriminator="signal"),
]


class NormalizedBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: UUID | None = None
    events: list[NormalizedEvent] = Field(min_length=1)


class FeedbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_id: int | None = Field(default=None, gt=0)
    verdict: Literal["correct", "partially_correct", "incorrect", "unknown"]
    comment: str | None = Field(default=None, max_length=10_000)
    user_name: str | None = Field(default=None, max_length=255)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("verdict", mode="before")
    @classmethod
    def normalize_verdict(cls, value: object) -> object:
        # Friendly aliases remain accepted while the stored values match the
        # database constraint and evaluation vocabulary.
        aliases = {
            "confirmed": "correct",
            "rejected": "incorrect",
            "inconclusive": "unknown",
        }
        return aliases.get(str(value), value)


ExperimentStatus = Literal[
    "created",
    "warming",
    "running",
    "recovering",
    "completed",
    "failed",
]
DatasetSplit = Literal["train", "validation", "test", "holdout"]
RESERVED_EXPERIMENT_RUN_ID = UUID(int=0)


class ExperimentRunRequest(BaseModel):
    """Authoritative metadata for one reproducible workload/fault run.

    The caller may generate ``id`` before the run starts and stamp that UUID into
    OpenTelemetry resource attributes. Reusing the same UUID is idempotent and
    enriches an auto-registered telemetry placeholder instead of creating a
    second run.
    """

    model_config = ConfigDict(extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(min_length=1, max_length=512)
    source_repository: str | None = Field(default=None, max_length=2048)
    source_version: str | None = Field(default=None, max_length=255)
    source_sha: str | None = Field(default=None, max_length=255)
    seed: int | None = None
    workload: str | None = Field(default=None, max_length=512)
    status: ExperimentStatus = "created"
    split: DatasetSplit | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    configuration: dict[str, Any] = Field(default_factory=dict)
    environment: dict[str, Any] = Field(default_factory=dict)

    @field_validator("started_at", "ended_at")
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        return _ensure_aware(value) if value is not None else None

    @model_validator(mode="after")
    def valid_time_range(self) -> "ExperimentRunRequest":
        if self.started_at and self.ended_at and self.ended_at < self.started_at:
            raise ValueError("ended_at must be greater than or equal to started_at")
        return self


class ExperimentRunUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: ExperimentStatus | None = None
    split: DatasetSplit | None = None
    workload: str | None = Field(default=None, max_length=512)
    started_at: datetime | None = None
    ended_at: datetime | None = None
    configuration: dict[str, Any] | None = None
    environment: dict[str, Any] | None = None

    @field_validator("started_at", "ended_at")
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        return _ensure_aware(value) if value is not None else None

    @model_validator(mode="after")
    def contains_update(self) -> "ExperimentRunUpdate":
        if not self.model_fields_set:
            raise ValueError("at least one experiment field must be supplied")
        if self.started_at and self.ended_at and self.ended_at < self.started_at:
            raise ValueError("ended_at must be greater than or equal to started_at")
        return self


class FaultInjectionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    flag_name: str = Field(min_length=1, max_length=512)
    variant: str | None = Field(default=None, max_length=255)
    injector: str = Field(min_length=1, max_length=512)
    causal_entity_type: str = Field(min_length=1, max_length=128)
    causal_entity_id: str = Field(min_length=1, max_length=1024)
    causal_edge_source: str | None = Field(default=None, max_length=512)
    causal_edge_target: str | None = Field(default=None, max_length=512)
    mechanism: str = Field(min_length=1, max_length=255)
    impacted_sli: str = Field(min_length=1, max_length=512)
    injected_at: datetime
    cleared_at: datetime | None = None
    expected_runbook_key: str | None = Field(default=None, max_length=512)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("injected_at", "cleared_at")
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        return _ensure_aware(value) if value is not None else None

    @model_validator(mode="after")
    def valid_time_range(self) -> "FaultInjectionRequest":
        if self.cleared_at and self.cleared_at < self.injected_at:
            raise ValueError("cleared_at must be greater than or equal to injected_at")
        if bool(self.causal_edge_source) != bool(self.causal_edge_target):
            raise ValueError("causal edge source and target must be supplied together")
        return self


class GroundTruthLabelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: UUID | None = None
    entity_type: str = Field(min_length=1, max_length=128)
    entity_id: str = Field(min_length=1, max_length=1024)
    fault_class: str = Field(min_length=1, max_length=255)
    relevance: int = Field(default=1, ge=0, le=3)
    is_primary: bool = True


class IngestSummary(BaseModel):
    accepted: dict[str, int]
    inserted: dict[str, int]
    duplicates: dict[str, int]
    rejected: int = 0
