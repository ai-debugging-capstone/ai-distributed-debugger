"""Small, serialisable domain objects shared by every RCA component."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class FeatureBucket:
    """Aggregated telemetry for one entity in one fixed time bucket.

    ``features`` carries both the stable feature set mirrored into SQL columns and
    optional, namespaced measurements (for example ``metric.process_cpu.mean``).
    Keeping this object generic lets the pure analytics layer evolve without a
    database migration for every experimental feature.
    """

    bucket_start: datetime
    bucket_end: datetime
    entity_type: str
    entity_id: str
    run_id: str | None
    features: Mapping[str, float]
    sample_count: int = 0
    missingness: Mapping[str, float] = field(default_factory=dict)

    def value(self, name: str, default: float = 0.0) -> float:
        value = self.features.get(name, default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default


@dataclass(frozen=True, slots=True)
class DependencyEdge:
    bucket_start: datetime
    source: str
    target: str
    run_id: str | None = None
    protocol: str = "unknown"
    request_count: int = 0
    error_count: int = 0
    latency_ms_p50: float = 0.0
    latency_ms_p95: float = 0.0
    latency_ms_p99: float = 0.0
    attributes: Mapping[str, Any] = field(default_factory=dict)

    @property
    def edge_id(self) -> str:
        return f"{self.source}->{self.target}"

    @property
    def error_rate(self) -> float:
        return self.error_count / self.request_count if self.request_count else 0.0


@dataclass(frozen=True, slots=True)
class AnomalyResult:
    bucket: FeatureBucket
    score: float
    onset_score: float
    feature_scores: Mapping[str, float]
    baselines: Mapping[str, float]
    contributions: Mapping[str, float]
    sufficient_history: bool
    explanation: tuple[str, ...] = ()


@dataclass(slots=True)
class Incident:
    started_at: datetime
    detected_at: datetime
    trigger_entity: str
    affected_entities: list[str]
    max_anomaly: float
    severity: str
    run_id: str | None = None
    id: str = field(default_factory=lambda: str(uuid4()))
    status: str = "open"
    ended_at: datetime | None = None
    summary: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EvidenceItem:
    evidence_type: str
    entity_id: str
    occurred_at: datetime
    signal_name: str
    value: float | str | None
    baseline: float | str | None
    contribution: float
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CandidateRanking:
    entity_type: str
    entity_id: str
    rank: int
    score: float
    probability: float
    confidence: str
    fault_class: str
    evidence_summary: str
    evidence: tuple[EvidenceItem, ...] = ()
    model_version: str = "heuristic-v1"
    online_features: Mapping[str, float] = field(default_factory=dict)
