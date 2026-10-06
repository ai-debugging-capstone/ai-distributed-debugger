"""Transparent topology-aware root-cause ranking with explicit abstention."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .domain import AnomalyResult, CandidateRanking, EvidenceItem, Incident
from .graph import DirectedDependencyGraph


@dataclass(frozen=True, slots=True)
class RankerDecision:
    rankings: tuple[CandidateRanking, ...]
    abstained: bool
    abstention_reason: str | None
    data_completeness: float

    @property
    def actionable_rankings(self) -> tuple[CandidateRanking, ...]:
        """Ranked causes only; abstention is reported separately, never counted in Hit@k."""

        return tuple(item for item in self.rankings if item.entity_id != "inconclusive")


def _parse_time(value: str | datetime | None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _bounded_anomaly(score: float) -> float:
    return 1.0 - math.exp(-max(0.0, score) / 4.0)


def _fault_class(result: AnomalyResult | None) -> str:
    if result is None or not result.feature_scores:
        return "unknown"
    feature = max(result.feature_scores, key=result.feature_scores.get)
    if feature in {"error_rate", "error_count", "log_error_count"}:
        return "service_error"
    if feature.startswith("latency"):
        return "service_error"
    if "cpu" in feature:
        return "resource_saturation"
    if "memory" in feature or "heap" in feature:
        return "resource_saturation"
    if "queue" in feature or "lag" in feature:
        return "queue_lag"
    if feature == "request_count":
        return "workload_spike"
    return "service_error"


def _looks_like_database(entity_id: str) -> bool:
    lowered = entity_id.lower()
    return any(token in lowered for token in ("postgres", "postgresql", "database", "mysql")) or lowered.endswith(
        ("-db", "_db", ".db")
    )


def canonical_edge_fault_class(entity_id: str, result: AnomalyResult | None) -> str:
    """Map observed edge symptoms to the seeded deterministic runbook taxonomy."""

    lowered = entity_id.lower()
    base = _fault_class(result)
    if (
        _looks_like_database(lowered)
        and result
        and any(name.startswith("latency") for name in result.feature_scores)
    ):
        return "database_lock"
    if any(token in lowered for token in ("kafka", "queue", "rabbit", "pubsub")):
        return "queue_lag"
    if base == "service_error":
        return "dependency_unreachable"
    return base


def canonical_node_semantics(entity_id: str, result: AnomalyResult | None) -> tuple[str, str]:
    lowered = entity_id.lower()
    features = result.feature_scores if result else {}
    latency_anomalous = any(
        name.startswith("latency") and value > 0 for name, value in features.items()
    )
    if _looks_like_database(lowered):
        return "database", "database_lock" if latency_anomalous else "resource_saturation"
    if any(token in lowered for token in ("kafka", "queue", "rabbitmq", "pubsub")):
        return "queue", "queue_lag"
    if entity_id == "external_workload":
        return "external", "workload_spike"
    return "service", _fault_class(result)


def _confidence(probability: float, margin: float, completeness: float) -> str:
    if probability >= 0.65 and margin >= 0.15 and completeness >= 0.75:
        return "high"
    if probability >= 0.4 and margin >= 0.06 and completeness >= 0.5:
        return "medium"
    return "low"


class HeuristicRootCauseRanker:
    """Evidence-first baseline used before and alongside learned rankers.

    The weights are intentionally explicit.  They can be changed and ablated in
    evaluation without hiding causal assumptions inside an opaque model.
    """

    def __init__(
        self,
        *,
        anomaly_weight: float = 0.35,
        temporal_weight: float = 0.25,
        propagation_weight: float = 0.30,
        quality_weight: float = 0.10,
        min_score: float = 0.35,
        min_margin: float = 0.03,
        min_completeness: float = 0.40,
        model_version: str = "heuristic-v1",
    ) -> None:
        total = anomaly_weight + temporal_weight + propagation_weight + quality_weight
        if not math.isclose(total, 1.0, abs_tol=1e-9):
            raise ValueError("ranking weights must sum to 1")
        self.weights = {
            "anomaly": anomaly_weight,
            "temporal": temporal_weight,
            "propagation": propagation_weight,
            "quality": quality_weight,
        }
        self.min_score = min_score
        self.min_margin = min_margin
        self.min_completeness = min_completeness
        self.model_version = model_version

    def rank(
        self,
        incident: Incident,
        anomalies: Sequence[AnomalyResult],
        graph: DirectedDependencyGraph,
        *,
        top_k: int = 10,
    ) -> RankerDecision:
        relevant = [
            result
            for result in anomalies
            if result.bucket.run_id == incident.run_id
            and result.bucket.bucket_start >= incident.started_at
            and (incident.ended_at is None or result.bucket.bucket_start <= incident.ended_at)
        ]
        by_entity: dict[str, AnomalyResult] = {}
        for result in relevant:
            old = by_entity.get(result.bucket.entity_id)
            if old is None or result.score > old.score:
                by_entity[result.bucket.entity_id] = result

        anomaly_times = {
            entity: parsed
            for entity, value in (incident.metadata.get("anomaly_times") or {}).items()
            if (parsed := _parse_time(value)) is not None
        }
        for entity, result in by_entity.items():
            anomaly_times.setdefault(entity, result.bucket.bucket_start)
        earliest = min(anomaly_times.values(), default=incident.started_at)

        candidates = set(by_entity) | set(incident.affected_entities)
        for entity in list(candidates):
            candidates.update(graph.ancestors(entity, max_depth=3))
            candidates.update(graph.descendants(entity, max_depth=3))
        if not candidates:
            return self._fully_abstained("no candidates were observable")

        raw: list[dict[str, Any]] = []
        completeness_values: list[float] = []
        for entity in sorted(candidates):
            result = by_entity.get(entity)
            anomaly = _bounded_anomaly(result.score if result else 0.0)
            onset = anomaly_times.get(entity)
            delay = max(0.0, (onset - earliest).total_seconds()) if onset else 300.0
            temporal = math.exp(-delay / 60.0) if onset else 0.0
            propagation, propagation_details = graph.propagation_score(
                entity,
                incident.affected_entities,
                anomaly_times=anomaly_times,
            )
            quality = 0.0 if result is None else max(
                0.0, 1.0 - sum(result.bucket.missingness.values()) / max(1, len(result.bucket.missingness))
            )
            completeness_values.append(quality)
            components = {
                "anomaly": anomaly,
                "temporal": temporal,
                "propagation": propagation,
                "quality": quality,
            }
            score = sum(self.weights[name] * value for name, value in components.items())
            entity_type, fault_class = canonical_node_semantics(entity, result)
            evidence = self._node_evidence(entity, result, onset, earliest, propagation, propagation_details)
            raw.append(
                {
                    "entity_type": entity_type,
                    "entity_id": entity,
                    "score": score,
                    "fault_class": fault_class,
                    "components": components,
                    "evidence": evidence,
                }
            )

        # Edges are first-class candidates: dependency failures are often more
        # accurate than blaming either endpoint alone.
        for edge in graph.subgraph_edges(candidates):
            target_result = by_entity.get(edge.target)
            endpoint_anomaly = _bounded_anomaly(target_result.score if target_result else 0.0)
            edge_health = min(1.0, edge.error_rate * 4.0 + math.log1p(max(0.0, edge.latency_ms_p95)) / 20.0)
            propagation, details = graph.propagation_score(
                edge.target, incident.affected_entities, anomaly_times=anomaly_times
            )
            onset = anomaly_times.get(edge.target) or anomaly_times.get(edge.source)
            delay = max(0.0, (onset - earliest).total_seconds()) if onset else 300.0
            temporal = math.exp(-delay / 60.0) if onset else 0.0
            quality = (
                max(
                    0.0,
                    1.0
                    - sum(target_result.bucket.missingness.values())
                    / max(1, len(target_result.bucket.missingness)),
                )
                if target_result
                else float(edge.request_count > 0)
            )
            # Preserve the same interpretable four components; edge telemetry is
            # folded into anomaly rather than introduced as a hidden fifth weight.
            # If the server is unreachable there is intentionally no target
            # service span. In that case the failed client edge is the primary
            # observable rather than weak supporting evidence for a missing
            # endpoint anomaly.
            direct_unreachable = edge.error_count > 0 and (
                target_result is None
                or target_result.bucket.value("error_count") == 0
            )
            anomaly = (
                edge_health
                if direct_unreachable
                else 0.65 * endpoint_anomaly + 0.35 * edge_health
            )
            components = {
                "anomaly": anomaly,
                "temporal": temporal,
                "propagation": propagation,
                "quality": quality,
            }
            edge_fault_class = (
                "dependency_unreachable"
                if direct_unreachable
                else canonical_edge_fault_class(edge.edge_id, target_result)
            )
            # A concrete failing dependency edge is more actionable and precise
            # than blaming either endpoint. This explicit, auditable specificity
            # prior prevents a generic downstream service anomaly from masking a
            # PostgreSQL/queue/connection edge with matching evidence.
            specificity_bonus = (
                0.32
                if direct_unreachable
                else {
                    "database_lock": 0.18,
                    "dependency_unreachable": 0.05,
                    "queue_lag": 0.08,
                }.get(edge_fault_class, 0.0)
            )
            score = min(
                1.0,
                sum(self.weights[name] * value for name, value in components.items())
                + specificity_bonus,
            )
            components["specificity_bonus"] = specificity_bonus
            evidence = list(self._node_evidence(edge.edge_id, target_result, onset, earliest, propagation, details))
            evidence.append(
                EvidenceItem(
                    evidence_type="dependency_health",
                    entity_id=edge.edge_id,
                    occurred_at=edge.bucket_start,
                    signal_name="edge_error_rate",
                    value=edge.error_rate,
                    baseline=None,
                    contribution=0.35 * self.weights["anomaly"],
                    details={
                        "request_count": edge.request_count,
                        "latency_ms_p95": edge.latency_ms_p95,
                        "protocol": edge.protocol,
                    },
                )
            )
            raw.append(
                {
                    "entity_type": "dependency_edge",
                    "entity_id": edge.edge_id,
                    "score": score,
                    "fault_class": edge_fault_class,
                    "components": components,
                    "evidence": tuple(evidence),
                }
            )

        raw.sort(key=lambda item: (-item["score"], item["entity_type"], item["entity_id"]))
        logits = [math.exp((item["score"] - raw[0]["score"]) / 0.20) for item in raw]
        denominator = sum(logits)
        probabilities = [value / denominator for value in logits]
        top_score = raw[0]["score"]
        margin = top_score - (raw[1]["score"] if len(raw) > 1 else 0.0)
        completeness = sum(completeness_values) / len(completeness_values) if completeness_values else 0.0

        reason: str | None = None
        if completeness < self.min_completeness:
            reason = f"telemetry completeness {completeness:.0%} is below {self.min_completeness:.0%}"
        elif top_score < self.min_score:
            reason = f"best candidate score {top_score:.2f} is below {self.min_score:.2f}"
        elif margin < self.min_margin:
            reason = f"top-candidate margin {margin:.2f} is below {self.min_margin:.2f}"

        rankings: list[CandidateRanking] = []
        for index, (item, probability) in enumerate(zip(raw[:top_k], probabilities[:top_k]), start=1):
            evidence = tuple(item["evidence"])
            summary = "; ".join(
                f"{name}={value:.2f}" for name, value in item["components"].items()
            )
            rankings.append(
                CandidateRanking(
                    entity_type=item["entity_type"],
                    entity_id=item["entity_id"],
                    rank=index,
                    score=item["score"],
                    probability=probability,
                    confidence=_confidence(probability, margin, completeness),
                    fault_class=item["fault_class"],
                    evidence_summary=summary,
                    evidence=evidence,
                    model_version=self.model_version,
                )
            )
        if reason:
            rankings.append(
                CandidateRanking(
                    entity_type="system",
                    entity_id="inconclusive",
                    rank=len(rankings) + 1,
                    score=1.0 - top_score,
                    probability=max(0.0, 1.0 - probabilities[0]),
                    confidence="low",
                    fault_class="insufficient_evidence",
                    evidence_summary=reason,
                    evidence=(),
                    model_version=self.model_version,
                )
            )
        return RankerDecision(tuple(rankings), bool(reason), reason, completeness)

    def _fully_abstained(self, reason: str) -> RankerDecision:
        ranking = CandidateRanking(
            entity_type="system",
            entity_id="inconclusive",
            rank=1,
            score=1.0,
            probability=1.0,
            confidence="low",
            fault_class="insufficient_evidence",
            evidence_summary=reason,
            model_version=self.model_version,
        )
        return RankerDecision((ranking,), True, reason, 0.0)

    def _node_evidence(
        self,
        entity: str,
        result: AnomalyResult | None,
        onset: datetime | None,
        earliest: datetime,
        propagation: float,
        propagation_details: Mapping[str, Any],
    ) -> tuple[EvidenceItem, ...]:
        evidence: list[EvidenceItem] = []
        if result:
            for signal, contribution in sorted(
                result.contributions.items(), key=lambda item: (-item[1], item[0])
            )[:3]:
                evidence.append(
                    EvidenceItem(
                        evidence_type="signal_anomaly",
                        entity_id=entity,
                        occurred_at=result.bucket.bucket_start,
                        signal_name=signal,
                        value=result.bucket.value(signal),
                        baseline=result.baselines.get(signal),
                        contribution=contribution * self.weights["anomaly"],
                        details={"robust_z": result.feature_scores.get(signal, 0.0)},
                    )
                )
        if onset:
            evidence.append(
                EvidenceItem(
                    evidence_type="temporal_precedence",
                    entity_id=entity,
                    occurred_at=onset,
                    signal_name="anomaly_onset_delay_seconds",
                    value=max(0.0, (onset - earliest).total_seconds()),
                    baseline=0.0,
                    contribution=self.weights["temporal"] * math.exp(
                        -max(0.0, (onset - earliest).total_seconds()) / 60.0
                    ),
                )
            )
        evidence.append(
            EvidenceItem(
                evidence_type="topology_propagation",
                entity_id=entity,
                occurred_at=onset or earliest,
                signal_name="downstream_coverage",
                value=propagation,
                baseline=0.0,
                contribution=propagation * self.weights["propagation"],
                details=dict(propagation_details),
            )
        )
        return tuple(evidence)
