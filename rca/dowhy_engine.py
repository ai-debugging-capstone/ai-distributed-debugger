"""Use DoWhy GCM to attribute synchronous service-latency anomalies.

The adapter prepares observed numeric windows and a latency-influence DAG.  The
causal fitting and Shapley attribution are supplied by the external DoWhy
library; attribution values are signed contributions, never probabilities.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import random
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from importlib.metadata import version
from statistics import median
from typing import Any

from .domain import AnomalyResult, CandidateRanking, EvidenceItem, Incident
from .graph import DirectedDependencyGraph
from .ranking import HeuristicRootCauseRanker, RankerDecision, canonical_node_semantics

LOGGER = logging.getLogger("rca.dowhy")
LATENCY_SIGNAL = "latency_ms_p95"
ASSUMPTIONS = (
    "Calls in the modeled path are synchronous; dependency latency influences caller latency.",
    "The observed dependency DAG is correct and relevant common causes are measured.",
    "Healthy aligned bucket p95 latencies approximate stable additive causal mechanisms.",
)
LIMITATIONS = (
    "Window-level attribution is conditional on these assumptions, not causal proof.",
    "Scores are signed anomaly contributions, not calibrated probabilities.",
    "This engine currently attributes latency only; other mechanisms use the fallback.",
    "Latency attribution does not establish a specific lock, resource, or code-level failure mechanism.",
)


class UnsupportedLatencyData(ValueError):
    """An observed incident cannot support the configured latency model."""


@dataclass(frozen=True)
class LatencyAnalysisInput:
    target: str
    nodes: tuple[str, ...]
    causal_edges: tuple[tuple[str, str], ...]
    baseline_times: tuple[datetime, ...]
    baseline: tuple[Mapping[str, float], ...]
    observation_time: datetime
    observation: Mapping[str, float]
    results: Mapping[str, AnomalyResult]


def _latency(result: AnomalyResult | None) -> float | None:
    if result is None:
        return None
    bucket = result.bucket
    if bucket.value("request_count") <= 0 or bucket.missingness.get("span", 0) >= 1:
        return None
    try:
        value = float(bucket.features[LATENCY_SIGNAL])
    except (KeyError, TypeError, ValueError):
        return None
    return value if math.isfinite(value) and value > 0 else None


def _reachable(start: str, outgoing: Mapping[str, set[str]]) -> set[str]:
    visited = {start}
    pending = [start]
    while pending:
        for node in sorted(outgoing.get(pending.pop(), ())):
            if node not in visited:
                visited.add(node)
                pending.append(node)
    return visited


def _acyclic(nodes: Sequence[str], edges: Sequence[tuple[str, str]]) -> bool:
    incoming = dict.fromkeys(nodes, 0)
    outgoing: dict[str, set[str]] = {node: set() for node in nodes}
    for source, target in edges:
        outgoing[source].add(target)
        incoming[target] += 1
    ready = [node for node in nodes if incoming[node] == 0]
    visited = 0
    while ready:
        source = ready.pop()
        visited += 1
        for target in outgoing[source]:
            incoming[target] -= 1
            if incoming[target] == 0:
                ready.append(target)
    return visited == len(nodes)


class DoWhyRootCauseRanker:
    """Bounded, run-scoped adapter with explicit fallback for unsupported data."""

    def __init__(
        self,
        *,
        min_baseline_samples: int = 10,
        max_history_buckets: int = 300,
        max_nodes: int = 12,
        num_samples: int = 100,
        seed: int = 42,
        logger: logging.Logger = LOGGER,
    ) -> None:
        if min_baseline_samples < 3 or max_history_buckets < min_baseline_samples:
            raise ValueError("DoWhy requires at least three baseline rows and enough history")
        if not 2 <= max_nodes <= 32 or not 20 <= num_samples <= 5000:
            raise ValueError("DoWhy node/sample budgets are outside supported bounds")
        self.min_baseline_samples = min_baseline_samples
        self.max_history_buckets = max_history_buckets
        self.max_nodes = max_nodes
        self.num_samples = num_samples
        self.seed = seed
        self.logger = logger
        self.baseline = HeuristicRootCauseRanker()
        self._history: OrderedDict[
            str | None, dict[datetime, dict[str, AnomalyResult]]
        ] = OrderedDict()
        self._models: OrderedDict[str, Any] = OrderedDict()
        self._provider_version = "unknown"

    def observe(self, results: Sequence[AnomalyResult]) -> None:
        """Retain observed windows, including warm-up, without using labels."""

        for result in results:
            bucket = result.bucket
            history = self._history.setdefault(bucket.run_id, {})
            self._history.move_to_end(bucket.run_id)
            history.setdefault(bucket.bucket_start, {})[bucket.entity_id] = result
            for timestamp in sorted(history)[: -self.max_history_buckets]:
                del history[timestamp]
        # Retired experiments must not accumulate indefinitely in a live worker.
        while len(self._history) > 32:
            self._history.popitem(last=False)

    def _prepare(
        self,
        incident: Incident,
        anomalies: Sequence[AnomalyResult],
        graph: DirectedDependencyGraph,
    ) -> LatencyAnalysisInput:
        scoped = [
            result
            for result in anomalies
            if result.bucket.run_id == incident.run_id
            and result.bucket.bucket_start >= incident.started_at
            and (incident.ended_at is None or result.bucket.bucket_start <= incident.ended_at)
        ]
        if not scoped:
            raise UnsupportedLatencyData("no current observations for this incident/run")
        current_time = max(result.bucket.bucket_start for result in scoped)
        # Use one complete timestamp, never independently latest values per node.
        current = {
            result.bucket.entity_id: result
            for result in scoped
            if result.bucket.bucket_start == current_time
        }
        candidates = [
            entity
            for entity in incident.affected_entities
            if entity in current
            and _latency(current[entity]) is not None
            and current[entity].feature_scores.get(LATENCY_SIGNAL, 0) >= 3.5
        ]
        if not candidates:
            raise UnsupportedLatencyData("no observed p95 latency anomaly in the current bucket")
        call_edges = [
            edge
            for edge in graph.edges.values()
            if edge.run_id == incident.run_id and edge.bucket_start <= current_time
        ]
        outgoing: dict[str, set[str]] = {}
        for edge in call_edges:
            outgoing.setdefault(edge.source, set()).add(edge.target)
        # Explain an upstream affected caller, whose latency reflects its dependencies.
        target = min(candidates, key=lambda node: (-len(_reachable(node, outgoing)), node))
        selected = _reachable(target, outgoing)
        if len(selected) < 2:
            raise UnsupportedLatencyData("no observed dependency path for the latency target")
        if len(selected) > self.max_nodes:
            raise UnsupportedLatencyData("latency graph exceeds the configured node budget")
        selected_edges = [
            edge for edge in call_edges if edge.source in selected and edge.target in selected
        ]
        for edge in selected_edges:
            protocol = edge.protocol.lower()
            endpoint = f"{edge.source} {edge.target}".lower()
            if any(
                token in protocol
                for token in ("kafka", "amqp", "messaging", "pubsub", "producer", "publish")
            ) or any(token in endpoint for token in ("kafka", "rabbitmq", "pubsub")):
                raise UnsupportedLatencyData("asynchronous dependency is outside the latency model")
        nodes = tuple(sorted(selected))
        causal_edges = tuple(sorted({(edge.target, edge.source) for edge in selected_edges}))
        if not _acyclic(nodes, causal_edges):
            raise UnsupportedLatencyData("observed latency dependency graph contains a cycle")
        observation = {node: _latency(current.get(node)) for node in nodes}
        if any(value is None for value in observation.values()):
            raise UnsupportedLatencyData("current latency row is incomplete; missing spans are not imputed")

        baseline_times: list[datetime] = []
        baseline: list[Mapping[str, float]] = []
        for timestamp, row in sorted(self._history.get(incident.run_id, {}).items()):
            if timestamp >= incident.started_at:
                continue
            if any(node not in row or row[node].score >= 3.5 for node in nodes):
                continue
            values = {node: _latency(row[node]) for node in nodes}
            if any(value is None for value in values.values()):
                continue
            baseline_times.append(timestamp)
            baseline.append(values)
        if len(baseline) < self.min_baseline_samples:
            raise UnsupportedLatencyData(
                f"only {len(baseline)} aligned healthy baseline rows; "
                f"need {self.min_baseline_samples}"
            )
        for node in nodes:
            values = [row[node] for row in baseline]
            if max(values) - min(values) <= max(1.0, abs(median(values))) * 1e-8:
                raise UnsupportedLatencyData(f"healthy latency baseline is constant for {node}")
        return LatencyAnalysisInput(
            target,
            nodes,
            causal_edges,
            tuple(baseline_times),
            tuple(baseline),
            current_time,
            observation,
            current,
        )

    def _attribute(self, prepared: LatencyAnalysisInput) -> Mapping[str, float]:
        # Lazy imports preserve the lightweight fallback when the optional provider
        # cannot load. The container installs the pinned external implementation.
        import networkx as nx
        import numpy as np
        import pandas as pd
        from dowhy import gcm
        from dowhy.gcm.shapley import ShapleyApproximationMethods, ShapleyConfig

        self._provider_version = version("dowhy")
        signature = hashlib.sha256(
            json.dumps(
                {"nodes": prepared.nodes, "edges": prepared.causal_edges, "baseline": prepared.baseline},
                sort_keys=True,
            ).encode()
        ).hexdigest()
        random_state, numpy_state = random.getstate(), np.random.get_state()
        progress = gcm.config.show_progress_bars
        try:
            random.seed(self.seed)
            np.random.seed(self.seed)
            gcm.config.show_progress_bars = False
            model = self._models.get(signature)
            if model is None:
                causal_graph = nx.DiGraph()
                causal_graph.add_nodes_from(prepared.nodes)
                causal_graph.add_edges_from(prepared.causal_edges)
                model = gcm.InvertibleStructuralCausalModel(causal_graph)
                for node in prepared.nodes:
                    mechanism = (
                        gcm.EmpiricalDistribution()
                        if causal_graph.in_degree(node) == 0
                        else gcm.AdditiveNoiseModel(gcm.ml.create_linear_regressor())
                    )
                    model.set_causal_mechanism(node, mechanism)
                gcm.fit(model, pd.DataFrame(prepared.baseline, columns=prepared.nodes))
                self._models[signature] = model
                while len(self._models) > 16:
                    self._models.popitem(last=False)
            self._models.move_to_end(signature)
            contributions = gcm.attribute_anomalies(
                model,
                prepared.target,
                anomaly_samples=pd.DataFrame([prepared.observation], columns=prepared.nodes),
                num_distribution_samples=self.num_samples,
                shapley_config=ShapleyConfig(
                    approximation_method=ShapleyApproximationMethods.PERMUTATION,
                    num_permutations=32,
                    n_jobs=1,
                ),
            )
            scores = {
                str(node): float(np.asarray(value).reshape(-1)[0])
                for node, value in contributions.items()
            }
        finally:
            random.setstate(random_state)
            np.random.set_state(numpy_state)
            gcm.config.show_progress_bars = progress
        if set(scores) != set(prepared.nodes) or any(not math.isfinite(value) for value in scores.values()):
            raise ValueError("DoWhy returned incomplete or non-finite node contributions")
        return scores

    def _fallback(
        self,
        incident: Incident,
        anomalies: Sequence[AnomalyResult],
        graph: DirectedDependencyGraph,
        reason: str,
        top_k: int,
    ) -> RankerDecision:
        incident.metadata["analysis"] = {
            "provider": "dowhy-gcm",
            "status": "fallback",
            "reason": reason,
            "active_provider": "heuristic-v1",
            "score_kind": "heuristic_score",
            "target": None,
            "baseline_samples": 0,
            "assumptions": list(ASSUMPTIONS),
            "limitations": list(LIMITATIONS),
        }
        return self.baseline.rank(incident, anomalies, graph, top_k=top_k)

    def rank(
        self,
        incident: Incident,
        anomalies: Sequence[AnomalyResult],
        graph: DirectedDependencyGraph,
        *,
        top_k: int = 10,
    ) -> RankerDecision:
        if top_k < 1:
            raise ValueError("top_k must be positive")
        try:
            prepared = self._prepare(incident, anomalies, graph)
            scores = self._attribute(prepared)
            if set(scores) != set(prepared.nodes) or any(not math.isfinite(value) for value in scores.values()):
                raise ValueError("DoWhy returned incomplete or non-finite node contributions")
        except UnsupportedLatencyData as error:
            return self._fallback(incident, anomalies, graph, str(error), top_k)
        except Exception as error:  # noqa: BLE001 - optional provider failures must retain live diagnosis
            self.logger.warning("DoWhy latency attribution unavailable: %s: %s", type(error).__name__, error)
            return self._fallback(
                incident, anomalies, graph,
                f"provider unavailable ({type(error).__name__}): {str(error)[:300]}", top_k,
            )
        model_version = f"dowhy-gcm-{self._provider_version}-latency-v1"
        context = {
            "provider": "dowhy-gcm",
            "active_provider": "dowhy-gcm",
            "status": "analyzed",
            "reason": "aligned healthy baseline and synchronous latency DAG passed the analysis gates",
            "score_kind": "causal_attribution",
            "engine_version": self._provider_version,
            "target": prepared.target,
            "signal": LATENCY_SIGNAL,
            "baseline_samples": len(prepared.baseline),
            "baseline_start": prepared.baseline_times[0].isoformat(),
            "baseline_end": prepared.baseline_times[-1].isoformat(),
            "observation_time": prepared.observation_time.isoformat(),
            "modeled_nodes": list(prepared.nodes),
            "causal_edges": [list(edge) for edge in prepared.causal_edges],
            "assumptions": list(ASSUMPTIONS),
            "limitations": list(LIMITATIONS),
            "num_distribution_samples": self.num_samples,
            "max_permutations": 32,
        }
        incident.metadata["analysis"] = context
        ordered = sorted(scores, key=lambda node: (-scores[node], node))
        rankings = []
        for rank, node in enumerate(ordered[:top_k], start=1):
            entity_type, _ = canonical_node_semantics(node, prepared.results.get(node))
            value = prepared.observation[node]
            baseline_value = median(row[node] for row in prepared.baseline)
            rankings.append(
                CandidateRanking(
                    entity_type=entity_type,
                    entity_id=node,
                    rank=rank,
                    score=scores[node],
                    probability=None,
                    confidence="low",
                    # The existing generic service-error runbook investigates
                    # telemetry/dependencies. A latency contribution alone cannot
                    # prove a database lock or other specific failure mechanism.
                    fault_class="service_error",
                    evidence_summary=(
                        f"DoWhy latency attribution={scores[node]:.4g} for {prepared.target}; "
                        f"{LATENCY_SIGNAL}={value:.4g} ms vs healthy median={baseline_value:.4g} ms; "
                        "conditional attribution, not probability"
                    ),
                    evidence=(EvidenceItem(
                        evidence_type="causal_attribution",
                        entity_id=node,
                        occurred_at=prepared.observation_time,
                        signal_name=LATENCY_SIGNAL,
                        value=value,
                        baseline=baseline_value,
                        contribution=scores[node],
                        details={**context, "model_version": model_version},
                    ),),
                    model_version=model_version,
                )
            )
        abstained = not ordered or scores[ordered[0]] <= 1e-8
        reason = "DoWhy found no positive material latency contribution" if abstained else None
        if reason:
            context["reason"] = reason
            rankings.append(CandidateRanking(
                entity_type="system", entity_id="inconclusive", rank=len(rankings) + 1,
                score=0.0, probability=None, confidence="low", fault_class="insufficient_evidence",
                evidence_summary=reason, model_version=model_version,
            ))
        return RankerDecision(tuple(rankings), abstained, reason, 1.0)
