"""Directed, time-aware dependency graph construction and propagation features."""

from __future__ import annotations

import re
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .domain import DependencyEdge
from .features import (
    _as_datetime,
    _is_error_span,
    _span_duration_ms,
    floor_time,
    percentile,
)


def _protocol(attributes: Mapping[str, Any]) -> str:
    for key in (
        "rpc.system",
        "rpc.system.name",
        "network.protocol.name",
        "http.request.method",
        "db.system",
        "messaging.system",
    ):
        if attributes.get(key):
            return str(attributes[key]).lower()
    return "unknown"


def _normalise_rpc_service(value: Any, *, require_method_path: bool = False) -> str | None:
    """Map a qualified RPC service/method to the observed service naming style."""

    raw = str(value or "").strip()
    if not raw or (require_method_path and "/" not in raw):
        return None
    service = raw.split("/", 1)[0].rsplit(".", 1)[-1]
    service = service.removesuffix("Service")
    service = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "-", service)
    service = re.sub(r"[^A-Za-z0-9]+", "-", service).strip("-").lower()
    return service or None


def _peer_service(span: Mapping[str, Any]) -> str | None:
    attrs = span.get("attributes") or {}
    for key in (
        "peer.service",
        "server.address",
        "net.peer.name",
        "db.namespace",
        "db.system",
        "messaging.destination.name",
    ):
        value = attrs.get(key)
        if value:
            return str(value)
    # An unreachable RPC target has no server span and may omit endpoint
    # attributes, but the client span still carries its qualified RPC service.
    # Newer Demo instrumentation records the full service/method in
    # ``rpc.method`` (for example ``oteldemo.PaymentService/Charge``).
    explicit_rpc_service = _normalise_rpc_service(attrs.get("rpc.service"))
    if explicit_rpc_service:
        return explicit_rpc_service
    return _normalise_rpc_service(
        attrs.get("rpc.method"), require_method_path=True
    )


@dataclass
class _EdgeAccumulator:
    durations: list[float] = field(default_factory=list)
    errors: int = 0
    protocol_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))


def build_dependency_edges(
    spans: Iterable[Mapping[str, Any]], *, bucket_seconds: int = 10
) -> list[DependencyEdge]:
    """Infer directed service calls from trace parentage and client peer metadata.

    Cross-service parent/child relationships are preferred.  Client/producer spans
    with ``peer.service`` (or standard endpoint attributes) fill gaps when a remote
    server span was not sampled.
    """

    records = [dict(span) for span in spans]
    by_span = {
        (str(span.get("trace_id") or ""), str(span.get("span_id") or "")): span
        for span in records
        if span.get("trace_id") and span.get("span_id")
    }
    grouped: dict[tuple[str | None, datetime, str, str], _EdgeAccumulator] = defaultdict(_EdgeAccumulator)
    represented: set[tuple[str, str]] = set()

    for child in records:
        trace_id = str(child.get("trace_id") or "")
        parent_id = str(child.get("parent_span_id") or "")
        parent = by_span.get((trace_id, parent_id))
        if parent is None:
            continue
        source = str(parent.get("service_name") or "unknown")
        target = str(child.get("service_name") or "unknown")
        if source == target:
            continue
        timestamp = child.get("start_time") or child.get("observed_at")
        if timestamp is None:
            continue
        run_id = str(child["run_id"]) if child.get("run_id") else None
        key = (run_id, floor_time(_as_datetime(timestamp), bucket_seconds), source, target)
        acc = grouped[key]
        acc.durations.append(_span_duration_ms(child))
        acc.errors += int(_is_error_span(child))
        acc.protocol_counts[_protocol(child.get("attributes") or {})] += 1
        represented.add((trace_id, str(child.get("span_id") or "")))

    for span in records:
        trace_and_span = (str(span.get("trace_id") or ""), str(span.get("span_id") or ""))
        if trace_and_span in represented:
            continue
        kind = str(span.get("span_kind") or "").upper()
        if not any(token in kind for token in ("CLIENT", "PRODUCER")):
            continue
        target = _peer_service(span)
        source = str(span.get("service_name") or "unknown")
        timestamp = span.get("start_time") or span.get("observed_at")
        if not target or target == source or timestamp is None:
            continue
        run_id = str(span["run_id"]) if span.get("run_id") else None
        key = (run_id, floor_time(_as_datetime(timestamp), bucket_seconds), source, target)
        acc = grouped[key]
        acc.durations.append(_span_duration_ms(span))
        acc.errors += int(_is_error_span(span))
        acc.protocol_counts[_protocol(span.get("attributes") or {})] += 1

    result: list[DependencyEdge] = []
    for (run_id, bucket, source, target), acc in sorted(grouped.items(), key=lambda item: item[0][1:]):
        protocol = max(acc.protocol_counts, key=acc.protocol_counts.get) if acc.protocol_counts else "unknown"
        result.append(
            DependencyEdge(
                bucket_start=bucket,
                source=source,
                target=target,
                run_id=run_id,
                protocol=protocol,
                request_count=len(acc.durations),
                error_count=acc.errors,
                latency_ms_p50=percentile(acc.durations, 0.50),
                latency_ms_p95=percentile(acc.durations, 0.95),
                latency_ms_p99=percentile(acc.durations, 0.99),
                attributes={"observed_from": "trace_parentage_or_peer"},
            )
        )
    return result


class DirectedDependencyGraph:
    """Compact adjacency representation with causal propagation helpers."""

    def __init__(self, edges: Iterable[DependencyEdge] = ()) -> None:
        self.edges: dict[tuple[str, str], DependencyEdge] = {}
        self.outgoing: dict[str, set[str]] = defaultdict(set)
        self.incoming: dict[str, set[str]] = defaultdict(set)
        for edge in edges:
            self.add_edge(edge)

    def add_edge(self, edge: DependencyEdge) -> None:
        key = (edge.source, edge.target)
        previous = self.edges.get(key)
        if previous is None:
            self.edges[key] = edge
        else:
            # A ranking window normally contains several fixed-time buckets for
            # the same dependency.  Keeping only the newest bucket lets one
            # later successful request erase the failing calls that opened the
            # incident.  Merge the observable counts across the window and
            # retain the worst latency percentile as conservative evidence.
            # The source data remains bucketed in PostgreSQL; this is only the
            # in-memory incident view used by the ranker.
            protocols = {
                value
                for value in (previous.protocol, edge.protocol)
                if value and value != "unknown"
            }
            protocol = next(iter(protocols)) if len(protocols) == 1 else (
                edge.protocol if edge.protocol != "unknown" else previous.protocol
            )
            self.edges[key] = DependencyEdge(
                bucket_start=max(previous.bucket_start, edge.bucket_start),
                source=edge.source,
                target=edge.target,
                run_id=edge.run_id or previous.run_id,
                protocol=protocol,
                request_count=previous.request_count + edge.request_count,
                error_count=previous.error_count + edge.error_count,
                latency_ms_p50=max(previous.latency_ms_p50, edge.latency_ms_p50),
                latency_ms_p95=max(previous.latency_ms_p95, edge.latency_ms_p95),
                latency_ms_p99=max(previous.latency_ms_p99, edge.latency_ms_p99),
                attributes={
                    "observed_from": "incident_window_aggregate",
                    "window_start": min(
                        previous.bucket_start, edge.bucket_start
                    ).isoformat(),
                    "window_end": max(
                        previous.bucket_start, edge.bucket_start
                    ).isoformat(),
                },
            )
        self.outgoing[edge.source].add(edge.target)
        self.incoming[edge.target].add(edge.source)
        # Ensure leaf/source nodes both appear in the node set.
        self.outgoing.setdefault(edge.target, set())
        self.incoming.setdefault(edge.source, set())

    @property
    def nodes(self) -> set[str]:
        return set(self.outgoing) | set(self.incoming)

    def descendants(self, source: str, *, max_depth: int | None = None) -> dict[str, int]:
        return self._walk(source, self.outgoing, max_depth=max_depth)

    def ancestors(self, target: str, *, max_depth: int | None = None) -> dict[str, int]:
        return self._walk(target, self.incoming, max_depth=max_depth)

    @staticmethod
    def _walk(start: str, adjacency: Mapping[str, set[str]], *, max_depth: int | None) -> dict[str, int]:
        distances: dict[str, int] = {}
        queue: deque[tuple[str, int]] = deque([(start, 0)])
        visited = {start}
        while queue:
            node, depth = queue.popleft()
            if max_depth is not None and depth >= max_depth:
                continue
            for neighbour in sorted(adjacency.get(node, ())):
                if neighbour in visited:
                    continue
                visited.add(neighbour)
                distances[neighbour] = depth + 1
                queue.append((neighbour, depth + 1))
        return distances

    def shortest_distance(self, source: str, target: str) -> int | None:
        if source == target:
            return 0
        return self.descendants(source).get(target)

    def propagation_score(
        self,
        candidate: str,
        affected: Iterable[str],
        *,
        anomaly_times: Mapping[str, datetime] | None = None,
        decay: float = 0.7,
    ) -> tuple[float, dict[str, Any]]:
        """Measure how well a candidate explains downstream affected entities."""

        affected_set = {entity for entity in affected if entity != candidate}
        if not affected_set:
            return 0.0, {"covered": [], "uncovered": [], "paths": {}}
        # Edges point caller -> dependency.  A broken dependency propagates back
        # toward its callers, so causal coverage follows reverse edges.
        distances = self.ancestors(candidate)
        covered = sorted(entity for entity in affected_set if entity in distances)
        paths = {entity: distances[entity] for entity in covered}
        weighted_coverage = sum(decay ** max(0, distances[entity] - 1) for entity in covered) / len(affected_set)
        temporal_factor = 1.0
        if anomaly_times and candidate in anomaly_times and covered:
            start = anomaly_times[candidate]
            ordering = [1.0 if start <= anomaly_times.get(entity, start) else 0.0 for entity in covered]
            temporal_factor = sum(ordering) / len(ordering)
        score = weighted_coverage * (0.5 + 0.5 * temporal_factor)
        return score, {
            "covered": covered,
            "uncovered": sorted(affected_set - set(covered)),
            "paths": paths,
            "temporal_factor": temporal_factor,
        }

    def subgraph_edges(self, nodes: Iterable[str]) -> list[DependencyEdge]:
        selected = set(nodes)
        return [edge for (source, target), edge in self.edges.items() if source in selected and target in selected]
