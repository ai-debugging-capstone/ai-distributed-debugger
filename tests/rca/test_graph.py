from __future__ import annotations

import unittest
from datetime import datetime, timezone

from rca.domain import DependencyEdge
from rca.graph import DirectedDependencyGraph, build_dependency_edges


class GraphTests(unittest.TestCase):
    def test_builds_directed_cross_service_parent_edge(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        spans = [
            {
                "trace_id": "t",
                "span_id": "a",
                "service_name": "frontend",
                "start_time": now,
                "duration_ms": 30,
                "status_code": "OK",
            },
            {
                "trace_id": "t",
                "span_id": "b",
                "parent_span_id": "a",
                "service_name": "checkout",
                "start_time": now,
                "duration_ms": 20,
                "status_code": "ERROR",
                "attributes": {"rpc.system": "grpc"},
            },
        ]
        edges = build_dependency_edges(spans)
        self.assertEqual(len(edges), 1)
        self.assertEqual((edges[0].source, edges[0].target), ("frontend", "checkout"))
        self.assertEqual(edges[0].protocol, "grpc")
        self.assertEqual(edges[0].error_rate, 1)

    def test_peer_service_fills_unsampled_server_gap(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        edges = build_dependency_edges(
            [
                {
                    "trace_id": "t",
                    "span_id": "a",
                    "service_name": "catalog",
                    "span_kind": "CLIENT",
                    "start_time": now,
                    "duration_ms": 7,
                    "attributes": {"peer.service": "postgres", "db.system": "postgresql"},
                }
            ]
        )
        self.assertEqual(edges[0].edge_id, "catalog->postgres")

    def test_qualified_rpc_method_fills_unreachable_server_gap(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        edges = build_dependency_edges(
            [
                {
                    "trace_id": "t",
                    "span_id": "client",
                    "service_name": "checkout",
                    "span_kind": "CLIENT",
                    "start_time": now,
                    "duration_ms": 0.04,
                    "status_code": "ERROR",
                    "attributes": {
                        "rpc.method": "oteldemo.PaymentService/Charge",
                        "rpc.system.name": "grpc",
                        "rpc.response.status_code": "UNAVAILABLE",
                    },
                }
            ]
        )

        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0].edge_id, "checkout->payment")
        self.assertEqual(edges[0].protocol, "grpc")
        self.assertEqual(edges[0].request_count, 1)
        self.assertEqual(edges[0].error_count, 1)

    def test_propagation_respects_direction_and_distance(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        graph = DirectedDependencyGraph(
            [
                DependencyEdge(now, "frontend", "checkout"),
                DependencyEdge(now, "checkout", "catalog"),
                DependencyEdge(now, "catalog", "db"),
            ]
        )
        score, evidence = graph.propagation_score("db", ["catalog", "checkout", "frontend"])
        self.assertGreater(score, 0.6)
        self.assertEqual(evidence["paths"]["frontend"], 3)
        reverse, _ = graph.propagation_score("frontend", ["db", "catalog"])
        self.assertEqual(reverse, 0)

    def test_repeated_edge_buckets_preserve_failure_evidence_across_window(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        graph = DirectedDependencyGraph(
            [
                DependencyEdge(
                    now,
                    "checkout",
                    "payment",
                    request_count=4,
                    error_count=4,
                    latency_ms_p95=120,
                ),
                DependencyEdge(
                    now.replace(second=10),
                    "checkout",
                    "payment",
                    request_count=6,
                    error_count=0,
                    latency_ms_p95=20,
                ),
            ]
        )
        edge = graph.edges[("checkout", "payment")]
        self.assertEqual(edge.request_count, 10)
        self.assertEqual(edge.error_count, 4)
        self.assertEqual(edge.error_rate, 0.4)
        self.assertEqual(edge.latency_ms_p95, 120)
        self.assertEqual(edge.attributes["observed_from"], "incident_window_aggregate")


if __name__ == "__main__":
    unittest.main()
