from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from rca.domain import FeatureBucket
from rca.features import (
    StreamingAnomalyScorer,
    aggregate_feature_buckets,
    percentile,
    robust_z_score,
    score_feature_bucket,
)

UTC = timezone.utc


class FeatureTests(unittest.TestCase):
    def test_percentile_interpolates(self) -> None:
        self.assertEqual(percentile([0, 10], 0.5), 5)
        self.assertEqual(percentile([], 0.95), 0)

    def test_aggregation_correlates_all_signal_types(self) -> None:
        when = datetime(2026, 1, 1, tzinfo=UTC)
        spans = [
            {"service_name": "checkout", "start_time": when, "duration_ms": 10, "status_code": "OK"},
            {
                "service_name": "checkout",
                "start_time": when + timedelta(seconds=1),
                "duration_ms": 30,
                "status_code": "ERROR",
            },
        ]
        metrics = [
            {
                "service_name": "checkout",
                "time_unix": when,
                "metric_name": "process.cpu.utilization",
                "value": 0.5,
            }
        ]
        logs = [
            {"service_name": "checkout", "time_unix": when, "severity_number": 17, "severity_text": "ERROR"}
        ]
        result = aggregate_feature_buckets(spans, metrics, logs)
        self.assertEqual(len(result), 1)
        features = result[0].features
        self.assertEqual(features["request_count"], 2)
        self.assertEqual(features["error_rate"], 0.5)
        self.assertEqual(features["latency_ms_p50"], 20)
        self.assertEqual(features["cpu_percent"], 50)
        self.assertEqual(features["log_error_count"], 1)
        self.assertTrue(all(value == 0 for value in result[0].missingness.values()))

    def test_robust_score_flags_flat_baseline_departure(self) -> None:
        score, baseline = robust_z_score(100, [10] * 10)
        self.assertEqual(baseline, 10)
        self.assertGreater(score, 10)

    def test_streaming_scorer_waits_for_history_then_flags_latency(self) -> None:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        scorer = StreamingAnomalyScorer(lookback=10, min_history=4)
        for index in range(4):
            result = scorer.score(self._bucket(start + timedelta(seconds=index * 10), 10 + index % 2))
            self.assertFalse(result.sufficient_history)
        result = scorer.score(self._bucket(start + timedelta(seconds=40), 500))
        self.assertTrue(result.sufficient_history)
        self.assertGreater(result.score, 3.5)
        self.assertIn("latency_ms_p95", result.feature_scores)
        self.assertGreater(result.onset_score, 0)

    def test_cumulative_raw_metric_does_not_trigger_incident_score(self) -> None:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        history = [
            FeatureBucket(
                start + timedelta(seconds=index * 10),
                start + timedelta(seconds=(index + 1) * 10),
                "service",
                "checkout",
                "run-1",
                {
                    "request_count": 10,
                    "error_rate": 0,
                    "metric.container_uptime.mean": float(index * 10),
                },
            )
            for index in range(10)
        ]
        current = FeatureBucket(
            start + timedelta(seconds=100),
            start + timedelta(seconds=110),
            "service",
            "checkout",
            "run-1",
            {
                "request_count": 10,
                "error_rate": 0,
                "metric.container_uptime.mean": 100.0,
            },
        )

        result = score_feature_bucket(current, history, min_history=6)

        self.assertEqual(result.score, 0)
        self.assertNotIn("metric.container_uptime.mean", result.feature_scores)

    def test_direct_error_signal_outranks_resource_jitter(self) -> None:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        history = [
            FeatureBucket(
                start + timedelta(seconds=index * 10),
                start + timedelta(seconds=(index + 1) * 10),
                "service",
                "payment",
                "run-1",
                {"error_count": 0, "error_rate": 0, "cpu_percent": 10},
            )
            for index in range(10)
        ]
        current = FeatureBucket(
            start + timedelta(seconds=100),
            start + timedelta(seconds=110),
            "service",
            "payment",
            "run-1",
            {"error_count": 4, "error_rate": 1, "cpu_percent": 30},
        )

        result = score_feature_bucket(current, history, min_history=6)

        self.assertGreater(result.score, 10)
        ranked = sorted(
            result.contributions, key=result.contributions.get, reverse=True
        )
        self.assertEqual(ranked[:2], ["error_rate", "error_count"])

    @staticmethod
    def _bucket(when: datetime, latency: float) -> FeatureBucket:
        return FeatureBucket(
            when,
            when + timedelta(seconds=10),
            "service",
            "checkout",
            "run-1",
            {"request_count": 10, "error_rate": 0, "latency_ms_p95": latency},
            missingness={"span": 0, "metric": 0, "log": 0},
        )


if __name__ == "__main__":
    unittest.main()
