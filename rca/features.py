"""Telemetry aggregation and robust, explainable anomaly scoring.

There is intentionally no database or ML dependency in this module.  It is used
identically by the live worker, replay evaluation, and unit tests.
"""

from __future__ import annotations

import math
import re
import statistics
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from .domain import AnomalyResult, FeatureBucket

STABLE_FEATURES = (
    "request_count",
    "error_count",
    "error_rate",
    "latency_ms_p50",
    "latency_ms_p95",
    "latency_ms_p99",
    "cpu_percent",
    "memory_bytes",
    "log_error_count",
)


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        # Accept seconds as well as OTLP nanoseconds.
        seconds = float(value)
        if seconds > 10_000_000_000:
            seconds /= 1_000_000_000
        parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"Unsupported timestamp: {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def floor_time(value: datetime, bucket_seconds: int) -> datetime:
    if bucket_seconds <= 0:
        raise ValueError("bucket_seconds must be positive")
    value = _as_datetime(value)
    epoch = int(value.timestamp())
    return datetime.fromtimestamp(epoch - epoch % bucket_seconds, tz=timezone.utc)


def percentile(values: Sequence[float], quantile: float) -> float:
    """Linear-interpolated percentile with no third-party dependency."""

    if not values:
        return 0.0
    if not 0 <= quantile <= 1:
        raise ValueError("quantile must be between zero and one")
    ordered = sorted(float(v) for v in values if math.isfinite(float(v)))
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _normalise_metric_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "unnamed"


def _is_error_span(span: Mapping[str, Any]) -> bool:
    status = str(span.get("status_code") or "").upper()
    if status in {"ERROR", "STATUS_CODE_ERROR", "2"}:
        return True
    attrs = span.get("attributes") or {}
    code = attrs.get("http.response.status_code", attrs.get("http.status_code"))
    try:
        return int(code) >= 500
    except (TypeError, ValueError):
        return False


def _span_duration_ms(span: Mapping[str, Any]) -> float:
    """Use stored duration or derive it from the strict normalized API fields."""

    if span.get("duration_ms") is not None:
        try:
            return max(0.0, float(span["duration_ms"]))
        except (TypeError, ValueError):
            return 0.0
    if span.get("start_time") is not None and span.get("end_time") is not None:
        return max(0.0, (_as_datetime(span["end_time"]) - _as_datetime(span["start_time"])).total_seconds() * 1000)
    return 0.0


def _is_error_log(log: Mapping[str, Any]) -> bool:
    try:
        if int(log.get("severity_number") or 0) >= 17:
            return True
    except (TypeError, ValueError):
        pass
    severity = str(log.get("severity_text") or "").upper()
    return any(token in severity for token in ("ERROR", "FATAL", "CRITICAL"))


@dataclass
class _Accumulator:
    spans: list[float] = field(default_factory=list)
    span_errors: int = 0
    logs: int = 0
    log_errors: int = 0
    metrics: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    signals: set[str] = field(default_factory=set)


def aggregate_feature_buckets(
    spans: Iterable[Mapping[str, Any]],
    metrics: Iterable[Mapping[str, Any]],
    logs: Iterable[Mapping[str, Any]],
    *,
    bucket_seconds: int = 10,
    expected_signals: Sequence[str] = ("span", "metric", "log"),
) -> list[FeatureBucket]:
    """Aggregate normalized raw records into deterministic per-service buckets."""

    grouped: dict[tuple[str | None, datetime, str], _Accumulator] = defaultdict(_Accumulator)

    for span in spans:
        service = str(span.get("service_name") or "unknown")
        timestamp = span.get("start_time") or span.get("observed_at")
        if timestamp is None:
            continue
        key = (str(span["run_id"]) if span.get("run_id") else None, floor_time(_as_datetime(timestamp), bucket_seconds), service)
        acc = grouped[key]
        duration = _span_duration_ms(span)
        if math.isfinite(duration):
            acc.spans.append(max(0.0, duration))
        acc.span_errors += int(_is_error_span(span))
        acc.signals.add("span")

    for metric in metrics:
        service = str(metric.get("service_name") or "unknown")
        timestamp = metric.get("time_unix") or metric.get("observed_at")
        if timestamp is None:
            continue
        value = metric.get("value")
        if value is None:
            value = metric.get("sum")
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(number):
            continue
        key = (str(metric["run_id"]) if metric.get("run_id") else None, floor_time(_as_datetime(timestamp), bucket_seconds), service)
        acc = grouped[key]
        acc.metrics[_normalise_metric_name(str(metric.get("metric_name") or "unnamed"))].append(number)
        acc.signals.add("metric")

    for log in logs:
        service = str(log.get("service_name") or "unknown")
        timestamp = log.get("time_unix") or log.get("observed_time") or log.get("observed_at")
        if timestamp is None:
            continue
        key = (str(log["run_id"]) if log.get("run_id") else None, floor_time(_as_datetime(timestamp), bucket_seconds), service)
        acc = grouped[key]
        acc.logs += 1
        acc.log_errors += int(_is_error_log(log))
        acc.signals.add("log")

    buckets: list[FeatureBucket] = []
    for (run_id, start, service), acc in sorted(grouped.items(), key=lambda item: (item[0][1], item[0][2])):
        count = len(acc.spans)
        features: dict[str, float] = {
            "request_count": float(count),
            "error_count": float(acc.span_errors),
            "error_rate": acc.span_errors / count if count else 0.0,
            "latency_ms_p50": percentile(acc.spans, 0.50),
            "latency_ms_p95": percentile(acc.spans, 0.95),
            "latency_ms_p99": percentile(acc.spans, 0.99),
            "cpu_percent": 0.0,
            "memory_bytes": 0.0,
            "log_error_count": float(acc.log_errors),
            "log_count": float(acc.logs),
        }
        for name, values in sorted(acc.metrics.items()):
            mean = statistics.fmean(values)
            features[f"metric.{name}.mean"] = mean
            features[f"metric.{name}.max"] = max(values)
            if "cpu" in name and ("util" in name or "percent" in name or "time" in name):
                features["cpu_percent"] = max(features["cpu_percent"], mean * 100 if 0 <= mean <= 1 else mean)
            if any(token in name for token in ("memory", "working_set", "heap")) and any(
                token in name for token in ("usage", "used", "bytes", "working_set", "heap")
            ):
                features["memory_bytes"] = max(features["memory_bytes"], mean)
            if "queue" in name and "lag" in name:
                features["queue_lag"] = mean
        missingness = {signal: float(signal not in acc.signals) for signal in expected_signals}
        buckets.append(
            FeatureBucket(
                bucket_start=start,
                bucket_end=start + timedelta(seconds=bucket_seconds),
                entity_type="service",
                entity_id=service,
                run_id=run_id,
                features=features,
                sample_count=count + acc.logs + sum(len(v) for v in acc.metrics.values()),
                missingness=missingness,
            )
        )
    return buckets


def median_absolute_deviation(values: Sequence[float], center: float | None = None) -> float:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if not clean:
        return 0.0
    middle = statistics.median(clean) if center is None else center
    return statistics.median(abs(value - middle) for value in clean)


def robust_z_score(value: float, history: Sequence[float], *, cap: float = 20.0) -> tuple[float, float]:
    """Return (signed robust-z, median) with safe behavior for flat histories."""

    clean = [float(v) for v in history if math.isfinite(float(v))]
    if not clean or not math.isfinite(float(value)):
        return 0.0, 0.0
    center = statistics.median(clean)
    mad = median_absolute_deviation(clean, center)
    if mad > 1e-12:
        # A very small non-zero MAD over a short rolling window can turn normal
        # process jitter into a huge z-score (memory gauges are a common case).
        # Keep the robust estimator, but require a modest relative noise floor.
        # Zero-centred failure signals such as error rate still use the absolute
        # floor below and therefore remain highly sensitive to first failures.
        scale = max(mad / 0.6744897501960817, abs(center) * 0.02, 1e-6)
    else:
        q75, q25 = percentile(clean, 0.75), percentile(clean, 0.25)
        iqr_scale = (q75 - q25) / 1.3489795003921634
        # Flat healthy series still needs to flag a meaningful departure.
        scale = max(iqr_scale, abs(center) * 0.05, 1e-6)
    score = (float(value) - center) / scale
    return max(-cap, min(cap, score)), center


DEFAULT_DIRECTIONS: dict[str, str] = {
    "error_count": "high",
    "error_rate": "high",
    "latency_ms_p50": "high",
    "latency_ms_p95": "high",
    "latency_ms_p99": "high",
    "cpu_percent": "high",
    "memory_bytes": "high",
    "log_error_count": "high",
    "queue_lag": "high",
    "request_count": "both",
}

# Incident detection deliberately uses a small, auditable feature contract.
# Raw ``metric.*`` series remain in every FeatureBucket for evidence and
# offline feature engineering, but many receiver metrics are cumulative
# counters (uptime, CPU time, network bytes). Scoring their absolute values as
# gauges creates a permanent anomaly simply because time moves forward.
# Resource and traffic features are down-weighted so a single noisy bucket does
# not outrank direct error/latency evidence; sustained severe saturation can
# still cross the incident threshold.
DEFAULT_FEATURE_WEIGHTS: dict[str, float] = {
    "request_count": 0.15,
    "error_count": 1.0,
    "error_rate": 1.5,
    "latency_ms_p50": 0.50,
    "latency_ms_p95": 1.0,
    "latency_ms_p99": 0.75,
    "cpu_percent": 0.20,
    "memory_bytes": 0.20,
    "log_error_count": 1.0,
    "queue_lag": 1.0,
}


def score_feature_bucket(
    current: FeatureBucket,
    history: Sequence[FeatureBucket],
    *,
    min_history: int = 6,
    directions: Mapping[str, str] | None = None,
    feature_weights: Mapping[str, float] | None = None,
    previous_score: float = 0.0,
) -> AnomalyResult:
    """Score one entity bucket against its own robust historical baseline."""

    compatible = [
        item
        for item in history
        if item.entity_type == current.entity_type
        and item.entity_id == current.entity_id
        and (current.run_id is None or item.run_id == current.run_id)
        and item.bucket_start < current.bucket_start
    ]
    if len(compatible) < min_history:
        return AnomalyResult(current, 0.0, 0.0, {}, {}, {}, False, ("insufficient baseline history",))

    direction_map = {**DEFAULT_DIRECTIONS, **(directions or {})}
    weights = {**DEFAULT_FEATURE_WEIGHTS, **(feature_weights or {})}
    feature_scores: dict[str, float] = {}
    baselines: dict[str, float] = {}
    weighted: dict[str, float] = {}
    for name, raw_value in current.features.items():
        # Unknown features are retained in the bucket but are opt-in for
        # detection through either a direction or an explicit weight.
        if name not in direction_map and name not in weights:
            continue
        historical = [item.value(name) for item in compatible if name in item.features]
        if len(historical) < min_history:
            continue
        signed, baseline = robust_z_score(float(raw_value), historical)
        direction = direction_map.get(name, "both")
        if direction == "high":
            severity = max(0.0, signed)
        elif direction == "low":
            severity = max(0.0, -signed)
        else:
            severity = abs(signed)
        feature_scores[name] = severity
        baselines[name] = baseline
        weighted[name] = severity * float(weights.get(name, 1.0))

    ordered = sorted(weighted.items(), key=lambda item: (-item[1], item[0]))
    top = [value for _, value in ordered[:3]]
    score = 0.0 if not top else 0.65 * max(top) + 0.35 * statistics.fmean(top)
    contributions = {name: (value / sum(top) if sum(top) else 0.0) for name, value in ordered[:3]}
    explanation = tuple(
        f"{name}={current.value(name):.4g} vs baseline={baselines[name]:.4g} ({feature_scores[name]:.2f} robust z)"
        for name, _ in ordered[:3]
    )
    return AnomalyResult(
        bucket=current,
        score=score,
        onset_score=max(0.0, score - previous_score),
        feature_scores=feature_scores,
        baselines=baselines,
        contributions=contributions,
        sufficient_history=True,
        explanation=explanation,
    )


class StreamingAnomalyScorer:
    """Bounded per-entity rolling history for live and replay scoring."""

    def __init__(self, *, lookback: int = 30, min_history: int = 6) -> None:
        if lookback < min_history:
            raise ValueError("lookback must be at least min_history")
        self.lookback = lookback
        self.min_history = min_history
        self._history: dict[tuple[str | None, str, str], list[FeatureBucket]] = defaultdict(list)
        self._previous_score: dict[tuple[str | None, str, str], float] = defaultdict(float)

    def score(self, bucket: FeatureBucket) -> AnomalyResult:
        key = (bucket.run_id, bucket.entity_type, bucket.entity_id)
        result = score_feature_bucket(
            bucket,
            self._history[key],
            min_history=self.min_history,
            previous_score=self._previous_score[key],
        )
        self._history[key].append(bucket)
        del self._history[key][:-self.lookback]
        self._previous_score[key] = result.score
        return result
