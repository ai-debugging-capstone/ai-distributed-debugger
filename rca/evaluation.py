"""Ranking, detection and calibration metrics with no optional dependencies."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


@dataclass(frozen=True)
class RankingExample:
    relevant: frozenset[str]
    ranked: tuple[str, ...]
    probabilities: tuple[float, ...] = ()


def hit_at_k(examples: Iterable[RankingExample], k: int) -> float:
    if k <= 0:
        raise ValueError("k must be positive")
    items = list(examples)
    if not items:
        return 0.0
    return sum(bool(example.relevant & set(example.ranked[:k])) for example in items) / len(items)


def reciprocal_rank(example: RankingExample) -> float:
    for index, candidate in enumerate(example.ranked, start=1):
        if candidate in example.relevant:
            return 1.0 / index
    return 0.0


def mean_reciprocal_rank(examples: Iterable[RankingExample]) -> float:
    items = list(examples)
    return sum(reciprocal_rank(item) for item in items) / len(items) if items else 0.0


def ndcg_at_k(example: RankingExample, k: int) -> float:
    if k <= 0:
        raise ValueError("k must be positive")
    dcg = sum(
        (1.0 / math.log2(index + 2)) if candidate in example.relevant else 0.0
        for index, candidate in enumerate(example.ranked[:k])
    )
    ideal_hits = min(k, len(example.relevant))
    ideal = sum(1.0 / math.log2(index + 2) for index in range(ideal_hits))
    return dcg / ideal if ideal else 0.0


def mean_ndcg_at_k(examples: Iterable[RankingExample], k: int) -> float:
    items = list(examples)
    return sum(ndcg_at_k(item, k) for item in items) / len(items) if items else 0.0


def average_precision_at_k(example: RankingExample, k: int) -> float:
    hits = 0
    total = 0.0
    for index, candidate in enumerate(example.ranked[:k], start=1):
        if candidate in example.relevant:
            hits += 1
            total += hits / index
    denominator = min(k, len(example.relevant))
    return total / denominator if denominator else 0.0


def brier_score(probabilities: Sequence[float], outcomes: Sequence[bool | int]) -> float:
    if len(probabilities) != len(outcomes):
        raise ValueError("probabilities and outcomes must have equal length")
    if not probabilities:
        return 0.0
    if any(not 0 <= value <= 1 for value in probabilities):
        raise ValueError("probabilities must lie in [0, 1]")
    return sum((probability - int(bool(outcome))) ** 2 for probability, outcome in zip(probabilities, outcomes)) / len(
        probabilities
    )


def expected_calibration_error(
    probabilities: Sequence[float], outcomes: Sequence[bool | int], *, bins: int = 10
) -> float:
    if len(probabilities) != len(outcomes):
        raise ValueError("probabilities and outcomes must have equal length")
    if bins <= 0:
        raise ValueError("bins must be positive")
    if not probabilities:
        return 0.0
    if any(not 0 <= value <= 1 for value in probabilities):
        raise ValueError("probabilities must lie in [0, 1]")
    total = len(probabilities)
    error = 0.0
    for bucket in range(bins):
        lower, upper = bucket / bins, (bucket + 1) / bins
        members = [
            index
            for index, probability in enumerate(probabilities)
            if lower <= probability < upper or (bucket == bins - 1 and probability == 1.0)
        ]
        if not members:
            continue
        confidence = sum(probabilities[index] for index in members) / len(members)
        accuracy = sum(int(bool(outcomes[index])) for index in members) / len(members)
        error += len(members) / total * abs(accuracy - confidence)
    return error


def risk_coverage(
    probabilities: Sequence[float], outcomes: Sequence[bool | int], thresholds: Sequence[float]
) -> list[Mapping[str, float]]:
    """Accuracy/error at confidence thresholds, exposing abstention trade-offs."""

    if len(probabilities) != len(outcomes):
        raise ValueError("probabilities and outcomes must have equal length")
    total = len(probabilities)
    result: list[Mapping[str, float]] = []
    for threshold in thresholds:
        selected = [index for index, value in enumerate(probabilities) if value >= threshold]
        accuracy = (
            sum(int(bool(outcomes[index])) for index in selected) / len(selected) if selected else 0.0
        )
        result.append(
            {
                "threshold": float(threshold),
                "coverage": len(selected) / total if total else 0.0,
                "risk": 1.0 - accuracy if selected else 0.0,
            }
        )
    return result


def evaluate_rankings(examples: Sequence[RankingExample]) -> dict[str, float]:
    return {
        "hit_at_1": hit_at_k(examples, 1),
        "hit_at_3": hit_at_k(examples, 3),
        "hit_at_5": hit_at_k(examples, 5),
        "mrr": mean_reciprocal_rank(examples),
        "ndcg_at_3": mean_ndcg_at_k(examples, 3),
        "ndcg_at_5": mean_ndcg_at_k(examples, 5),
        "map_at_5": sum(average_precision_at_k(item, 5) for item in examples) / len(examples) if examples else 0.0,
    }
