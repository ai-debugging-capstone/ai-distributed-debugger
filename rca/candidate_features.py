"""Leakage-safe features shared by online inference and offline export.

The learned candidate ranker is deliberately downstream of the transparent
heuristic ranker.  Its inputs are limited to fields and evidence that are
available at incident-analysis time.  Experiment labels, injected fault names,
and run metadata are never accepted by this module.
"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from typing import Any, Iterable, Mapping


def canonical_entity(value: Any) -> str:
    return str(value or "").strip().casefold().replace("→", "->")


def canonical_type(value: Any) -> str:
    normalized = str(value or "").strip().casefold().replace("-", "_")
    if normalized in {"edge", "dependency", "dependency_edge"}:
        return "dependency_edge"
    return normalized


def candidate_key(entity_type: Any, entity_id: Any) -> str:
    """Return an ontology-qualified candidate identity."""

    return f"{canonical_type(entity_type)}:{canonical_entity(entity_id)}"


def feature_key(value: Any) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value or "unknown").casefold()).strip("_")
    return normalized or "unknown"


def number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def evidence_feature_map(rows: Iterable[Mapping[str, Any]]) -> dict[str, float]:
    """Aggregate evidence without exposing entity names or ground truth."""

    features: dict[str, float] = defaultdict(float)
    contributions: list[float] = []
    absolute_deviations: list[float] = []
    for row in rows:
        contribution = number(row.get("contribution"))
        contributions.append(contribution)
        evidence_type = feature_key(row.get("evidence_type"))
        signal_name = feature_key(row.get("signal_name"))
        features[f"evidence_type_{evidence_type}_count"] += 1.0
        features[f"evidence_type_{evidence_type}_contribution"] += contribution
        features[f"signal_{signal_name}_contribution"] += contribution

        value = row.get("value")
        baseline = row.get("baseline")
        if value is not None and baseline is not None:
            absolute_deviations.append(abs(number(value) - number(baseline)))
        details = row.get("details") or {}
        if isinstance(details, Mapping) and details.get("robust_z") is not None:
            robust_z = abs(number(details["robust_z"]))
            features["evidence_max_abs_robust_z"] = max(
                features["evidence_max_abs_robust_z"], robust_z
            )

    features["evidence_count"] = float(len(contributions))
    features["evidence_contribution_sum"] = sum(contributions)
    features["evidence_contribution_max"] = max(contributions, default=0.0)
    features["evidence_abs_deviation_max"] = max(absolute_deviations, default=0.0)
    return dict(features)


def candidate_feature_map(
    candidate: Mapping[str, Any],
    evidence: Iterable[Mapping[str, Any]],
    *,
    stored_features: Mapping[str, Any] | None = None,
) -> dict[str, float]:
    """Build or recover the exact feature vector used for live inference.

    New candidate rows persist the pre-rerank vector in ``online_features``.
    Exporters should pass that mapping through ``stored_features``.  Empty or
    absent snapshots intentionally fall back to reconstruction for legacy rows.
    """

    if stored_features:
        unsafe = sorted(name for name in stored_features if not is_online_safe_feature(name))
        if unsafe:
            raise ValueError(
                "stored online features include unsupported keys: " + ", ".join(unsafe[:5])
            )
        return {str(name): number(value) for name, value in stored_features.items()}

    confidence = str(candidate.get("confidence") or "").casefold()
    entity_type = canonical_type(candidate.get("entity_type"))
    return {
        "baseline_score": number(candidate.get("score")),
        "baseline_probability": number(candidate.get("probability")),
        "baseline_inverse_rank": 1.0 / max(1.0, number(candidate.get("rank"), 1.0)),
        "candidate_is_service": float(entity_type == "service"),
        "candidate_is_dependency_edge": float(entity_type == "dependency_edge"),
        "candidate_is_system": float(entity_type == "system"),
        "confidence_low": float(confidence == "low"),
        "confidence_medium": float(confidence == "medium"),
        "confidence_high": float(confidence == "high"),
        **evidence_feature_map(evidence),
    }


_STATIC_FEATURES = frozenset(
    {
        "baseline_score",
        "baseline_probability",
        "baseline_inverse_rank",
        "candidate_is_service",
        "candidate_is_dependency_edge",
        "candidate_is_system",
        "confidence_low",
        "confidence_medium",
        "confidence_high",
        "evidence_count",
        "evidence_contribution_sum",
        "evidence_contribution_max",
        "evidence_abs_deviation_max",
        "evidence_max_abs_robust_z",
    }
)
_DYNAMIC_PREFIXES = (
    "evidence_type_",
    "signal_",
)


def is_online_safe_feature(name: Any) -> bool:
    """Reject artifacts trained with labels or other offline-only columns."""

    normalized = str(name)
    return normalized in _STATIC_FEATURES or normalized.startswith(_DYNAMIC_PREFIXES)
