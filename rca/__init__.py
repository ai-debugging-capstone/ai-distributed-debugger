"""Dependency-aware, non-LLM root-cause analysis engine.

The package deliberately keeps the analytical core free of framework and database
dependencies.  Optional integrations (PostgreSQL, scikit-learn and PyTorch) live
behind small adapters so the deterministic debugger remains runnable everywhere.
"""

from .domain import (
    AnomalyResult,
    CandidateRanking,
    DependencyEdge,
    EvidenceItem,
    FeatureBucket,
    Incident,
)

__all__ = [
    "AnomalyResult",
    "CandidateRanking",
    "DependencyEdge",
    "EvidenceItem",
    "FeatureBucket",
    "Incident",
]

__version__ = "0.1.0"
