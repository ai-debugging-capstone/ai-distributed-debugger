"""Safe online integration for promoted supervised candidate rankers."""

from __future__ import annotations

import hashlib
import logging
import math
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Sequence

from .candidate_features import candidate_feature_map, candidate_key, is_online_safe_feature
from .domain import AnomalyResult, CandidateRanking, Incident
from .graph import DirectedDependencyGraph
from .models import ChampionRanker
from .ranking import HeuristicRootCauseRanker, RankerDecision


LOGGER = logging.getLogger("rca.online_ranking")
_MAX_ARTIFACT_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class LoadedChampionArtifact:
    ranker: ChampionRanker
    path: Path
    model_version: str
    sha256: str


def load_champion_artifact(
    path: str | Path | None,
    *,
    logger: logging.Logger = LOGGER,
) -> LoadedChampionArtifact | None:
    """Load and validate a configured artifact, returning ``None`` on failure.

    The worker is an availability-critical debugging component, so a missing,
    incompatible, or corrupt learned model never prevents the transparent
    heuristic baseline from starting.
    """

    if not path:
        logger.info("RCA_MODEL_PATH is not configured; using heuristic-v1")
        return None
    target = Path(path)
    try:
        if not target.is_file():
            raise FileNotFoundError(f"artifact is not a regular file: {target}")
        size = target.stat().st_size
        if size <= 0 or size > _MAX_ARTIFACT_BYTES:
            raise ValueError(f"artifact size {size} bytes is outside the accepted range")
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        model = ChampionRanker.load(target)
        if not model.schema:
            raise ValueError("artifact has an empty feature schema")
        unsafe = sorted(name for name in model.schema if not is_online_safe_feature(name))
        if unsafe:
            raise ValueError(
                "artifact includes offline-only or unknown features: " + ", ".join(unsafe[:5])
            )
        probe = {"features": {name: 0.0 for name in model.schema}}
        scores = model.predict_scores([probe])
        if len(scores) != 1 or not math.isfinite(scores[0]) or not 0.0 <= scores[0] <= 1.0:
            raise ValueError("artifact failed its finite probability self-check")
        version = f"champion-{model.backend}-sha256:{digest[:12]}"
    except Exception as exc:
        logger.warning(
            "Could not load promoted RCA model from %s (%s: %s); using heuristic-v1",
            target,
            type(exc).__name__,
            exc,
        )
        return None
    logger.info(
        "Loaded promoted RCA model %s from %s with %d features",
        version,
        target,
        len(model.schema),
    )
    return LoadedChampionArtifact(model, target, version, digest)


def _evidence_rows(ranking: CandidateRanking) -> list[dict[str, Any]]:
    return [
        {
            "evidence_type": evidence.evidence_type,
            "signal_name": evidence.signal_name,
            "value": evidence.value,
            "baseline": evidence.baseline,
            "contribution": evidence.contribution,
            "details": evidence.details,
        }
        for evidence in ranking.evidence
    ]


def online_candidate_record(incident: Incident, ranking: CandidateRanking) -> dict[str, Any]:
    """Build one inference row using only data available to the live worker."""

    candidate = {
        "entity_type": ranking.entity_type,
        "entity_id": ranking.entity_id,
        "rank": ranking.rank,
        "score": ranking.score,
        "probability": ranking.probability,
        "confidence": ranking.confidence,
    }
    return {
        "schema_version": 1,
        "run_id": incident.run_id or "",
        "incident_id": incident.id,
        "candidate_id": candidate_key(ranking.entity_type, ranking.entity_id),
        "candidate_type": ranking.entity_type,
        "candidate_entity_id": ranking.entity_id,
        "features": candidate_feature_map(candidate, _evidence_rows(ranking)),
    }


def _learned_confidence(score: float, margin: float, completeness: float) -> str:
    if score >= 0.70 and margin >= 0.15 and completeness >= 0.75:
        return "high"
    if score >= 0.45 and margin >= 0.06 and completeness >= 0.50:
        return "medium"
    return "low"


class ChampionRerankingRootCauseRanker:
    """Generate explainable candidates heuristically, then rerank them with ML."""

    def __init__(
        self,
        artifact: LoadedChampionArtifact,
        *,
        baseline: HeuristicRootCauseRanker | None = None,
        logger: logging.Logger = LOGGER,
    ) -> None:
        self.artifact = artifact
        self.baseline = baseline or HeuristicRootCauseRanker()
        self.logger = logger
        self._disabled = False

    def rank(
        self,
        incident: Incident,
        anomalies: Sequence[AnomalyResult],
        graph: DirectedDependencyGraph,
        *,
        top_k: int = 10,
    ) -> RankerDecision:
        baseline = self.baseline.rank(incident, anomalies, graph, top_k=top_k)
        if self._disabled:
            return baseline
        actionable = list(baseline.actionable_rankings)
        if not actionable:
            return baseline
        records = [online_candidate_record(incident, ranking) for ranking in actionable]
        try:
            scores = self.artifact.ranker.predict_scores(records)
            if len(scores) != len(actionable):
                raise ValueError(
                    f"model returned {len(scores)} scores for {len(actionable)} candidates"
                )
            learned = [float(score) for score in scores]
            if any(not math.isfinite(score) or not 0.0 <= score <= 1.0 for score in learned):
                raise ValueError("model returned a non-finite or out-of-range probability")
        except Exception as exc:
            self._disabled = True
            self.logger.warning(
                "Promoted RCA model %s failed live inference (%s: %s); "
                "disabling it and using heuristic-v1",
                self.artifact.model_version,
                type(exc).__name__,
                exc,
            )
            return baseline

        ordered = sorted(
            zip(actionable, learned, records),
            key=lambda item: (
                -item[1],
                item[0].rank,
                item[0].entity_type,
                item[0].entity_id,
            ),
        )
        top_score = ordered[0][1]
        margin = top_score - (ordered[1][1] if len(ordered) > 1 else 0.0)
        reranked: list[CandidateRanking] = []
        for rank, (candidate, score, record) in enumerate(ordered, start=1):
            reranked.append(
                replace(
                    candidate,
                    rank=rank,
                    score=score,
                    probability=score,
                    confidence=_learned_confidence(score, margin, baseline.data_completeness),
                    evidence_summary=(
                        f"learned_relevance={score:.3f}; heuristic_rank={candidate.rank}; "
                        f"{candidate.evidence_summary}"
                    ),
                    model_version=self.artifact.model_version,
                    online_features=dict(record["features"]),
                )
            )

        inconclusive = [
            candidate for candidate in baseline.rankings if candidate.entity_id == "inconclusive"
        ]
        for candidate in inconclusive:
            reranked.append(
                replace(
                    candidate,
                    rank=len(reranked) + 1,
                    model_version=self.artifact.model_version,
                )
            )
        return RankerDecision(
            tuple(reranked),
            baseline.abstained,
            baseline.abstention_reason,
            baseline.data_completeness,
        )
