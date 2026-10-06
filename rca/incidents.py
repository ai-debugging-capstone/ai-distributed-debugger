"""Stateful incident creation, correlation, and recovery detection."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from itertools import groupby

from .domain import AnomalyResult, Incident


def severity_from_score(score: float) -> str:
    if score >= 10:
        return "critical"
    if score >= 4:
        return "warning"
    return "info"


@dataclass(frozen=True, slots=True)
class IncidentEvent:
    action: str  # opened, updated, resolved
    incident: Incident


class IncidentDetector:
    """Correlate sustained service anomalies into one incident per run.

    The detector does not infer root cause.  Its only job is to suppress transient
    spikes and establish a bounded incident window for the ranking engine.
    """

    def __init__(
        self,
        *,
        anomaly_threshold: float = 3.5,
        min_consecutive: int = 2,
        recovery_consecutive: int = 3,
    ) -> None:
        if min_consecutive < 1 or recovery_consecutive < 1:
            raise ValueError("consecutive bucket counts must be positive")
        self.anomaly_threshold = anomaly_threshold
        self.min_consecutive = min_consecutive
        self.recovery_consecutive = recovery_consecutive
        self._streak: dict[tuple[str | None, str], int] = defaultdict(int)
        self._first_abnormal: dict[tuple[str | None, str], datetime] = {}
        self._active: dict[str | None, Incident] = {}
        self._recovery: dict[str | None, int] = defaultdict(int)

    @property
    def active_incidents(self) -> tuple[Incident, ...]:
        return tuple(self._active.values())

    def has_active_incident(self, run_id: str | None) -> bool:
        """Return whether ``run_id`` currently owns an open incident."""

        return run_id in self._active

    def resolve_active(
        self, run_id: str | None, resolved_at: datetime
    ) -> IncidentEvent | None:
        """Close an in-memory incident when its experiment reaches a terminal state.

        Experiment completion can happen after the last telemetry bucket, so a
        detector cannot rely exclusively on receiving enough clean buckets to
        close an incident.  This explicit lifecycle hook prevents an otherwise
        healthy completed run from leaving a permanently open incident.
        """

        active = self._active.pop(run_id, None)
        if active is None:
            return None
        active.status = "resolved"
        active.ended_at = resolved_at
        self._recovery.pop(run_id, None)
        for key in [key for key in self._streak if key[0] == run_id]:
            self._streak.pop(key, None)
            self._first_abnormal.pop(key, None)
        return IncidentEvent("resolved", active)

    def update(
        self,
        results: Sequence[AnomalyResult],
        *,
        allow_open: bool = True,
        allow_growth: bool = True,
    ) -> list[IncidentEvent]:
        if not results:
            return []
        timestamps = {result.bucket.bucket_start for result in results}
        run_ids = {result.bucket.run_id for result in results}
        if len(timestamps) != 1 or len(run_ids) != 1:
            raise ValueError("update expects one timestamp and one run_id")
        timestamp = next(iter(timestamps))
        run_id = next(iter(run_ids))

        anomalous: dict[str, AnomalyResult] = {}
        for result in results:
            key = (run_id, result.bucket.entity_id)
            is_anomaly = result.sufficient_history and result.score >= self.anomaly_threshold
            if is_anomaly:
                self._streak[key] += 1
                self._first_abnormal.setdefault(key, timestamp)
                anomalous[result.bucket.entity_id] = result
            else:
                self._streak[key] = 0
                self._first_abnormal.pop(key, None)

        confirmed = {
            entity: result
            for entity, result in anomalous.items()
            if self._streak[(run_id, entity)] >= self.min_consecutive
        }
        active = self._active.get(run_id)
        events: list[IncidentEvent] = []
        if active is None and confirmed and allow_open:
            trigger, _trigger_result = max(
                confirmed.items(), key=lambda item: (item[1].onset_score, item[1].score, item[0])
            )
            starts = [self._first_abnormal[(run_id, entity)] for entity in confirmed]
            active = Incident(
                started_at=min(starts),
                detected_at=timestamp,
                trigger_entity=trigger,
                affected_entities=sorted(confirmed),
                max_anomaly=max(item.score for item in confirmed.values()),
                severity=severity_from_score(max(item.score for item in confirmed.values())),
                run_id=run_id,
                summary=f"Sustained anomaly affecting {', '.join(sorted(confirmed))}",
                metadata={
                    "anomaly_times": {
                        entity: self._first_abnormal[(run_id, entity)].isoformat() for entity in confirmed
                    },
                    "detector": "robust-baseline-v1",
                },
            )
            self._active[run_id] = active
            self._recovery[run_id] = 0
            events.append(IncidentEvent("opened", active))
        elif active is not None:
            changed = False
            relevant_anomalous = anomalous
            if not allow_growth:
                relevant_anomalous = {
                    entity: result
                    for entity, result in anomalous.items()
                    if entity in active.affected_entities
                }
            for entity, result in relevant_anomalous.items():
                if entity not in active.affected_entities:
                    active.affected_entities.append(entity)
                    active.affected_entities.sort()
                    active.metadata.setdefault("anomaly_times", {})[entity] = timestamp.isoformat()
                    changed = True
                if result.score > active.max_anomaly:
                    active.max_anomaly = result.score
                    active.severity = severity_from_score(result.score)
                    changed = True

            if relevant_anomalous:
                self._recovery[run_id] = 0
                # During active detection an ``updated`` event is also the
                # worker's signal to refresh rankings. Dependency evidence can
                # strengthen after the incident's entity set and maximum score
                # stop changing (notably when an unreachable server has no
                # server span), so a structurally unchanged anomalous bucket
                # must not leave the opening candidate snapshot stale.
                if changed or allow_growth:
                    events.append(IncidentEvent("updated", active))
            else:
                self._recovery[run_id] += 1
                if self._recovery[run_id] >= self.recovery_consecutive:
                    active.status = "resolved"
                    active.ended_at = timestamp
                    events.append(IncidentEvent("resolved", active))
                    del self._active[run_id]
                    del self._recovery[run_id]
        return events


def detect_incidents(
    results: Iterable[AnomalyResult],
    *,
    anomaly_threshold: float = 3.5,
    min_consecutive: int = 2,
    recovery_consecutive: int = 3,
) -> list[Incident]:
    """Replay helper that returns all incidents, including still-open incidents."""

    detector = IncidentDetector(
        anomaly_threshold=anomaly_threshold,
        min_consecutive=min_consecutive,
        recovery_consecutive=recovery_consecutive,
    )
    ordered = sorted(results, key=lambda r: (r.bucket.run_id or "", r.bucket.bucket_start, r.bucket.entity_id))
    incidents: dict[str, Incident] = {}
    for (_, _), group in groupby(ordered, key=lambda r: (r.bucket.run_id, r.bucket.bucket_start)):
        for event in detector.update(list(group)):
            incidents[event.incident.id] = event.incident
    for incident in detector.active_incidents:
        incidents[incident.id] = incident
    return sorted(incidents.values(), key=lambda incident: incident.started_at)
