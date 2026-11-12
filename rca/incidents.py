"""Stateful incident creation, correlation, and recovery detection."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import groupby

from .domain import AnomalyResult, Incident
from .entities import DEFAULT_ROLES, EntityRoles


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

    Only application entities participate (see :mod:`rca.entities`). Once an
    incident is open, an entity outside its affected set must itself show a
    sustained anomaly before it joins or keeps the incident alive; otherwise
    unrelated single-bucket blips across many services grow the incident to
    the whole system and prevent it from ever recovering.

    A bucket in which a request-serving entity received no requests at all is
    *no evidence* either way. A low-QPS service such as the Demo's payment sees
    a failed charge, then often an empty bucket, then the next failure; such an
    idle bucket neither breaks the anomaly streak nor counts toward recovery,
    for at most ``max_idle_hold`` consecutive idle buckets.

    A bucket with no telemetry at all for an entity is an idle bucket too: a
    request-driven streak is held across it within the same budget, and any
    other streak ends. A longer silence is a telemetry gap, not evidence, so
    anomalies on either side of it are never consecutive. Likewise a run-wide
    silence of more than ``max_idle_hold`` buckets restarts the recovery count,
    while a shorter one only pauses it.
    """

    def __init__(
        self,
        *,
        anomaly_threshold: float = 3.5,
        min_consecutive: int = 2,
        recovery_consecutive: int = 3,
        max_idle_hold: int = 6,
        roles: EntityRoles = DEFAULT_ROLES,
        bucket_seconds: int = 10,
    ) -> None:
        if min_consecutive < 1 or recovery_consecutive < 1:
            raise ValueError("consecutive bucket counts must be positive")
        if max_idle_hold < 0:
            raise ValueError("max_idle_hold must be non-negative")
        if bucket_seconds < 1:
            raise ValueError("bucket_seconds must be positive")
        self.max_idle_hold = max_idle_hold
        self.roles = roles
        self.bucket_interval = timedelta(seconds=bucket_seconds)
        self.anomaly_threshold = anomaly_threshold
        self.min_consecutive = min_consecutive
        self.recovery_consecutive = recovery_consecutive
        self._streak: dict[tuple[str | None, str], int] = defaultdict(int)
        self._first_abnormal: dict[tuple[str | None, str], datetime] = {}
        self._active: dict[str | None, Incident] = {}
        self._recovery: dict[str | None, int] = defaultdict(int)
        self._idle: dict[tuple[str | None, str], int] = defaultdict(int)
        # Whether an entity's current anomaly streak came from request
        # traffic; only those streaks are held across idle buckets.
        self._request_streak: dict[tuple[str | None, str], bool] = {}
        self._last_observed: dict[tuple[str | None, str], datetime] = {}
        self._last_run_observed: dict[str | None, datetime] = {}

    @property
    def active_incidents(self) -> tuple[Incident, ...]:
        return tuple(self._active.values())

    def has_active_incident(self, run_id: str | None) -> bool:
        """Return whether ``run_id`` currently owns an open incident."""

        return run_id in self._active

    def restore(self, incident: Incident) -> None:
        """Re-adopt an incident persisted as open by a previous worker process.

        Without this, a worker restart orphans every open incident: nothing in
        memory owns it, so no amount of clean telemetry can ever resolve it.
        """

        self._active[incident.run_id] = incident
        self._recovery[incident.run_id] = 0

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
        self._last_run_observed.pop(run_id, None)
        for key in [key for key in self._streak if key[0] == run_id]:
            self._streak.pop(key, None)
            self._idle.pop(key, None)
            self._request_streak.pop(key, None)
            self._first_abnormal.pop(key, None)
            self._last_observed.pop(key, None)
        return IncidentEvent("resolved", active)

    def _missed_buckets(self, previous: datetime | None, timestamp: datetime) -> int | None:
        """Return how many buckets passed unobserved between two observations.

        ``None`` means they are not on one bucket grid (out of order, or a
        different bucket size), so nothing links them.
        """

        if previous is None:
            return 0
        elapsed = timestamp - previous
        if elapsed <= timedelta(0) or elapsed % self.bucket_interval:
            return None
        return elapsed // self.bucket_interval - 1

    def _end_streak(self, key: tuple[str | None, str]) -> None:
        self._streak[key] = 0
        self._idle[key] = 0
        self._request_streak.pop(key, None)
        self._first_abnormal.pop(key, None)

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

        missed = self._missed_buckets(self._last_run_observed.get(run_id), timestamp)
        if missed is None or missed > self.max_idle_hold:
            # Recovery evidence from before a long run-wide silence does not
            # carry over; a short silence only pauses the count.
            self._recovery[run_id] = 0
        self._last_run_observed[run_id] = timestamp

        anomalous: dict[str, AnomalyResult] = {}
        held: set[str] = set()
        for result in results:
            entity = result.bucket.entity_id
            if not self.roles.can_trigger_incident(entity):
                continue
            key = (run_id, entity)
            missed = self._missed_buckets(self._last_observed.get(key), timestamp)
            self._last_observed[key] = timestamp
            if missed != 0 and self._streak[key] > 0:
                # Buckets with no row at all for this entity are idle buckets:
                # held for a request-driven streak within the idle budget,
                # otherwise a gap that ends the streak.
                if (
                    missed is not None
                    and self._request_streak.get(key, False)
                    and self._idle[key] + missed <= self.max_idle_hold
                ):
                    self._idle[key] += missed
                else:
                    self._end_streak(key)
            is_anomaly = result.sufficient_history and result.score >= self.anomaly_threshold
            requests = result.bucket.value("request_count")
            if is_anomaly:
                if self._streak[key] == 0:
                    self._request_streak[key] = requests > 0
                self._streak[key] += 1
                self._idle[key] = 0
                self._first_abnormal.setdefault(key, timestamp)
                anomalous[entity] = result
            elif (
                requests == 0
                and self._streak[key] > 0
                and self._request_streak.get(key, False)
                and self._idle[key] < self.max_idle_hold
            ):
                self._idle[key] += 1
                held.add(entity)
            else:
                self._end_streak(key)

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
            relevant_anomalous = {
                entity: result
                for entity, result in anomalous.items()
                if entity in active.affected_entities
                or (allow_growth and entity in confirmed)
            }
            for entity, result in relevant_anomalous.items():
                if entity not in active.affected_entities:
                    active.affected_entities.append(entity)
                    active.affected_entities.sort()
                    onset = self._first_abnormal.get((run_id, entity), timestamp)
                    active.metadata.setdefault("anomaly_times", {})[entity] = onset.isoformat()
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
            elif held & set(active.affected_entities):
                # An affected low-QPS service merely saw no traffic: no
                # evidence of recovery yet.
                pass
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
    bucket_seconds: int = 10,
) -> list[Incident]:
    """Replay helper that returns all incidents, including still-open incidents."""

    detector = IncidentDetector(
        anomaly_threshold=anomaly_threshold,
        min_consecutive=min_consecutive,
        recovery_consecutive=recovery_consecutive,
        bucket_seconds=bucket_seconds,
    )
    ordered = sorted(results, key=lambda r: (r.bucket.run_id or "", r.bucket.bucket_start, r.bucket.entity_id))
    incidents: dict[str, Incident] = {}
    for (_, _), group in groupby(ordered, key=lambda r: (r.bucket.run_id, r.bucket.bucket_start)):
        for event in detector.update(list(group)):
            incidents[event.incident.id] = event.incident
    for incident in detector.active_incidents:
        incidents[incident.id] = incident
    return sorted(incidents.values(), key=lambda incident: incident.started_at)
