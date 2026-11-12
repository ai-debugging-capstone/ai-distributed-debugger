"""Operational roles for telemetry entities.

Not every ``service.name`` in the telemetry stream is a component that can be
the root cause of an application incident:

* **infrastructure** entities are the observability and host plumbing (the
  Demo's own collector, the Docker host's metrics). Their telemetry is stored
  and scored for the platform-health view, but they never open, join, or keep
  an application incident alive and are never ranked as suspects.
* **workload** entities generate traffic (the Demo's Locust load generator).
  They do not open incidents either — client-side latency and Locust errors are
  symptoms — but they are ranked as the *external workload* candidate whose
  influence propagates downstream to the services they call.

Defaults match the pinned OpenTelemetry Demo 3.1.0 topology and can be
overridden with comma-separated ``RCA_INFRASTRUCTURE_ENTITIES`` and
``RCA_WORKLOAD_ENTITIES`` environment variables.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

DEFAULT_INFRASTRUCTURE_ENTITIES = frozenset(
    {
        "otel-collector",
        "otelcol-contrib",
        "rca-otel-gateway",
        "docker-desktop",
        "unknown",
        "unknown_service",
    }
)
DEFAULT_WORKLOAD_ENTITIES = frozenset({"load-generator"})


def _parse(value: str | None, default: frozenset[str]) -> frozenset[str]:
    if value is None:
        return default
    return frozenset(item.strip() for item in value.split(",") if item.strip())


@dataclass(frozen=True, slots=True)
class EntityRoles:
    infrastructure: frozenset[str] = DEFAULT_INFRASTRUCTURE_ENTITIES
    workload: frozenset[str] = DEFAULT_WORKLOAD_ENTITIES

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> EntityRoles:
        environ = os.environ if environ is None else environ
        return cls(
            infrastructure=_parse(
                environ.get("RCA_INFRASTRUCTURE_ENTITIES"),
                DEFAULT_INFRASTRUCTURE_ENTITIES,
            ),
            workload=_parse(environ.get("RCA_WORKLOAD_ENTITIES"), DEFAULT_WORKLOAD_ENTITIES),
        )

    def is_infrastructure(self, entity_id: str) -> bool:
        return entity_id in self.infrastructure

    def is_workload(self, entity_id: str) -> bool:
        return entity_id in self.workload

    def can_trigger_incident(self, entity_id: str) -> bool:
        """Only application components may open or sustain an incident."""

        return not (self.is_infrastructure(entity_id) or self.is_workload(entity_id))

    def can_be_candidate(self, entity_id: str) -> bool:
        return not self.is_infrastructure(entity_id)

    def application_entities(self, entities: Iterable[str]) -> list[str]:
        return [entity for entity in entities if self.can_trigger_incident(entity)]


DEFAULT_ROLES = EntityRoles()
