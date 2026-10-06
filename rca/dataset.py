"""Leakage-safe dataset construction and experiment-run splitting."""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence


@dataclass(frozen=True)
class DatasetSplit:
    train: tuple[Mapping[str, Any], ...]
    validation: tuple[Mapping[str, Any], ...]
    test: tuple[Mapping[str, Any], ...]
    train_runs: frozenset[str]
    validation_runs: frozenset[str]
    test_runs: frozenset[str]

    def assert_disjoint(self) -> None:
        if self.train_runs & self.validation_runs:
            raise AssertionError("train and validation contain the same experiment run")
        if self.train_runs & self.test_runs:
            raise AssertionError("train and test contain the same experiment run")
        if self.validation_runs & self.test_runs:
            raise AssertionError("validation and test contain the same experiment run")


def _allocation(size: int, ratios: tuple[float, float, float]) -> tuple[int, int, int]:
    if size <= 0:
        return 0, 0, 0
    raw = [size * ratio for ratio in ratios]
    counts = [int(value) for value in raw]
    for index in sorted(range(3), key=lambda i: raw[i] - counts[i], reverse=True)[: size - sum(counts)]:
        counts[index] += 1
    # If a stratum can cover every split, preserve representation in all of them.
    if size >= 3:
        for target in range(3):
            if ratios[target] > 0 and counts[target] == 0:
                donor = max(range(3), key=lambda i: counts[i])
                counts[donor] -= 1
                counts[target] += 1
    return counts[0], counts[1], counts[2]


def split_by_run(
    records: Iterable[Mapping[str, Any]],
    *,
    run_key: str = "run_id",
    stratify_key: str | None = "fault_type",
    train_ratio: float = 0.70,
    validation_ratio: float = 0.15,
    test_ratio: float = 0.15,
    seed: int = 42,
) -> DatasetSplit:
    """Split complete experiment runs, never overlapping windows or rows.

    Stratification is best-effort at the run level.  A run with multiple labels is
    assigned a stable combined stratum, preventing any of its samples from leaking
    across partitions.
    """

    ratios = (train_ratio, validation_ratio, test_ratio)
    if any(ratio < 0 for ratio in ratios) or not abs(sum(ratios) - 1.0) < 1e-9:
        raise ValueError("split ratios must be non-negative and sum to one")
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for record in records:
        run = record.get(run_key)
        if run is None or str(run) == "":
            raise ValueError(f"every record must have a non-empty {run_key}")
        grouped[str(run)].append(record)
    if not grouped:
        return DatasetSplit((), (), (), frozenset(), frozenset(), frozenset())

    strata: dict[str, list[str]] = defaultdict(list)
    for run, rows in grouped.items():
        if stratify_key:
            labels = sorted({str(row.get(stratify_key, "unknown")) for row in rows})
            stratum = "|".join(labels)
        else:
            stratum = "all"
        strata[stratum].append(run)

    rng = random.Random(seed)
    partitions: list[set[str]] = [set(), set(), set()]
    for stratum in sorted(strata):
        runs = sorted(strata[stratum])
        rng.shuffle(runs)
        n_train, n_validation, _ = _allocation(len(runs), ratios)
        partitions[0].update(runs[:n_train])
        partitions[1].update(runs[n_train : n_train + n_validation])
        partitions[2].update(runs[n_train + n_validation :])

    def rows_for(run_ids: set[str]) -> tuple[Mapping[str, Any], ...]:
        return tuple(row for run in sorted(run_ids) for row in grouped[run])

    result = DatasetSplit(
        train=rows_for(partitions[0]),
        validation=rows_for(partitions[1]),
        test=rows_for(partitions[2]),
        train_runs=frozenset(partitions[0]),
        validation_runs=frozenset(partitions[1]),
        test_runs=frozenset(partitions[2]),
    )
    result.assert_disjoint()
    return result


def leave_groups_out(
    records: Iterable[Mapping[str, Any]],
    held_out_values: Sequence[str],
    *,
    group_key: str = "fault_type",
) -> tuple[tuple[Mapping[str, Any], ...], tuple[Mapping[str, Any], ...]]:
    """Create explicit unseen-fault/service/severity evaluation partitions."""

    held_out = {str(value) for value in held_out_values}
    train: list[Mapping[str, Any]] = []
    test: list[Mapping[str, Any]] = []
    for record in records:
        (test if str(record.get(group_key)) in held_out else train).append(record)
    return tuple(train), tuple(test)


def feature_schema(records: Iterable[Mapping[str, Any]], *, feature_key: str = "features") -> tuple[str, ...]:
    names: set[str] = set()
    for record in records:
        values = record.get(feature_key) or {}
        if not isinstance(values, Mapping):
            raise ValueError(f"{feature_key} must be a mapping")
        names.update(str(name) for name in values)
    return tuple(sorted(names))


def feature_matrix(
    records: Sequence[Mapping[str, Any]], schema: Sequence[str], *, feature_key: str = "features"
) -> list[list[float]]:
    return [
        [float((record.get(feature_key) or {}).get(name, 0.0)) for name in schema]
        for record in records
    ]
