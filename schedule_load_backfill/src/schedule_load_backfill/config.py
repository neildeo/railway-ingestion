from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BackfillConfig:
    project: str
    source_bucket: str
    audit_dataset: str
    workflow_parent: str
    max_in_flight: int
    poll_interval_seconds: float
    dry_run: bool


def load_config_from_env() -> BackfillConfig:
    raise NotImplementedError
