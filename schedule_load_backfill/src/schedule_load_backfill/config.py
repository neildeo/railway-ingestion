from __future__ import annotations

from dataclasses import dataclass
from typing import Type
import os


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
    project = _parse_env_variable("BQ_PROJECT", str)
    source_bucket = _parse_env_variable("SOURCE_BUCKET", str)
    audit_dataset = _parse_env_variable("BQ_DATASET", str)
    workflow_parent = _parse_env_variable("WORKFLOW_PARENT", str)
    max_in_flight = _parse_env_variable("MAX_IN_FLIGHT", int)
    poll_interval_seconds = _parse_env_variable("POLL_INTERVAL_SECONDS", float)
    dry_run = _parse_env_variable("DRY_RUN", bool)

    return BackfillConfig(
        project=project,
        source_bucket=source_bucket,
        audit_dataset=audit_dataset,
        workflow_parent=workflow_parent,
        max_in_flight=max_in_flight,
        poll_interval_seconds=poll_interval_seconds,
        dry_run=dry_run,
    )


def _parse_env_variable(env_var_name: str, type: Type):
    var_str = os.getenv(env_var_name)

    if var_str is None:
        raise ValueError(f"Missing env variable: {env_var_name}")

    return var_str
