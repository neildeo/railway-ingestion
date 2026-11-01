from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from google.cloud.workflows.executions_v1 import ExecutionsAsyncClient


class WorkflowTerminalState(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class WorkflowExecutionArgs:
    object: str
    generation: int
    dataset: str | None = None


@dataclass(frozen=True)
class WorkflowExecutionResult:
    execution_name: str
    arguments: WorkflowExecutionArgs
    state: WorkflowTerminalState


async def run_workflow_to_completion(
    *,
    client: ExecutionsAsyncClient,
    parent: str,
    arguments: WorkflowExecutionArgs,
    poll_interval_seconds: float = 10.0,
) -> WorkflowExecutionResult:
    raise NotImplementedError
