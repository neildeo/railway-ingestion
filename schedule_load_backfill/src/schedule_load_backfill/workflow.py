from __future__ import annotations

from dataclasses import dataclass, asdict
from enum import Enum

import asyncio
import json
from google.cloud.workflows.executions_v1 import ExecutionsAsyncClient, Execution


class WorkflowTerminalState(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class WorkflowExecutionArgs:
    object: str
    generation: int
    dataset: str | None = None

    def __str__(self) -> str:
        args_dict = asdict(self)

        if self.dataset is None:
            args_dict.pop("dataset")

        return json.dumps(args_dict)


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
    exec_request = {
        "parent": parent,
        "execution": Execution(
            argument=arguments.__str__(),
        ),
    }

    exec = await client.create_execution(request=exec_request)

    while not _execution_is_terminal(exec):
        if _execution_state_is_unspecified(exec):
            raise ValueError("Execution state is STATE_UNSPECIFIED")
        await asyncio.sleep(poll_interval_seconds)
        exec = await client.get_execution(request={
            "name": exec.name
        })

    return _get_execution_result(exec, arguments)


def _execution_is_terminal(
        exec: Execution
) -> bool:
    return exec.state in [
        Execution.State.SUCCEEDED,
        Execution.State.FAILED,
        Execution.State.CANCELLED,
    ]


def _execution_state_is_unspecified(
        exec: Execution
) -> bool:
    return exec.state == Execution.State.STATE_UNSPECIFIED


def _get_execution_result(
    exec: Execution,
    arguments: WorkflowExecutionArgs,
) -> WorkflowExecutionResult:
    return WorkflowExecutionResult(
        execution_name=exec.name,
        arguments=arguments,
        state=_terminal_state_from_execution_state(exec.state))


def _terminal_state_from_execution_state(state: Execution.State) -> WorkflowTerminalState:
    match state:
        case state.SUCCEEDED:
            return WorkflowTerminalState("SUCCEEDED")
        case state.FAILED:
            return WorkflowTerminalState("FAILED")
        case state.CANCELLED:
            return WorkflowTerminalState("CANCELLED")
        case _:
            raise ValueError(
                f"Terminal execution has invalid state {state.name}")
