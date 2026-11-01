from __future__ import annotations

import asyncio
import json

from google.cloud.workflows.executions_v1.types import Execution
import pytest

from schedule_load_backfill.orchestrator import (
    execute_workflows,
)
from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
    WorkflowExecutionResult,
    WorkflowTerminalState,
)


PARENT = (
    "projects/test-project/"
    "locations/europe-west1/"
    "workflows/railway-load-schedule-object"
)

ARGUMENTS_A = WorkflowExecutionArgs(
    object="schedules/update/sequence=5251/schedules.json.gz",
    generation=111,
)

ARGUMENTS_B = WorkflowExecutionArgs(
    object="schedules/update/sequence=5252/schedules.json.gz",
    generation=222,
)

EXECUTION_A = f"{PARENT}/executions/execution-a"
EXECUTION_B = f"{PARENT}/executions/execution-b"


class BarrierExecutionsClient:
    def __init__(self) -> None:
        self.poll_barrier = asyncio.Barrier(2)

        self.execution_names = {
            ARGUMENTS_A.object: EXECUTION_A,
            ARGUMENTS_B.object: EXECUTION_B,
        }

        self.create_requests: list[dict] = []
        self.get_requests: list[dict] = []

    async def create_execution(
        self,
        *,
        request: dict,
    ) -> Execution:
        self.create_requests.append(request)

        arguments = json.loads(
            request["execution"].argument
        )

        execution_name = self.execution_names[
            arguments["object"]
        ]

        return Execution(
            name=execution_name,
            state=Execution.State.ACTIVE,
        )

    async def get_execution(
        self,
        *,
        request: dict,
    ) -> Execution:
        self.get_requests.append(request)

        await self.poll_barrier.wait()

        return Execution(
            name=request["name"],
            state=Execution.State.SUCCEEDED,
        )


@pytest.mark.asyncio
async def test_execute_workflows_runs_executions_concurrently() -> None:
    client = BarrierExecutionsClient()

    results = await asyncio.wait_for(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
            ],
            poll_interval_seconds=0,
        ),
        timeout=1,
    )

    assert set(results) == {
        WorkflowExecutionResult(
            execution_name=EXECUTION_A,
            arguments=ARGUMENTS_A,
            state=WorkflowTerminalState.SUCCEEDED,
        ),
        WorkflowExecutionResult(
            execution_name=EXECUTION_B,
            arguments=ARGUMENTS_B,
            state=WorkflowTerminalState.SUCCEEDED,
        ),
    }


@pytest.mark.asyncio
async def test_execute_workflows_starts_all_executions() -> None:
    client = BarrierExecutionsClient()

    await asyncio.wait_for(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
            ],
            poll_interval_seconds=0,
        ),
        timeout=1,
    )

    created_arguments = {
        WorkflowExecutionArgs(
            **json.loads(
                request["execution"].argument
            )
        )
        for request in client.create_requests
    }

    assert created_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
    }
