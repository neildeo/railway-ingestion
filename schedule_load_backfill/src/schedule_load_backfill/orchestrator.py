from __future__ import annotations

from google.cloud.workflows.executions_v1 import ExecutionsAsyncClient
import asyncio

from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
    WorkflowExecutionResult,
    run_workflow_to_completion,
)


async def execute_workflows(
    *,
    client: ExecutionsAsyncClient,
    parent: str,
    execution_configs: list[WorkflowExecutionArgs],
    max_in_flight: int,
    poll_interval_seconds: float = 10.0,
) -> list[WorkflowExecutionResult]:
    execution_tasks = [
        run_workflow_to_completion(
            client=client,
            parent=parent,
            arguments=cfg,
            poll_interval_seconds=poll_interval_seconds
        ) for cfg in execution_configs
    ]

    return await asyncio.gather(*execution_tasks)
