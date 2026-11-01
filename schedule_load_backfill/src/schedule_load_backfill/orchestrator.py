from __future__ import annotations

from google.cloud.workflows.executions_v1 import ExecutionsAsyncClient

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
    poll_interval_seconds: float = 10.0,
) -> list[WorkflowExecutionResult]:
    results = []

    for e in execution_configs:
        result = await run_workflow_to_completion(
            client=client,
            parent=parent,
            arguments=e,
            poll_interval_seconds=poll_interval_seconds
        )
        results.append(result)

    return results
