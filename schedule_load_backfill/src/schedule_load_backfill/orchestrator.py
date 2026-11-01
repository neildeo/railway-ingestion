from __future__ import annotations

from google.cloud.workflows.executions_v1 import ExecutionsAsyncClient
import asyncio
from collections import deque

from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
    WorkflowExecutionResult,
    WorkflowTerminalState,
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
    if max_in_flight < 1:
        raise ValueError(f"Invalid max_in_flight: {max_in_flight}")

    execution_config_queue = deque(execution_configs)

    in_flight = set(
        asyncio.create_task(
            run_workflow_to_completion(
                client=client,
                parent=parent,
                arguments=execution_config_queue.popleft(),
                poll_interval_seconds=poll_interval_seconds,
            )
        ) for _ in range(min(
            max_in_flight,
            len(execution_config_queue)
        ))
    )

    continue_admitting = True
    results: list[WorkflowExecutionResult] = []

    while in_flight:
        done, pending = await asyncio.wait(
            in_flight,
            return_when=asyncio.FIRST_COMPLETED
        )

        for d in done:
            result = await d
            if result.state != WorkflowTerminalState.SUCCEEDED:
                continue_admitting = False
            results.append(result)

        in_flight = pending

        if continue_admitting:
            for _ in range(len(done)):
                if execution_config_queue:
                    in_flight.add(asyncio.create_task(
                        run_workflow_to_completion(
                            client=client,
                            parent=parent,
                            arguments=execution_config_queue.popleft(),
                            poll_interval_seconds=poll_interval_seconds,
                        )
                    ))

    return results
