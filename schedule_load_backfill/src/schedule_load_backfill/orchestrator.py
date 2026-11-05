from __future__ import annotations

from google.cloud.workflows.executions_v1 import ExecutionsAsyncClient
import asyncio
from collections import deque
import logging

from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
    WorkflowExecutionResult,
    WorkflowTerminalState,
    run_workflow_to_completion,
)


logger = logging.getLogger(__name__)


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
        _create_execution_task(
            client=client,
            parent=parent,
            arguments=execution_config_queue.popleft(),
            poll_interval_seconds=poll_interval_seconds,
        ) for _ in range(min(
            max_in_flight,
            len(execution_config_queue)
        ))
    )

    continue_admitting = True
    results: list[WorkflowExecutionResult] = []
    exceptions: list[Exception] = []

    while in_flight:
        done, in_flight = await asyncio.wait(
            in_flight,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in done:
            try:
                result = task.result()
            except Exception as exc:
                exceptions.append(exc)
                continue_admitting = False
                continue

            logger.info(
                "SCHEDULE workflow execution completed",
                extra={
                    "event": "schedule_backfill_workflow_completed",
                    "execution_name": result.execution_name,
                    "source_object": result.arguments.object,
                    "source_generation": result.arguments.generation,
                    "workflow_state": result.state.value,
                },
            )

            results.append(result)

            if result.state is not WorkflowTerminalState.SUCCEEDED:
                continue_admitting = False

        if continue_admitting:
            for _ in range(len(done)):
                if not execution_config_queue:
                    break

                in_flight.add(
                    _create_execution_task(
                        client=client,
                        parent=parent,
                        arguments=execution_config_queue.popleft(),
                        poll_interval_seconds=poll_interval_seconds,
                    )
                )

    if len(exceptions) == 1:
        raise exceptions[0]
    elif len(exceptions) > 1:
        raise ExceptionGroup(
            "Multiple workflow orchestration errors", exceptions)

    return results


def _create_execution_task(
    *,
    client: ExecutionsAsyncClient,
    parent: str,
    arguments: WorkflowExecutionArgs,
    poll_interval_seconds: float,
) -> asyncio.Task[WorkflowExecutionResult]:
    return asyncio.create_task(
        run_workflow_to_completion(
            client=client,
            parent=parent,
            arguments=arguments,
            poll_interval_seconds=poll_interval_seconds,
        )
    )
