from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from google.cloud import bigquery, storage
from google.cloud.workflows.executions_v1 import (
    ExecutionsAsyncClient,
)

from schedule_load_backfill.config import (
    BackfillConfig,
    load_config_from_env,
)
from schedule_load_backfill.discovery import (
    discover_workflow_executions,
)
from schedule_load_backfill.logging_config import (
    configure_logging,
)
from schedule_load_backfill.orchestrator import (
    execute_workflows,
)
from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
    WorkflowExecutionResult,
    WorkflowTerminalState,
)


logger = logging.getLogger(__name__)


class WorkflowBatchFailed(RuntimeError):
    def __init__(
        self,
        *,
        discovered_count: int,
        started_count: int,
        succeeded_count: int,
        unsuccessful_count: int,
        not_started_count: int,
    ) -> None:
        self.discovered_count = discovered_count
        self.started_count = started_count
        self.succeeded_count = succeeded_count
        self.unsuccessful_count = unsuccessful_count
        self.not_started_count = not_started_count

        super().__init__(
            f"{unsuccessful_count} workflow execution(s) "
            "completed unsuccessfully"
        )


Discovery = Callable[..., list[WorkflowExecutionArgs]]

Executor = Callable[
    ...,
    Awaitable[list[WorkflowExecutionResult]],
]


async def run_backfill(
    *,
    config: BackfillConfig,
    storage_client: Any,
    bigquery_client: Any,
    executions_client: Any,
    discover: Discovery = discover_workflow_executions,
    execute: Executor = execute_workflows,
) -> None:
    logger.info(
        "Starting SCHEDULE data load backfill...",
        extra={
            "event": "schedule_backfill_started"
        }
    )

    workflow_configs = discover(
        storage_client=storage_client,
        bigquery_client=bigquery_client,
        bucket_name=config.source_bucket,
        project=config.project,
        audit_dataset=config.audit_dataset,
    )

    for cfg in workflow_configs:
        logger.info(
            f"Unloaded schedule object found",
            extra={
                "event": "schedule_backfill_candidate",
                "source_object": cfg.object,
                "source_generation": cfg.generation,
                "dataset": cfg.dataset,
            },
        )

    if config.dry_run:
        logger.info(
            "DRY RUN - aborting after discovery phase",
            extra={
                "event": "schedule_backfill_dry_run_completed",
            },
        )
        return

    if len(workflow_configs) == 0:
        logger.info(
            "No unloaded objects found - exiting",
            extra={
                "event": "schedule_backfill_completed",
            },
        )
        return

    try:
        results = await execute(
            client=executions_client,
            parent=config.workflow_parent,
            execution_configs=workflow_configs,
            max_in_flight=config.max_in_flight,
            poll_interval_seconds=config.poll_interval_seconds,
        )
    except Exception as exc:
        logger.error(
            "Orchestration error",
            extra={
                "event": "schedule_backfill_orchestration_failed",
            },
            exc_info=True,
        )
        raise exc

    unsuccessful_jobs = [r for r in results if r.state !=
                         WorkflowTerminalState.SUCCEEDED]

    if unsuccessful_jobs:
        for job in unsuccessful_jobs:
            logger.error(
                "Schedule backfill failed",
                extra={
                    "event": "schedule_backfill_workflow_failed",
                    "execution_name": job.execution_name,
                    "source_object": job.arguments.object,
                    "source_generation": job.arguments.generation,
                    "workflow_state": job.state.value,
                },
            )
        raise WorkflowBatchFailed(
            discovered_count=len(workflow_configs),
            started_count=len(results),
            succeeded_count=len(
                [r for r in results if r.state == WorkflowTerminalState.SUCCEEDED]),
            unsuccessful_count=len(unsuccessful_jobs),
            not_started_count=len(workflow_configs) - len(results),
        )

    logger.info(
        "Schedule backfill completed successfully",
        extra={
            "event": "schedule_backfill_completed",
            "discovered_count": len(workflow_configs),
            "started_count": len(results),
            "succeeded_count": len([r for r in results if r.state == WorkflowTerminalState.SUCCEEDED]),
            "unsuccessful_count": 0,
            "not_started_count": len(workflow_configs) - len(results),
        },
    )


def main() -> None:
    configure_logging()

    config = load_config_from_env()

    storage_client = storage.Client(
        project=config.project,
    )
    bigquery_client = bigquery.Client(
        project=config.project,
    )
    executions_client = ExecutionsAsyncClient()

    asyncio.run(
        run_backfill(
            config=config,
            storage_client=storage_client,
            bigquery_client=bigquery_client,
            executions_client=executions_client,
        )
    )


if __name__ == "__main__":
    main()
