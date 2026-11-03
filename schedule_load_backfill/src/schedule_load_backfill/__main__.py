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
            "event": "schedule_backfill_started",
            "dry_run": config.dry_run,
            "source_bucket": config.source_bucket,
            "audit_dataset": config.audit_dataset,
            "max_in_flight": config.max_in_flight,
            "poll_interval_seconds": config.poll_interval_seconds,
        }
    )

    workflow_configs = discover(
        storage_client=storage_client,
        bigquery_client=bigquery_client,
        bucket_name=config.source_bucket,
        project=config.project,
        audit_dataset=config.audit_dataset,
    )

    logger.info(
        "SCHEDULE backfill discovery completed",
        extra={
            "event": "schedule_backfill_discovery_completed",
            "discovered_count": len(workflow_configs),
        },
    )

    if config.dry_run:
        for cfg in workflow_configs:
            logger.info(
                f"Unloaded schedule object found",
                extra={
                    "event": "schedule_backfill_candidate",
                    "source_object": cfg.object,
                    "source_generation": cfg.generation,
                },
            )

        logger.info(
            "DRY RUN - aborting after discovery phase",
            extra={
                "event": "schedule_backfill_dry_run_completed",
                "discovered_count": len(workflow_configs)
            },
        )
        return

    discovered_count = len(workflow_configs)
    if discovered_count == 0:
        logger.info(
            "No unloaded objects found - exiting",
            extra={
                "event": "schedule_backfill_completed",
                "discovered_count": 0,
                "started_count": 0,
                "succeeded_count": 0,
                "unsuccessful_count": 0,
                "not_started_count": 0,
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
    except Exception:
        logger.exception(
            "Orchestration error",
            extra={
                "event": "schedule_backfill_orchestration_failed",
            },
        )
        raise

    unsuccessful_results = [
        r for r in results
        if r.state != WorkflowTerminalState.SUCCEEDED]

    succeeded_count = sum(
        result.state is WorkflowTerminalState.SUCCEEDED
        for result in results
    )
    unsuccessful_count = len(unsuccessful_results)
    started_count = len(results)
    not_started_count = len(workflow_configs) - started_count

    if unsuccessful_results:
        for job in unsuccessful_results:
            logger.error(
                "SCHEDULE workflow execution completed unsuccessfully",
                extra={
                    "event": "schedule_backfill_workflow_failed",
                    "execution_name": job.execution_name,
                    "source_object": job.arguments.object,
                    "source_generation": job.arguments.generation,
                    "workflow_state": job.state.value,
                },
            )

        logger.error(
            "SCHEDULE backfill completed with unsuccessful workflow executions",
            extra={
                "event": "schedule_backfill_failed",
                "discovered_count": discovered_count,
                "started_count": started_count,
                "succeeded_count": succeeded_count,
                "unsuccessful_count": unsuccessful_count,
                "not_started_count": not_started_count,
            },
        )

        raise WorkflowBatchFailed(
            discovered_count=discovered_count,
            started_count=started_count,
            succeeded_count=succeeded_count,
            unsuccessful_count=unsuccessful_count,
            not_started_count=not_started_count,
        )

    logger.info(
        "Schedule backfill completed successfully",
        extra={
            "event": "schedule_backfill_completed",
            "discovered_count": discovered_count,
            "started_count": started_count,
            "succeeded_count": succeeded_count,
            "unsuccessful_count": unsuccessful_count,
            "not_started_count": not_started_count,
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
