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
    raise NotImplementedError


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
