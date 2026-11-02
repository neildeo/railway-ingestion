from __future__ import annotations

from google.cloud import bigquery, storage

from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
)


def discover_workflow_executions(
    *,
    storage_client: storage.Client,
    bigquery_client: bigquery.Client,
    bucket_name: str,
    project: str,
    audit_dataset: str,
) -> list[WorkflowExecutionArgs]:
    raise NotImplementedError
