from __future__ import annotations

from google.cloud import bigquery, storage

from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
)

EXPECTED_RECORD_FAMILIES = {
    "publication",
    "tiploc",
    "association",
    "schedule",
}


def discover_workflow_executions(
    *,
    storage_client: storage.Client,
    bigquery_client: bigquery.Client,
    bucket_name: str,
    project: str,
    audit_dataset: str,
) -> list[WorkflowExecutionArgs]:
    blobs = storage_client.list_blobs(
        bucket_name,
        prefix="schedules/",
    )

    completed = _fetch_completed_objects(
        bigquery_client=bigquery_client,
        project=project,
        audit_dataset=audit_dataset,
    )

    workflow_ex_args = []

    for blob in blobs:
        args = _workflow_execution_args_from_blob(blob)

        if args not in completed:
            workflow_ex_args.append(args)

    workflow_ex_args.sort(key=lambda args: args.object)

    return workflow_ex_args


def _workflow_execution_args_from_blob(blob: storage.Blob) -> WorkflowExecutionArgs:
    if blob.generation is None:
        raise ValueError(
            f"Blob {blob.name!r} has no generation"
        )

    return WorkflowExecutionArgs(
        object=blob.name,  # type: ignore
        generation=blob.generation,
    )


def _fetch_completed_objects(
    *,
    bigquery_client: bigquery.Client,
    project: str,
    audit_dataset: str,
) -> set[WorkflowExecutionArgs]:
    query_str = f"""
select
    source_object,
    source_generation,
    record_family,
    status,
from `{project}.{audit_dataset}.load_audit`
where
    source_feed = 'schedule'
"""

    job = bigquery_client.query(query_str)
    rows = job.result()

    families_by_object: dict[tuple[str, int], set[str]] = {}

    for row in rows:
        if row.status != "SUCCEEDED":
            continue

        key = (
            row.source_object,
            int(row.source_generation),
        )

        families_by_object.setdefault(
            key,
            set(),
        ).add(row.record_family)

    return {
        WorkflowExecutionArgs(
            object=object_name,
            generation=generation,
        )
        for (object_name, generation), families
        in families_by_object.items()
        if families == EXPECTED_RECORD_FAMILIES
    }
