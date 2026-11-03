from __future__ import annotations

from google.cloud import bigquery, storage
import pytest

from schedule_load_backfill.discovery import (
    discover_workflow_executions,
)


PROJECT = "railway-analytics-508615"
BUCKET_NAME = (
    "railway-analytics-508615-network-rail-open-data-raw"
)
DATASET = "integration_test"

SOURCE_OBJECT = (
    "schedules/full/sequence=5251/schedules.json.gz"
)
SOURCE_GENERATION = 1791272082108076


# This test doesn't test logic at all - it's mainly there to prove that the
# GCP clients actually work.
@pytest.mark.integration
def test_discovery_reads_live_storage_and_audit() -> None:
    storage_client = storage.Client(project=PROJECT)
    bigquery_client = bigquery.Client(project=PROJECT)

    result = discover_workflow_executions(
        storage_client=storage_client,
        bigquery_client=bigquery_client,
        bucket_name=BUCKET_NAME,
        project=PROJECT,
        audit_dataset="integration_test",
    )

    assert all(
        args.object.startswith("schedules/")
        for args in result
    )

    assert all(
        isinstance(args.generation, int)
        for args in result
    )

    assert result == sorted(
        result,
        key=lambda args: args.object,
    )
