from __future__ import annotations

from google.cloud import bigquery
from google.cloud.workflows.executions_v1 import (
    ExecutionsAsyncClient,
)
import pytest

from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
    WorkflowTerminalState,
    run_workflow_to_completion,
)


PROJECT = "railway-analytics-508615"
LOCATION = "europe-west1"
WORKFLOW = "railway-load-schedule-object"

PARENT = (
    f"projects/{PROJECT}/"
    f"locations/{LOCATION}/"
    f"workflows/{WORKFLOW}"
)

SOURCE_OBJECT = (
    "schedules/update/sequence=5251/"
    "schedules.json.gz"
)
SOURCE_GENERATION = 1791271962017410

DATASET = "integration_test"

# We choose not to clean up load_audit because it uses an insert method which
# makes instant cleanup flaky/impossible.
TABLES = (
    "schedule_publications",
    "schedule_tiplocs",
    "schedule_associations",
    "schedule_schedules",
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_run_workflow_to_completion_end_to_end() -> None:
    arguments = WorkflowExecutionArgs(
        object=SOURCE_OBJECT,
        generation=SOURCE_GENERATION,
        dataset=DATASET,
    )

    client = ExecutionsAsyncClient()
    bigquery_client = bigquery.Client(
        project=PROJECT,
    )

    try:
        result = await run_workflow_to_completion(
            client=client,
            parent=PARENT,
            arguments=arguments,
            poll_interval_seconds=1,
        )

        assert result.arguments == arguments
        assert (
            result.state
            is WorkflowTerminalState.SUCCEEDED
        )
        assert result.execution_name.startswith("projects/")
        assert (
            f"/locations/{LOCATION}/"
            f"workflows/{WORKFLOW}/executions/"
            in result.execution_name
        )

    finally:
        for table in TABLES:
            table_id = (
                f"{PROJECT}.{DATASET}.{table}"
            )

            bigquery_client.query(
                f"""
                DELETE FROM `{table_id}`
                WHERE source_object = @source_object
                  AND source_generation = @source_generation
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "source_object",
                            "STRING",
                            SOURCE_OBJECT,
                        ),
                        bigquery.ScalarQueryParameter(
                            "source_generation",
                            "INT64",
                            SOURCE_GENERATION,
                        ),
                    ]
                ),
            ).result()
