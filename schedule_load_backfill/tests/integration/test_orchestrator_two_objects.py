from __future__ import annotations

from google.cloud import bigquery
from google.cloud.workflows.executions_v1 import (
    ExecutionsAsyncClient,
)
import pytest

from schedule_load_backfill.orchestrator import (
    execute_workflows,
)
from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
    WorkflowTerminalState,
)


PROJECT = "railway-analytics-508615"
LOCATION = "europe-west1"
WORKFLOW = "railway-load-schedule-object"

PARENT = (
    f"projects/{PROJECT}/"
    f"locations/{LOCATION}/"
    f"workflows/{WORKFLOW}"
)

DATASET = "integration_test"

ARGUMENTS_A = WorkflowExecutionArgs(
    object=(
        "schedules/update/sequence=5251/"
        "schedules.json.gz"
    ),
    generation=1791271962017410,
    dataset=DATASET,
)

ARGUMENTS_B = WorkflowExecutionArgs(
    object=(
        "schedules/update/sequence=5252/"
        "schedules.json.gz"
    ),
    generation=1791352844571103,
    dataset=DATASET,
)

# load_audit is intentionally excluded because streaming-buffered
# rows cannot be deleted immediately after the workflow completes.
TABLES = (
    "schedule_publications",
    "schedule_tiplocs",
    "schedule_associations",
    "schedule_schedules",
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_workflows_end_to_end() -> None:
    client = ExecutionsAsyncClient()
    bigquery_client = bigquery.Client(
        project=PROJECT,
    )

    execution_configs = [
        ARGUMENTS_A,
        ARGUMENTS_B,
    ]

    try:
        results = await execute_workflows(
            client=client,
            parent=PARENT,
            execution_configs=execution_configs,
            poll_interval_seconds=1,
        )

        assert {
            result.arguments
            for result in results
        } == {
            ARGUMENTS_A,
            ARGUMENTS_B,
        }

        assert {
            result.state
            for result in results
        } == {
            WorkflowTerminalState.SUCCEEDED,
        }

        assert len(results) == 2

        for result in results:
            assert result.execution_name.startswith(
                "projects/"
            )
            assert (
                f"/locations/{LOCATION}/"
                f"workflows/{WORKFLOW}/executions/"
                in result.execution_name
            )

    finally:
        for arguments in execution_configs:
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
                                arguments.object,
                            ),
                            bigquery.ScalarQueryParameter(
                                "source_generation",
                                "INT64",
                                arguments.generation,
                            ),
                        ]
                    ),
                ).result()
