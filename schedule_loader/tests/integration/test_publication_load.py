from __future__ import annotations

from datetime import UTC, datetime, timezone
from pathlib import Path

import pytest
from tests.integration.support import IntegrationSource
from google.cloud import (
    bigquery,
    bigquery_storage_v1,
    storage,
)

from schedule_loader.__main__ import generate_load_id, load_schedule
from schedule_loader.config import LoadConfig
from schedule_loader.source import RecordType


@pytest.mark.integration
def test_load_publication_end_to_end(
    integration_source: IntegrationSource
) -> None:
    project = integration_source.project
    source_bucket = integration_source.source_bucket
    source_object = integration_source.source_object
    source_generation = integration_source.source_generation

    repo_root = Path(__file__).resolve().parents[3]

    config = LoadConfig(
        source_bucket=source_bucket,
        source_object=source_object,
        source_generation=source_generation,
        project=project,
        dataset="integration_test",
        record_type=RecordType.PUBLICATION,
        table_name="schedule_publications",
        schema_path=(
            repo_root
            / "schemas"
            / "schedule_publications.json"
        ),
    )

    load_id = generate_load_id(datetime.now(UTC))

    storage_client = storage.Client(
        project=project,
    )
    bigquery_client = bigquery.Client(
        project=project,
    )
    write_client = (
        bigquery_storage_v1.BigQueryWriteClient()
    )

    load_result = load_schedule(
        config=config,
        load_id=load_id,
        storage_client=storage_client,
        bigquery_client=bigquery_client,
        write_client=write_client,
    )

    table_id = (
        f"{project}."
        "integration_test."
        "schedule_publications"
    )

    try:
        rows = list(
            bigquery_client.query(
                f"""
                SELECT *
                FROM `{table_id}`
                WHERE load_id = @load_id
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "load_id",
                            "STRING",
                            load_id,
                        )
                    ]
                ),
            ).result()
        )

        assert len(rows) == 1

        row = rows[0]

        assert row.load_id == load_id
        assert row.source_object == source_object
        assert (
            row.source_generation
            == source_generation
        )

        assert row.publication_sequence == 5251
        assert row.extract_type == "full"

        assert row.timestamp == datetime.fromtimestamp(
            1791246721,
            tz=timezone.utc,
        )

        assert row.sender_application == "NTROD"
        assert row.sender_component == "SCHEDULE"
        assert row.sender_organisation == "Rockshore"
        assert row.classification == "public"
        assert row.owner == "Network Rail"

    finally:
        bigquery_client.query(
            f"""
            DELETE FROM `{table_id}`
            WHERE load_id = @load_id
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "load_id",
                        "STRING",
                        load_id,
                    )
                ]
            ),
        ).result()
