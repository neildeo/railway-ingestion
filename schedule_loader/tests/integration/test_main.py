from __future__ import annotations

import pytest
import os
from datetime import datetime, UTC
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path

import schedule_loader.__main__ as loader_main


@pytest.mark.integration
def test_main_writes_success_audit_for_tiploc_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    monkeypatch.chdir(repo_root)

    load_dotenv()

    project = os.environ["INTEGRATION_GCP_PROJECT"]
    source_bucket = os.environ[
        "INTEGRATION_SOURCE_BUCKET"
    ]
    source_object = os.environ[
        "INTEGRATION_SOURCE_OBJECT"
    ]
    source_generation = int(
        os.environ["INTEGRATION_SOURCE_GENERATION"]
    )

    load_id = loader_main.generate_load_id(
        datetime.now(UTC)
    )

    monkeypatch.setenv(
        "SOURCE_BUCKET",
        source_bucket,
    )
    monkeypatch.setenv(
        "SOURCE_OBJECT",
        source_object,
    )
    monkeypatch.setenv(
        "SOURCE_GENERATION",
        str(source_generation),
    )
    monkeypatch.setenv(
        "BQ_PROJECT",
        project,
    )
    monkeypatch.setenv(
        "BQ_DATASET",
        "integration_test",
    )
    monkeypatch.setenv(
        "RECORD_TYPE",
        "tiploc",
    )

    monkeypatch.setattr(
        loader_main,
        "generate_load_id",
        lambda timestamp: load_id,
    )

    bigquery_client = bigquery.Client(
        project=project,
    )

    tiploc_table_id = (
        f"{project}."
        "integration_test."
        "schedule_tiplocs"
    )

    audit_table_id = (
        f"{project}."
        "integration_test."
        "load_audit"
    )

    try:
        loader_main.main()

        loaded_rows = list(
            bigquery_client.query(
                f"""
                SELECT COUNT(*) AS row_count
                FROM `{tiploc_table_id}`
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

        assert len(loaded_rows) == 1
        assert loaded_rows[0].row_count == 12096

        audit_rows = list(
            bigquery_client.query(
                f"""
                SELECT *
                FROM `{audit_table_id}`
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

        assert len(audit_rows) == 1

        audit = audit_rows[0]

        assert audit.load_id == load_id
        assert audit.source_feed == "schedule"
        assert audit.record_family == "tiploc"

        assert audit.source_object == source_object
        assert (
            audit.source_generation
            == source_generation
        )

        assert audit.publication_sequence == 5251
        assert audit.extract_type == "full"

        assert audit.status == "SUCCEEDED"
        assert audit.row_count == 12096
        assert audit.error is None

        assert audit.started_at is not None
        assert audit.finished_at is not None
        assert audit.finished_at >= audit.started_at

    finally:
        bigquery_client.query(
            f"""
            DELETE FROM `{tiploc_table_id}`
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
