from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path

from dotenv import load_dotenv
import pytest
from google.cloud import (
    bigquery,
    bigquery_storage_v1,
    storage,
)

from schedule_loader.__main__ import generate_load_id, load_schedule
from schedule_loader.config import LoadConfig
from schedule_loader.source import RecordType


load_dotenv()


EXPECTED_ASSOCIATION_COUNT = 83299


@pytest.mark.integration
def test_load_associations_end_to_end() -> None:
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

    repo_root = Path(__file__).resolve().parents[3]

    config = LoadConfig(
        source_bucket=source_bucket,
        source_object=source_object,
        source_generation=source_generation,
        project=project,
        dataset="integration_test",
        record_type=RecordType.ASSOCIATION,
        table_name="schedule_associations",
        schema_path=(
            repo_root
            / "schemas"
            / "schedule_associations.json"
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
        "schedule_associations"
    )

    try:
        count_rows = list(
            bigquery_client.query(
                f"""
                SELECT COUNT(*) AS row_count
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

        assert len(count_rows) == 1
        assert (
            count_rows[0].row_count
            == EXPECTED_ASSOCIATION_COUNT
        )

        rows = list(
            bigquery_client.query(
                f"""
                SELECT *
                FROM `{table_id}`
                WHERE load_id = @load_id
                AND assoc_start_date = DATE '2026-05-17'
                AND assoc_end_date = DATE '2026-12-06'
                AND CIF_stp_indicator = 'P'
                AND (
                    (main_train_uid = 'C00202' AND assoc_train_uid = 'Y14938')
                    OR
                    (main_train_uid = 'C00203' AND assoc_train_uid = 'Y14906')
                    OR
                    (main_train_uid = 'C00204' AND assoc_train_uid = 'Y14837')
                )
                ORDER BY main_train_uid, assoc_train_uid
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

        assert len(rows) == 3

        first = rows[0]
        second = rows[1]
        third = rows[2]

        assert first.load_id == load_id
        assert first.source_object == source_object
        assert (
            first.source_generation
            == source_generation
        )
        assert first.publication_sequence == 5251
        assert first.extract_type == "full"

        assert first.transaction_type == "Create"
        assert first.main_train_uid == "C00202"
        assert first.assoc_train_uid == "Y14938"
        assert first.assoc_start_date == date(
            2026,
            5,
            17,
        )
        assert first.assoc_end_date == date(
            2026,
            12,
            6,
        )
        assert first.assoc_days == "0000001"
        assert first.category == "NP"
        assert first.date_indicator == "S"
        assert first.location == "KNGX"
        assert first.base_location_suffix is None
        assert first.assoc_location_suffix is None
        assert first.diagram_type == "T"
        assert first.CIF_stp_indicator == "P"

        assert second.transaction_type == "Create"
        assert second.main_train_uid == "C00203"
        assert second.assoc_train_uid == "Y14906"
        assert second.assoc_start_date == date(
            2026,
            5,
            17,
        )
        assert second.assoc_end_date == date(
            2026,
            12,
            6,
        )
        assert second.assoc_days == "0000001"
        assert second.category == "NP"
        assert second.date_indicator == "S"
        assert second.location == "BRADIN"
        assert second.base_location_suffix is None
        assert second.assoc_location_suffix is None
        assert second.diagram_type == "T"
        assert second.CIF_stp_indicator == "P"

        assert third.transaction_type == "Create"
        assert third.main_train_uid == "C00204"
        assert third.assoc_train_uid == "Y14837"
        assert third.assoc_start_date == date(
            2026,
            5,
            17,
        )
        assert third.assoc_end_date == date(
            2026,
            12,
            6,
        )
        assert third.assoc_days == "0000001"
        assert third.category == "NP"
        assert third.date_indicator == "S"
        assert third.location == "KNGX"
        assert third.base_location_suffix is None
        assert third.assoc_location_suffix is None
        assert third.diagram_type == "T"
        assert third.CIF_stp_indicator == "P"

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
