from __future__ import annotations

import os
from datetime import date, datetime, UTC
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


EXPECTED_SCHEDULE_COUNT = 728364

UPDATE_SOURCE_OBJECT = (
    "schedules/update/sequence=5251/"
    "schedules.json.gz"
)
UPDATE_SOURCE_GENERATION = 1791271962017410


@pytest.mark.integration
def test_load_schedules_end_to_end() -> None:
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
        record_type=RecordType.SCHEDULE,
        table_name="schedule_schedules",
        schema_path=(
            repo_root
            / "schemas"
            / "schedule_schedules.json"
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
        "schedule_schedules"
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
            == EXPECTED_SCHEDULE_COUNT
        )

        normal_rows = list(
            bigquery_client.query(
                f"""
                SELECT
                    load_id,
                    source_object,
                    source_generation,
                    publication_sequence,
                    extract_type,
                    transaction_type,
                    CIF_stp_indicator,
                    CIF_train_uid,
                    schedule_start_date,
                    schedule_end_date,
                    train_status,
                    atoc_code,
                    applicable_timetable,
                    new_schedule_segment,
                    schedule_segment
                FROM `{table_id}`
                WHERE load_id = @load_id
                  AND CIF_train_uid = 'C00038'
                  AND schedule_start_date
                      = DATE '2026-05-17'
                  AND schedule_end_date
                      = DATE '2026-11-08'
                  AND CIF_stp_indicator = 'P'
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

        assert len(normal_rows) == 1

        normal = normal_rows[0]

        assert normal.load_id == load_id
        assert normal.source_object == source_object
        assert (
            normal.source_generation
            == source_generation
        )
        assert normal.publication_sequence == 5251
        assert normal.extract_type == "full"

        assert normal.transaction_type == "Create"
        assert normal.CIF_stp_indicator == "P"
        assert normal.CIF_train_uid == "C00038"
        assert normal.schedule_start_date == date(
            2026,
            5,
            17,
        )
        assert normal.schedule_end_date == date(
            2026,
            11,
            8,
        )
        assert normal.train_status == "P"
        assert normal.atoc_code == "NT"
        assert normal.applicable_timetable == "Y"

        assert normal.new_schedule_segment["traction_class"] == ""
        assert normal.new_schedule_segment["uic_code"] == ""

        segment = normal.schedule_segment

        assert segment["signalling_id"] == "5H63"
        assert segment["CIF_train_category"] == "EE"
        assert (
            segment["CIF_train_service_code"]
            == "22974000"
        )

        locations = segment["schedule_location"]

        assert len(locations) == 3

        assert locations[0]["location_type"] == "LO"
        assert locations[0]["record_identity"] == "LO"
        assert locations[0]["tiploc_code"] == "CARLILE"
        assert locations[0]["departure"] == "0905"
        assert locations[0]["arrival"] is None

        assert locations[1]["location_type"] == "LI"
        assert locations[1]["record_identity"] == "LI"
        assert locations[1]["tiploc_code"] == "CARLNEC"
        assert locations[1]["arrival"] == "0908"
        assert locations[1]["departure"] == "0911"

        assert locations[2]["location_type"] == "LT"
        assert locations[2]["record_identity"] == "LT"
        assert locations[2]["tiploc_code"] == "CARLILE"
        assert locations[2]["tiploc_instance"] == "2"
        assert locations[2]["arrival"] == "0914"
        assert locations[2]["departure"] is None

        cancellation_rows = list(
            bigquery_client.query(
                f"""
                SELECT
                    CIF_train_uid,
                    CIF_stp_indicator,
                    schedule_start_date,
                    schedule_end_date,
                    atoc_code,
                    applicable_timetable,
                    new_schedule_segment,
                    schedule_segment,
                    ARRAY_LENGTH(
                        schedule_segment.schedule_location
                    ) AS location_count
                FROM `{table_id}`
                WHERE load_id = @load_id
                  AND CIF_train_uid = 'C18018'
                  AND schedule_start_date
                      = DATE '2026-05-17'
                  AND schedule_end_date
                      = DATE '2026-12-06'
                  AND CIF_stp_indicator = 'C'
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

        assert len(cancellation_rows) == 1

        cancellation = cancellation_rows[0]

        assert cancellation.CIF_train_uid == "C18018"
        assert cancellation.CIF_stp_indicator == "C"
        assert cancellation.atoc_code is None
        assert cancellation.applicable_timetable is None
        assert cancellation.new_schedule_segment is None

        assert cancellation.schedule_segment is not None
        assert cancellation.location_count == 0
        assert (
            cancellation.schedule_segment["schedule_location"]
            == []
        )

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


@pytest.mark.integration
def test_load_schedule_update_end_to_end() -> None:
    project = os.environ["INTEGRATION_GCP_PROJECT"]
    source_bucket = os.environ[
        "INTEGRATION_SOURCE_BUCKET"
    ]

    repo_root = Path(__file__).resolve().parents[3]

    config = LoadConfig(
        source_bucket=source_bucket,
        source_object=UPDATE_SOURCE_OBJECT,
        source_generation=UPDATE_SOURCE_GENERATION,
        project=project,
        dataset="integration_test",
        record_type=RecordType.SCHEDULE,
        table_name="schedule_schedules",
        schema_path=(
            repo_root
            / "schemas"
            / "schedule_schedules.json"
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

    table_id = (
        f"{project}."
        "integration_test."
        "schedule_schedules"
    )

    try:
        load_result = load_schedule(
            config=config,
            load_id=load_id,
            storage_client=storage_client,
            bigquery_client=bigquery_client,
            write_client=write_client,
        )

        assert load_result.publication_sequence == 5251
        assert load_result.extract_type == "update"
        assert load_result.row_count > 0

        rows = list(
            bigquery_client.query(
                f"""
                SELECT
                    COUNT(*) AS row_count,
                    COUNTIF(
                        source_object != @source_object
                    ) AS wrong_source_object_count,
                    COUNTIF(
                        source_generation
                            != @source_generation
                    ) AS wrong_source_generation_count,
                    COUNTIF(
                        publication_sequence != 5251
                    ) AS wrong_publication_sequence_count,
                    COUNTIF(
                        extract_type != 'update'
                    ) AS wrong_extract_type_count
                FROM `{table_id}`
                WHERE load_id = @load_id
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "load_id",
                            "STRING",
                            load_id,
                        ),
                        bigquery.ScalarQueryParameter(
                            "source_object",
                            "STRING",
                            UPDATE_SOURCE_OBJECT,
                        ),
                        bigquery.ScalarQueryParameter(
                            "source_generation",
                            "INT64",
                            UPDATE_SOURCE_GENERATION,
                        ),
                    ]
                ),
            ).result()
        )

        assert len(rows) == 1

        row = rows[0]

        assert row.row_count == load_result.row_count
        assert row.wrong_source_object_count == 0
        assert row.wrong_source_generation_count == 0
        assert row.wrong_publication_sequence_count == 0
        assert row.wrong_extract_type_count == 0

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
