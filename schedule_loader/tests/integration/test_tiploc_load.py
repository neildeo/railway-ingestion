from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
import pytest
from google.cloud import (
    bigquery,
    bigquery_storage_v1,
    storage,
)

from schedule_loader.__main__ import load_schedule
from schedule_loader.config import LoadConfig
from schedule_loader.source import RecordType


load_dotenv()


@pytest.mark.integration
def test_load_tiplocs_end_to_end() -> None:
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
        record_type=RecordType.TIPLOC,
        table_name="schedule_tiplocs",
        schema_path=(
            repo_root
            / "schedule_schemas"
            / "schedule_tiplocs.json"
        ),
    )

    storage_client = storage.Client(
        project=project,
    )
    bigquery_client = bigquery.Client(
        project=project,
    )
    write_client = (
        bigquery_storage_v1.BigQueryWriteClient()
    )

    load_id = load_schedule(
        config=config,
        storage_client=storage_client,
        bigquery_client=bigquery_client,
        write_client=write_client,
    )

    table_id = (
        f"{project}."
        "integration_test."
        "schedule_tiplocs"
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
        assert count_rows[0].row_count == 12096

        rows = list(
            bigquery_client.query(
                f"""
                SELECT *
                FROM `{table_id}`
                WHERE load_id = @load_id
                  AND tiploc_code IN (
                    'AACHEN',
                    'ABARASQ',
                    'ABCWM'
                  )
                ORDER BY tiploc_code
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

        aachen = rows[0]
        abarasq = rows[1]
        abcwm = rows[2]

        assert aachen.load_id == load_id
        assert aachen.source_object == source_object
        assert (
            aachen.source_generation
            == source_generation
        )
        assert aachen.publication_sequence == 5251
        assert aachen.extract_type == "full"

        assert aachen.transaction_type == "Create"
        assert aachen.tiploc_code == "AACHEN"
        assert aachen.nalco == "081601"
        assert aachen.stanox == "00005"
        assert aachen.crs_code is None
        assert aachen.description is None
        assert aachen.tps_description == "AACHEN"

        assert abarasq.transaction_type == "Create"
        assert abarasq.tiploc_code == "ABARASQ"
        assert abarasq.nalco == "027400"
        assert abarasq.stanox is None
        assert abarasq.crs_code == "AER"
        assert abarasq.description == "SQWAR ALBAN SQ"
        assert (
            abarasq.tps_description
            == "ABERAERON ALBAN SQUARE"
        )

        assert abcwm.transaction_type == "Create"
        assert abcwm.tiploc_code == "ABCWM"
        assert abcwm.nalco == "385964"
        assert abcwm.stanox == "78128"
        assert abcwm.crs_code is None
        assert abcwm.description is None
        assert abcwm.tps_description == "ABERCWMBOI"

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
