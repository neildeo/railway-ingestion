from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from google.cloud import bigquery
import pyarrow as pa
import pytest

import schedule_loader.__main__ as loader_main
from schedule_loader.config import LoadConfig
from schedule_loader.schema import SchemaMismatchError
from schedule_loader.source import (
    RecordType,
    SourceMetadata,
)


LOAD_AUDIT_SCHEMA = (
    bigquery.SchemaField(
        "load_id",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_feed",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "record_family",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_object",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_generation",
        "INTEGER",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "publication_sequence",
        "INTEGER",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "extract_type",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "status",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "row_count",
        "INTEGER",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "started_at",
        "TIMESTAMP",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "finished_at",
        "TIMESTAMP",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "error",
        "STRING",
        mode="NULLABLE",
    ),
)


PUBLICATION_SCHEMA = (
    bigquery.SchemaField(
        "load_id",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_object",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_generation",
        "INTEGER",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "publication_sequence",
        "INTEGER",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "extract_type",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "timestamp",
        "TIMESTAMP",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "sender_application",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "sender_component",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "sender_organisation",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "classification",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "owner",
        "STRING",
        mode="REQUIRED",
    ),
)


TIPLOC_SCHEMA = (
    bigquery.SchemaField(
        "load_id",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_object",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_generation",
        "INTEGER",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "publication_sequence",
        "INTEGER",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "extract_type",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "transaction_type",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "tiploc_code",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "tps_description",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "description",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "crs_code",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "stanox",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "nalco",
        "STRING",
        mode="REQUIRED",
    ),
)


ASSOCIATION_SCHEMA = (
    bigquery.SchemaField(
        "load_id",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_object",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_generation",
        "INTEGER",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "publication_sequence",
        "INTEGER",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "extract_type",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "transaction_type",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "CIF_stp_indicator",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "assoc_start_date",
        "DATE",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "assoc_train_uid",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "main_train_uid",
        "STRING",
        mode="REQUIRED",
    ),
)


SCHEDULE_SCHEMA = (
    bigquery.SchemaField(
        "load_id",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_object",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "source_generation",
        "INTEGER",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "publication_sequence",
        "INTEGER",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "extract_type",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "transaction_type",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "CIF_stp_indicator",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "CIF_train_uid",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "schedule_start_date",
        "DATE",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "schedule_end_date",
        "DATE",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "atoc_code",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "applicable_timetable",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "new_schedule_segment",
        "RECORD",
        mode="NULLABLE",
        fields=(
            bigquery.SchemaField(
                "traction_class",
                "STRING",
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                "uic_code",
                "STRING",
                mode="NULLABLE",
            ),
        ),
    ),
    bigquery.SchemaField(
        "schedule_segment",
        "RECORD",
        mode="NULLABLE",
        fields=(
            bigquery.SchemaField(
                "signalling_id",
                "STRING",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                "schedule_location",
                "RECORD",
                mode="REPEATED",
                fields=(
                    bigquery.SchemaField(
                        "location_type",
                        "STRING",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        "record_identity",
                        "STRING",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        "tiploc_code",
                        "STRING",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        "departure",
                        "STRING",
                        mode="NULLABLE",
                    ),
                ),
            ),
        ),
    ),
)


HEADER = {
    "Metadata": {
        "sequence": 5251,
        "type": "full",
    },
    "Sender": {
        "application": "NTROD",
        "component": "SCHEDULE",
        "organisation": "Rockshore",
    },
    "classification": "public",
    "owner": "Network Rail",
    "timestamp": 1791246721,
}


TIPLOC_RECORDS = [
    {
        "transaction_type": "Create",
        "tiploc_code": "PADTON",
        "tps_description": "LONDON PADDINGTON",
        "description": "PADDINGTON",
        "crs_code": "PAD",
        "stanox": "33057",
        "nalco": "308700",
    },
    {
        "transaction_type": "Create",
        "tiploc_code": "RDNGSTN",
        "tps_description": "READING",
        "nalco": "314900",
    },
    {
        "transaction_type": "Create",
        "tiploc_code": "OXFD",
        "tps_description": "OXFORD",
        "description": "OXFORD",
        "crs_code": "OXF",
        "stanox": "31105",
        "nalco": "311500",
    },
]


ASSOCIATION_RECORDS = [
    {
        "transaction_type": "Create",
        "main_train_uid": "C30123",
        "assoc_train_uid": "C42949",
        "assoc_start_date": "2024-06-03T00:00:00Z",
        "assoc_end_date": "2024-06-21T00:00:00Z",
        "assoc_days": "1111100",
        "category": "NP",
        "date_indicator": "S",
        "location": "YORK",
        "base_location_suffix": None,
        "assoc_location_suffix": None,
        "diagram_type": "T",
        "CIF_stp_indicator": "P",
    },
    {
        "transaction_type": "Create",
        "main_train_uid": "C11111",
        "assoc_train_uid": "C22222",
        "assoc_start_date": "2024-07-01T00:00:00Z",
        "assoc_end_date": "2024-07-31T00:00:00Z",
        "assoc_days": "0000011",
        "category": "JJ",
        "date_indicator": "S",
        "location": "LEEDS",
        "base_location_suffix": None,
        "assoc_location_suffix": None,
        "diagram_type": "T",
        "CIF_stp_indicator": "P",
    },
]


SCHEDULE_RECORDS = [
    {
        "transaction_type": "Create",
        "CIF_stp_indicator": "P",
        "CIF_train_uid": "C00038",
        "schedule_start_date": "2026-05-17",
        "schedule_end_date": "2026-11-08",
        "CIF_bank_holiday_running": None,
        "schedule_days_runs": "0000001",
        "train_status": "P",
        "atoc_code": "NT",
        "applicable_timetable": "Y",
        "new_schedule_segment": {
            "traction_class": "",
            "uic_code": "",
        },
        "schedule_segment": {
            "signalling_id": "5H63",
            "CIF_train_category": "EE",
            "CIF_headcode": "",
            "CIF_course_indicator": 1,
            "CIF_train_service_code": "22974000",
            "CIF_business_sector": "??",
            "CIF_power_type": "DMU",
            "CIF_timing_load": "S",
            "CIF_speed": "075",
            "CIF_operating_characteristics": None,
            "CIF_train_class": None,
            "CIF_sleepers": None,
            "CIF_reservations": None,
            "CIF_connection_indicator": None,
            "CIF_catering_code": None,
            "CIF_service_branding": "",
            "schedule_location": [
                {
                    "location_type": "LO",
                    "record_identity": "LO",
                    "tiploc_code": "CARLILE",
                    "tiploc_instance": None,
                    "departure": "0905",
                    "public_departure": None,
                    "platform": "CLS",
                    "line": None,
                    "engineering_allowance": None,
                    "pathing_allowance": None,
                    "performance_allowance": None,
                }
            ],
        },
    },
    {
        "transaction_type": "Create",
        "CIF_stp_indicator": "C",
        "CIF_train_uid": "C18018",
        "schedule_start_date": "2026-05-17",
        "schedule_end_date": "2026-12-06",
        "CIF_bank_holiday_running": None,
        "schedule_days_runs": "0000001",
        "train_status": " ",
        "schedule_segment": {
            "signalling_id": "    ",
            "CIF_train_category": "",
            "CIF_headcode": "",
            "CIF_course_indicator": 1,
            "CIF_train_service_code": "        ",
            "CIF_business_sector": "??",
            "CIF_power_type": None,
            "CIF_timing_load": None,
            "CIF_speed": None,
            "CIF_operating_characteristics": None,
            "CIF_train_class": None,
            "CIF_sleepers": None,
            "CIF_reservations": None,
            "CIF_connection_indicator": None,
            "CIF_catering_code": None,
            "CIF_service_branding": "",
        },
    },
]


SOURCE_METADATA = SourceMetadata(
    publication_sequence=5251,
    extract_type="full",
)


def _config(
    record_type: RecordType,
    table_name: str,
    schema_name: str,
) -> LoadConfig:
    return LoadConfig(
        source_bucket="railway-raw",
        source_object=(
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
        source_generation=123456789,
        project="test-project",
        dataset="raw",
        record_type=record_type,
        table_name=table_name,
        schema_path=Path(f"/schemas/{schema_name}"),
    )


def _publication_config() -> LoadConfig:
    return _config(
        RecordType.PUBLICATION,
        "schedule_publications",
        "schedule_publications.json",
    )


def _tiploc_config() -> LoadConfig:
    return _config(
        RecordType.TIPLOC,
        "schedule_tiplocs",
        "schedule_tiplocs.json",
    )


def _association_config() -> LoadConfig:
    return _config(
        RecordType.ASSOCIATION,
        "schedule_associations",
        "schedule_associations.json",
    )


def _schedule_config() -> LoadConfig:
    return _config(
        RecordType.SCHEDULE,
        "schedule_schedules",
        "schedule_schedules.json",
    )


class FakeBigQueryClient:
    def __init__(
        self,
        schemas: dict[str, tuple[bigquery.SchemaField, ...]],
    ) -> None:
        self.schemas = schemas
        self.requested_tables: list[str] = []
        self.insert_calls: list[dict[str, object]] = []
        self.insert_errors: list[dict[str, object]] = []

    def get_table(self, table: str):
        self.requested_tables.append(table)

        return SimpleNamespace(
            schema=self.schemas[table],
        )

    def insert_rows_json(
        self,
        table: str,
        rows,
        *,
        row_ids,
    ):
        self.insert_calls.append(
            {
                "table": table,
                "rows": rows,
                "row_ids": row_ids,
            }
        )
        return self.insert_errors


def _load_client(
    table_name: str,
    schema: tuple[bigquery.SchemaField, ...],
) -> FakeBigQueryClient:
    return FakeBigQueryClient(
        {
            f"test-project.raw.{table_name}": schema,
        }
    )


def _writer_that_materialises(
    written: dict[str, object],
    *,
    row_count: int | None = None,
):
    def fake_write_pending_batches(
        *,
        client,
        table: str,
        batches,
    ) -> int:
        materialised = list(batches)
        written["client"] = client
        written["table"] = table
        written["batches"] = materialised

        if row_count is not None:
            return row_count

        return sum(
            batch.num_rows
            for batch in materialised
        )

    return fake_write_pending_batches


def test_load_schedule_shapes_publication_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _publication_config()
    bigquery_client = _load_client(
        config.table_name,
        PUBLICATION_SCHEMA,
    )
    written: dict[str, object] = {}

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: PUBLICATION_SCHEMA,
    )
    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        lambda **kwargs: SOURCE_METADATA,
    )
    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        lambda **kwargs: iter([HEADER]),
    )
    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        _writer_that_materialises(written),
    )

    result = loader_main.load_schedule(
        config=config,
        load_id="1234ABC",
        storage_client=object(),  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=object(),  # pyright: ignore[reportArgumentType]
    )

    batches: list[pa.RecordBatch] = written["batches"]  # type: ignore
    row = batches[0].to_pylist()[0]

    assert row["load_id"] == "1234ABC"
    assert row["source_object"] == config.source_object
    assert row["source_generation"] == config.source_generation
    assert row["publication_sequence"] == 5251
    assert row["extract_type"] == "full"
    assert row["sender_application"] == "NTROD"
    assert row["sender_component"] == "SCHEDULE"
    assert row["sender_organisation"] == "Rockshore"
    assert row["classification"] == "public"
    assert row["owner"] == "Network Rail"

    assert result.publication_sequence == 5251
    assert result.extract_type == "full"
    assert result.row_count == 1


def test_load_schedule_shapes_association_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _association_config()
    bigquery_client = _load_client(
        config.table_name,
        ASSOCIATION_SCHEMA,
    )
    written: dict[str, object] = {}

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: ASSOCIATION_SCHEMA,
    )
    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        lambda **kwargs: SOURCE_METADATA,
    )
    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        lambda **kwargs: iter(ASSOCIATION_RECORDS),
    )
    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        _writer_that_materialises(written),
    )

    result = loader_main.load_schedule(
        config=config,
        load_id="1234ABC",
        storage_client=object(),  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=object(),  # pyright: ignore[reportArgumentType]
    )

    batches: list[pa.RecordBatch] = written["batches"]  # type: ignore
    rows = [
        row
        for batch in batches
        for row in batch.to_pylist()
    ]

    assert len(rows) == 2
    assert rows[0]["main_train_uid"] == "C30123"
    assert rows[0]["assoc_train_uid"] == "C42949"
    assert rows[0]["assoc_start_date"] == date(2024, 6, 3)
    assert result.row_count == 2


def test_load_schedule_shapes_and_batches_tiploc_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()
    bigquery_client = _load_client(
        config.table_name,
        TIPLOC_SCHEMA,
    )
    written: dict[str, object] = {}

    monkeypatch.setattr(
        loader_main,
        "ARROW_RECORD_BATCH_SIZE",
        2,
    )
    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: TIPLOC_SCHEMA,
    )
    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        lambda **kwargs: SOURCE_METADATA,
    )
    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        lambda **kwargs: iter(TIPLOC_RECORDS),
    )
    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        _writer_that_materialises(written),
    )

    result = loader_main.load_schedule(
        config=config,
        load_id="1234ABC",
        storage_client=object(),  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=object(),  # pyright: ignore[reportArgumentType]
    )

    batches: list[pa.RecordBatch] = written["batches"]  # type: ignore

    assert [
        batch.num_rows
        for batch in batches
    ] == [2, 1]

    rows = [
        row
        for batch in batches
        for row in batch.to_pylist()
    ]

    assert rows[0]["load_id"] == "1234ABC"
    assert rows[0]["tiploc_code"] == "PADTON"
    assert rows[1]["description"] is None
    assert rows[2]["tiploc_code"] == "OXFD"
    assert result.row_count == 3


def test_load_schedule_shapes_schedule_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _schedule_config()
    bigquery_client = _load_client(
        config.table_name,
        SCHEDULE_SCHEMA,
    )
    written: dict[str, object] = {}

    monkeypatch.setattr(
        loader_main,
        "ARROW_RECORD_BATCH_SIZE",
        1,
    )
    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: SCHEDULE_SCHEMA,
    )
    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        lambda **kwargs: SOURCE_METADATA,
    )
    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        lambda **kwargs: iter(SCHEDULE_RECORDS),
    )
    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        _writer_that_materialises(written),
    )

    result = loader_main.load_schedule(
        config=config,
        load_id="1234ABC",
        storage_client=object(),  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=object(),  # pyright: ignore[reportArgumentType]
    )

    batches: list[pa.RecordBatch] = written["batches"]  # type: ignore
    assert [batch.num_rows for batch in batches] == [1, 1]

    rows = [
        row
        for batch in batches
        for row in batch.to_pylist()
    ]

    full_schedule = rows[0]
    assert full_schedule["load_id"] == "1234ABC"
    assert full_schedule["CIF_train_uid"] == "C00038"
    assert full_schedule["schedule_start_date"] == date(
        2026,
        5,
        17,
    )
    assert full_schedule["new_schedule_segment"] == {
        "traction_class": "",
        "uic_code": "",
    }
    assert full_schedule["schedule_segment"]["signalling_id"] == (
        "5H63"
    )
    assert (
        full_schedule["schedule_segment"]["schedule_location"]
        == [
            {
                "location_type": "LO",
                "record_identity": "LO",
                "tiploc_code": "CARLILE",
                "departure": "0905",
            }
        ]
    )

    cancellation = rows[1]
    assert cancellation["CIF_train_uid"] == "C18018"
    assert cancellation["atoc_code"] is None
    assert cancellation["applicable_timetable"] is None
    assert cancellation["new_schedule_segment"] is None
    assert (
        cancellation["schedule_segment"]["schedule_location"]
        == []
    )
    assert result.row_count == 2


def test_load_schedule_reads_exact_source_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _publication_config()
    bigquery_client = _load_client(
        config.table_name,
        PUBLICATION_SCHEMA,
    )
    metadata_call: dict[str, object] = {}

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: PUBLICATION_SCHEMA,
    )

    def fake_read_schedule_metadata(**kwargs):
        metadata_call.update(kwargs)
        return SOURCE_METADATA

    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        fake_read_schedule_metadata,
    )
    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        lambda **kwargs: iter([HEADER]),
    )
    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        lambda **kwargs: 1,
    )

    storage_client = object()

    loader_main.load_schedule(
        config=config,
        load_id="1234ABC",
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=object(),  # pyright: ignore[reportArgumentType]
    )

    assert metadata_call == {
        "client": storage_client,
        "bucket_name": config.source_bucket,
        "object_name": config.source_object,
        "generation": config.source_generation,
    }


def test_load_schedule_requests_configured_record_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _publication_config()
    bigquery_client = _load_client(
        config.table_name,
        PUBLICATION_SCHEMA,
    )
    source_call: dict[str, object] = {}

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: PUBLICATION_SCHEMA,
    )
    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        lambda **kwargs: SOURCE_METADATA,
    )

    def fake_iter_schedule_records(**kwargs):
        source_call.update(kwargs)
        return iter([HEADER])

    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        fake_iter_schedule_records,
    )
    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        lambda **kwargs: 1,
    )

    storage_client = object()

    loader_main.load_schedule(
        config=config,
        load_id="1234ABC",
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=object(),  # pyright: ignore[reportArgumentType]
    )

    assert source_call == {
        "client": storage_client,
        "bucket_name": config.source_bucket,
        "object_name": config.source_object,
        "generation": config.source_generation,
        "record_type": RecordType.PUBLICATION,
    }


def test_load_schedule_uses_source_metadata_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _publication_config()
    bigquery_client = _load_client(
        config.table_name,
        PUBLICATION_SCHEMA,
    )
    written: dict[str, object] = {}

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: PUBLICATION_SCHEMA,
    )
    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        lambda **kwargs: SourceMetadata(
            publication_sequence=7777,
            extract_type="update",
        ),
    )
    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        lambda **kwargs: iter([HEADER]),
    )
    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        _writer_that_materialises(written),
    )

    result = loader_main.load_schedule(
        config=config,
        load_id="1234ABC",
        storage_client=object(),  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=object(),  # pyright: ignore[reportArgumentType]
    )

    batches: list[pa.RecordBatch] = written["batches"]  # type: ignore
    row = batches[0].to_pylist()[0]

    assert row["publication_sequence"] == 7777
    assert row["extract_type"] == "update"
    assert result.publication_sequence == 7777
    assert result.extract_type == "update"


def test_load_schedule_returns_zero_row_count_for_empty_record_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()
    bigquery_client = _load_client(
        config.table_name,
        TIPLOC_SCHEMA,
    )

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: TIPLOC_SCHEMA,
    )
    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        lambda **kwargs: SOURCE_METADATA,
    )
    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        lambda **kwargs: iter([]),
    )
    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        lambda **kwargs: 0,
    )

    result = loader_main.load_schedule(
        config=config,
        load_id="load-empty",
        storage_client=object(),  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=object(),  # pyright: ignore[reportArgumentType]
    )

    assert result.publication_sequence == 5251
    assert result.extract_type == "full"
    assert result.row_count == 0


def test_load_schedule_returns_writer_row_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()
    bigquery_client = _load_client(
        config.table_name,
        TIPLOC_SCHEMA,
    )

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: TIPLOC_SCHEMA,
    )
    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        lambda **kwargs: SOURCE_METADATA,
    )
    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        lambda **kwargs: iter(TIPLOC_RECORDS),
    )
    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        lambda **kwargs: 123,
    )

    result = loader_main.load_schedule(
        config=config,
        load_id="1234ABC",
        storage_client=object(),  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=object(),  # pyright: ignore[reportArgumentType]
    )

    assert result.row_count == 123


def test_load_schedule_validates_schema_before_reading_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _publication_config()

    remote_schema = (
        bigquery.SchemaField(
            "wrong_field",
            "STRING",
            mode="REQUIRED",
        ),
    )
    bigquery_client = _load_client(
        config.table_name,
        remote_schema,
    )

    metadata_was_read = False
    records_were_read = False
    writer_was_called = False

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: PUBLICATION_SCHEMA,
    )

    def fake_read_schedule_metadata(**kwargs):
        nonlocal metadata_was_read
        metadata_was_read = True
        return SOURCE_METADATA

    def fake_iter_schedule_records(**kwargs):
        nonlocal records_were_read
        records_were_read = True
        return iter([HEADER])

    def fake_write_pending_batches(**kwargs):
        nonlocal writer_was_called
        writer_was_called = True
        return 1

    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        fake_read_schedule_metadata,
    )
    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        fake_iter_schedule_records,
    )
    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        fake_write_pending_batches,
    )

    with pytest.raises(SchemaMismatchError):
        loader_main.load_schedule(
            config=config,
            load_id="1234ABC",
            storage_client=object(),  # pyright: ignore[reportArgumentType]
            bigquery_client=bigquery_client,  # type: ignore
            write_client=object(),  # pyright: ignore[reportArgumentType]
        )

    assert not metadata_was_read
    assert not records_were_read
    assert not writer_was_called


def test_load_schedule_does_not_materialise_all_records_before_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()
    bigquery_client = _load_client(
        config.table_name,
        TIPLOC_SCHEMA,
    )
    written_batch: pa.RecordBatch | None = None

    monkeypatch.setattr(
        loader_main,
        "ARROW_RECORD_BATCH_SIZE",
        2,
    )
    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: TIPLOC_SCHEMA,
    )
    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        lambda **kwargs: SOURCE_METADATA,
    )

    def source_records():
        yield TIPLOC_RECORDS[0]
        yield TIPLOC_RECORDS[1]
        raise AssertionError(
            "Source was consumed beyond the first batch"
        )

    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        lambda **kwargs: source_records(),
    )

    def fake_write_pending_batches(
        *,
        client,
        table,
        batches,
    ) -> int:
        nonlocal written_batch

        written_batch = next(batches)
        assert written_batch is not None
        return written_batch.num_rows

    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        fake_write_pending_batches,
    )

    result = loader_main.load_schedule(
        config=config,
        load_id="1234ABC",
        storage_client=object(),  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=object(),  # pyright: ignore[reportArgumentType]
    )

    assert written_batch is not None
    assert written_batch.num_rows == 2
    assert result.row_count == 2


def test_generate_load_id_uses_supplied_start_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started_at = datetime(
        2026,
        10,
        26,
        21,
        30,
        15,
        123456,
        tzinfo=timezone.utc,
    )

    monkeypatch.setattr(
        loader_main.secrets,
        "token_hex",
        lambda length: "abcdef",
    )

    assert loader_main.generate_load_id(started_at) == (
        "20261026T213015.123456Z-abcdef"
    )


def test_write_load_audit_validates_schema_then_inserts_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table_id = "test-project.raw.load_audit"
    client = FakeBigQueryClient(
        {
            table_id: LOAD_AUDIT_SCHEMA,
        }
    )

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: LOAD_AUDIT_SCHEMA,
    )

    row = {
        "load_id": "load-1",
        "source_feed": "schedule",
        "record_family": "tiploc",
    }

    loader_main.write_load_audit(
        client=client,  # pyright: ignore[reportArgumentType]
        project="test-project",
        dataset="raw",
        row=row,
    )

    assert client.requested_tables == [table_id]
    assert client.insert_calls == [
        {
            "table": table_id,
            "rows": [row],
            "row_ids": ["load-1"],
        }
    ]


def test_write_load_audit_does_not_insert_when_schema_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table_id = "test-project.raw.load_audit"
    client = FakeBigQueryClient(
        {
            table_id: (
                bigquery.SchemaField(
                    "wrong_field",
                    "STRING",
                    mode="REQUIRED",
                ),
            ),
        }
    )

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: LOAD_AUDIT_SCHEMA,
    )

    with pytest.raises(SchemaMismatchError):
        loader_main.write_load_audit(
            client=client,  # pyright: ignore[reportArgumentType]
            project="test-project",
            dataset="raw",
            row={"load_id": "load-1"},
        )

    assert client.insert_calls == []


def test_write_load_audit_raises_on_insert_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table_id = "test-project.raw.load_audit"
    client = FakeBigQueryClient(
        {
            table_id: LOAD_AUDIT_SCHEMA,
        }
    )
    client.insert_errors = [
        {
            "index": 0,
            "errors": [
                {
                    "reason": "invalid",
                    "message": "bad row",
                }
            ],
        }
    ]

    monkeypatch.setattr(
        loader_main,
        "load_bigquery_schema",
        lambda path: LOAD_AUDIT_SCHEMA,
    )

    with pytest.raises(
        RuntimeError,
        match="Failed to write load audit row",
    ):
        loader_main.write_load_audit(
            client=client,  # pyright: ignore[reportArgumentType]
            project="test-project",
            dataset="raw",
            row={"load_id": "load-1"},
        )


class FakeDateTime:
    values: list[datetime] = []

    @classmethod
    def now(cls, tz) -> datetime:
        assert tz is timezone.utc
        return cls.values.pop(0)


def _patch_main_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    config: LoadConfig,
    started_at: datetime,
    finished_at: datetime,
):
    storage_client = object()
    bigquery_client = object()
    write_client = object()

    FakeDateTime.values = [
        started_at,
        finished_at,
    ]

    monkeypatch.setattr(
        loader_main,
        "datetime",
        FakeDateTime,
    )
    monkeypatch.setattr(
        loader_main,
        "load_config_from_env",
        lambda: config,
    )
    monkeypatch.setattr(
        loader_main.storage,
        "Client",
        lambda project: storage_client,
    )
    monkeypatch.setattr(
        loader_main.bigquery,
        "Client",
        lambda project: bigquery_client,
    )
    monkeypatch.setattr(
        loader_main.bigquery_storage_v1,
        "BigQueryWriteClient",
        lambda: write_client,
    )
    monkeypatch.setattr(
        loader_main,
        "generate_load_id",
        lambda timestamp: "load-1",
    )

    return storage_client, bigquery_client, write_client


def test_main_writes_success_audit_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()
    started_at = datetime(
        2026,
        10,
        26,
        21,
        30,
        tzinfo=timezone.utc,
    )
    finished_at = datetime(
        2026,
        10,
        26,
        21,
        31,
        tzinfo=timezone.utc,
    )

    (
        storage_client,
        bigquery_client,
        write_client,
    ) = _patch_main_runtime(
        monkeypatch,
        config=config,
        started_at=started_at,
        finished_at=finished_at,
    )

    load_call: dict[str, object] = {}
    audit_call: dict[str, object] = {}

    def fake_load_schedule(**kwargs):
        load_call.update(kwargs)
        return SimpleNamespace(
            publication_sequence=5251,
            extract_type="full",
            row_count=12096,
        )

    def fake_write_load_audit(**kwargs):
        audit_call.update(kwargs)

    monkeypatch.setattr(
        loader_main,
        "load_schedule",
        fake_load_schedule,
    )
    monkeypatch.setattr(
        loader_main,
        "write_load_audit",
        fake_write_load_audit,
    )

    loader_main.main()

    assert load_call == {
        "config": config,
        "load_id": "load-1",
        "storage_client": storage_client,
        "bigquery_client": bigquery_client,
        "write_client": write_client,
    }

    assert audit_call == {
        "client": bigquery_client,
        "project": "test-project",
        "dataset": "raw",
        "row": {
            "load_id": "load-1",
            "source_feed": "schedule",
            "record_family": "tiploc",
            "source_object": config.source_object,
            "source_generation": config.source_generation,
            "publication_sequence": 5251,
            "extract_type": "full",
            "status": "SUCCEEDED",
            "row_count": 12096,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "error": None,
        },
    }


def test_main_writes_failure_audit_record_and_reraises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()
    started_at = datetime(
        2026,
        10,
        26,
        21,
        30,
        tzinfo=timezone.utc,
    )
    finished_at = datetime(
        2026,
        10,
        26,
        21,
        31,
        tzinfo=timezone.utc,
    )

    _, bigquery_client, _ = _patch_main_runtime(
        monkeypatch,
        config=config,
        started_at=started_at,
        finished_at=finished_at,
    )

    failure = ValueError("source metadata invalid")
    audit_call: dict[str, object] = {}

    def fake_load_schedule(**kwargs):
        raise failure

    def fake_write_load_audit(**kwargs):
        audit_call.update(kwargs)

    monkeypatch.setattr(
        loader_main,
        "load_schedule",
        fake_load_schedule,
    )
    monkeypatch.setattr(
        loader_main,
        "write_load_audit",
        fake_write_load_audit,
    )

    with pytest.raises(ValueError) as exc_info:
        loader_main.main()

    assert exc_info.value is failure

    assert audit_call == {
        "client": bigquery_client,
        "project": "test-project",
        "dataset": "raw",
        "row": {
            "load_id": "load-1",
            "source_feed": "schedule",
            "record_family": "tiploc",
            "source_object": config.source_object,
            "source_generation": config.source_generation,
            "publication_sequence": None,
            "extract_type": None,
            "status": "FAILED",
            "row_count": None,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "error": "source metadata invalid",
        },
    }


def test_main_raises_audit_failure_when_load_succeeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()
    timestamp = datetime(
        2026,
        10,
        26,
        21,
        30,
        tzinfo=timezone.utc,
    )

    _patch_main_runtime(
        monkeypatch,
        config=config,
        started_at=timestamp,
        finished_at=timestamp,
    )

    monkeypatch.setattr(
        loader_main,
        "load_schedule",
        lambda **kwargs: SimpleNamespace(
            publication_sequence=5251,
            extract_type="full",
            row_count=3,
        ),
    )

    audit_failure = RuntimeError("audit unavailable")

    def fake_write_load_audit(**kwargs):
        raise audit_failure

    monkeypatch.setattr(
        loader_main,
        "write_load_audit",
        fake_write_load_audit,
    )

    with pytest.raises(RuntimeError) as exc_info:
        loader_main.main()

    assert exc_info.value is audit_failure


def test_main_preserves_load_failure_when_audit_write_also_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()
    timestamp = datetime(
        2026,
        10,
        26,
        21,
        30,
        tzinfo=timezone.utc,
    )

    _patch_main_runtime(
        monkeypatch,
        config=config,
        started_at=timestamp,
        finished_at=timestamp,
    )

    load_failure = ValueError("load failed")

    def fake_load_schedule(**kwargs):
        raise load_failure

    def fake_write_load_audit(**kwargs):
        raise RuntimeError("audit failed")

    monkeypatch.setattr(
        loader_main,
        "load_schedule",
        fake_load_schedule,
    )
    monkeypatch.setattr(
        loader_main,
        "write_load_audit",
        fake_write_load_audit,
    )

    with pytest.raises(ValueError) as exc_info:
        loader_main.main()

    assert exc_info.value is load_failure
