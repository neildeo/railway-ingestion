from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from google.cloud import bigquery
from datetime import date

import schedule_loader.__main__ as loader_main
from schedule_loader.__main__ import load_schedule
from schedule_loader.config import LoadConfig
from schedule_loader.schema import SchemaMismatchError
from schedule_loader.source import (
    RecordType,
    SourceMetadata,
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
        "base_location_suffix",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "diagram_type",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "location",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "main_train_uid",
        "STRING",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "assoc_days",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "assoc_end_date",
        "DATE",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "assoc_location_suffix",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "category",
        "STRING",
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        "date_indicator",
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


SOURCE_METADATA = SourceMetadata(
    publication_sequence=5251,
    extract_type="full",
)


def _association_config() -> LoadConfig:
    return LoadConfig(
        source_bucket="railway-raw",
        source_object=(
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
        source_generation=123456789,
        project="test-project",
        dataset="raw",
        record_type=RecordType.ASSOCIATION,
        table_name="schedule_associations",
        schema_path=Path(
            "/schemas/schedule_associations.json"
        ),
    )


def _publication_config() -> LoadConfig:
    return LoadConfig(
        source_bucket="railway-raw",
        source_object=(
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
        source_generation=123456789,
        project="test-project",
        dataset="raw",
        record_type=RecordType.PUBLICATION,
        table_name="schedule_publications",
        schema_path=Path(
            "/schemas/schedule_publications.json"
        ),
    )


def _tiploc_config() -> LoadConfig:
    return LoadConfig(
        source_bucket="railway-raw",
        source_object=(
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
        source_generation=123456789,
        project="test-project",
        dataset="raw",
        record_type=RecordType.TIPLOC,
        table_name="schedule_tiplocs",
        schema_path=Path(
            "/schemas/schedule_tiplocs.json"
        ),
    )


class FakeBigQueryClient:
    def __init__(
        self,
        schema: tuple[bigquery.SchemaField, ...],
    ) -> None:
        self.schema = schema
        self.requested_table: str | None = None

    def get_table(self, table: str):
        self.requested_table = table

        return SimpleNamespace(
            schema=self.schema,
        )


def test_load_schedule_shapes_publication_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _publication_config()

    storage_client = object()
    write_client = object()
    bigquery_client = FakeBigQueryClient(
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
        "generate_load_id",
        lambda: "load-1",
    )

    def fake_write_pending_batches(
        *,
        client,
        table: str,
        batches,
    ) -> None:
        written["client"] = client
        written["table"] = table
        written["batches"] = list(batches)

    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        fake_write_pending_batches,
    )

    load_id = load_schedule(
        config=config,
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=write_client,  # pyright: ignore[reportArgumentType]
    )

    assert load_id == "load-1"

    assert bigquery_client.requested_table == (
        "test-project.raw.schedule_publications"
    )

    assert written["client"] is write_client
    assert written["table"] == (
        "projects/test-project/"
        "datasets/raw/"
        "tables/schedule_publications"
    )

    batches: list[pa.RecordBatch] = written["batches"]  # type: ignore

    assert len(batches) == 1
    assert batches[0].num_rows == 1

    row = batches[0].to_pylist()[0]

    assert row["load_id"] == "load-1"
    assert row["source_object"] == config.source_object
    assert (
        row["source_generation"]
        == config.source_generation
    )
    assert row["publication_sequence"] == 5251
    assert row["extract_type"] == "full"

    assert row["sender_application"] == "NTROD"
    assert row["sender_component"] == "SCHEDULE"
    assert row["sender_organisation"] == "Rockshore"
    assert row["classification"] == "public"
    assert row["owner"] == "Network Rail"


def test_load_schedule_shapes_association_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _association_config()

    storage_client = object()
    write_client = object()
    bigquery_client = FakeBigQueryClient(
        ASSOCIATION_SCHEMA,
    )

    written_batches: list[pa.RecordBatch] = []

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
        "generate_load_id",
        lambda: "load-1",
    )

    def fake_write_pending_batches(
        *,
        client,
        table,
        batches,
    ) -> None:
        written_batches.extend(batches)

    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        fake_write_pending_batches,
    )

    load_id = load_schedule(
        config=config,
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=write_client,  # pyright: ignore[reportArgumentType]
    )

    assert load_id == "load-1"

    rows = [
        row
        for batch in written_batches
        for row in batch.to_pylist()
    ]

    assert len(rows) == 2

    assert all(
        row["load_id"] == "load-1"
        for row in rows
    )

    assert rows[0]["main_train_uid"] == "C30123"
    assert rows[0]["assoc_train_uid"] == "C42949"
    assert rows[0]["assoc_start_date"] == date(
        2024,
        6,
        3,
    )
    assert rows[0]["assoc_end_date"] == date(
        2024,
        6,
        21,
    )


def test_load_schedule_shapes_and_batches_tiploc_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()

    storage_client = object()
    write_client = object()
    bigquery_client = FakeBigQueryClient(
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
        "generate_load_id",
        lambda: "load-1",
    )

    def fake_write_pending_batches(
        *,
        client,
        table: str,
        batches,
    ) -> None:
        written["client"] = client
        written["table"] = table
        written["batches"] = list(batches)

    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        fake_write_pending_batches,
    )

    load_id = load_schedule(
        config=config,
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=write_client,  # pyright: ignore[reportArgumentType]
    )

    assert load_id == "load-1"

    assert bigquery_client.requested_table == (
        "test-project.raw.schedule_tiplocs"
    )

    assert written["client"] is write_client
    assert written["table"] == (
        "projects/test-project/"
        "datasets/raw/"
        "tables/schedule_tiplocs"
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

    assert rows == [
        {
            "load_id": "load-1",
            "source_object": config.source_object,
            "source_generation": (
                config.source_generation
            ),
            "publication_sequence": 5251,
            "extract_type": "full",
            "transaction_type": "Create",
            "tiploc_code": "PADTON",
            "tps_description": "LONDON PADDINGTON",
            "description": "PADDINGTON",
            "crs_code": "PAD",
            "stanox": "33057",
            "nalco": "308700",
        },
        {
            "load_id": "load-1",
            "source_object": config.source_object,
            "source_generation": (
                config.source_generation
            ),
            "publication_sequence": 5251,
            "extract_type": "full",
            "transaction_type": "Create",
            "tiploc_code": "RDNGSTN",
            "tps_description": "READING",
            "description": None,
            "crs_code": None,
            "stanox": None,
            "nalco": "314900",
        },
        {
            "load_id": "load-1",
            "source_object": config.source_object,
            "source_generation": (
                config.source_generation
            ),
            "publication_sequence": 5251,
            "extract_type": "full",
            "transaction_type": "Create",
            "tiploc_code": "OXFD",
            "tps_description": "OXFORD",
            "description": "OXFORD",
            "crs_code": "OXF",
            "stanox": "31105",
            "nalco": "311500",
        },
    ]


def test_load_schedule_reads_exact_source_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _publication_config()

    storage_client = object()
    write_client = object()
    bigquery_client = FakeBigQueryClient(
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
        "generate_load_id",
        lambda: "load-1",
    )

    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        lambda **kwargs: list(kwargs["batches"]),
    )

    load_schedule(
        config=config,
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=write_client,  # pyright: ignore[reportArgumentType]
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

    storage_client = object()
    write_client = object()
    bigquery_client = FakeBigQueryClient(
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
        "generate_load_id",
        lambda: "load-1",
    )

    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        lambda **kwargs: list(kwargs["batches"]),
    )

    load_schedule(
        config=config,
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=write_client,  # pyright: ignore[reportArgumentType]
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

    storage_client = object()
    write_client = object()
    bigquery_client = FakeBigQueryClient(
        PUBLICATION_SCHEMA,
    )

    written_batches: list[pa.RecordBatch] = []

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
        "generate_load_id",
        lambda: "load-1",
    )

    def fake_write_pending_batches(
        *,
        client,
        table,
        batches,
    ) -> None:
        written_batches.extend(batches)

    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        fake_write_pending_batches,
    )

    load_schedule(
        config=config,
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=write_client,  # pyright: ignore[reportArgumentType]
    )

    assert len(written_batches) == 1

    row = written_batches[0].to_pylist()[0]

    assert row["publication_sequence"] == 7777
    assert row["extract_type"] == "update"


def test_load_schedule_returns_load_id_for_empty_record_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()

    storage_client = object()
    write_client = object()
    bigquery_client = FakeBigQueryClient(
        TIPLOC_SCHEMA,
    )

    written_batches: list[pa.RecordBatch] = []

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
        "generate_load_id",
        lambda: "load-empty",
    )

    def fake_write_pending_batches(
        *,
        client,
        table,
        batches,
    ) -> None:
        written_batches.extend(batches)

    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        fake_write_pending_batches,
    )

    load_id = load_schedule(
        config=config,
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=write_client,  # pyright: ignore[reportArgumentType]
    )

    assert load_id == "load-empty"
    assert written_batches == []


def test_load_schedule_validates_schema_before_reading_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _publication_config()

    storage_client = object()
    write_client = object()

    remote_schema = (
        bigquery.SchemaField(
            "wrong_field",
            "STRING",
            mode="REQUIRED",
        ),
    )

    bigquery_client = FakeBigQueryClient(
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

    monkeypatch.setattr(
        loader_main,
        "read_schedule_metadata",
        fake_read_schedule_metadata,
    )

    def fake_iter_schedule_records(**kwargs):
        nonlocal records_were_read
        records_were_read = True
        return iter([HEADER])

    monkeypatch.setattr(
        loader_main,
        "iter_schedule_records",
        fake_iter_schedule_records,
    )

    def fake_write_pending_batches(**kwargs):
        nonlocal writer_was_called
        writer_was_called = True

    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        fake_write_pending_batches,
    )

    with pytest.raises(SchemaMismatchError):
        load_schedule(
            config=config,
            storage_client=storage_client,  # type: ignore
            bigquery_client=bigquery_client,  # type: ignore
            write_client=write_client,  # pyright: ignore[reportArgumentType]
        )

    assert not metadata_was_read
    assert not records_were_read
    assert not writer_was_called


def test_load_schedule_does_not_materialise_all_records_before_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _tiploc_config()

    storage_client = object()
    write_client = object()
    bigquery_client = FakeBigQueryClient(
        TIPLOC_SCHEMA,
    )

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

    monkeypatch.setattr(
        loader_main,
        "generate_load_id",
        lambda: "load-1",
    )

    written_batch: pa.RecordBatch | None = None

    def fake_write_pending_batches(
        *,
        client,
        table,
        batches,
    ) -> None:
        nonlocal written_batch

        written_batch = next(batches)

        # Deliberately do not request another batch.
        # This verifies that upstream records are consumed
        # only as the writer advances the batch iterator.

    monkeypatch.setattr(
        loader_main,
        "write_pending_batches",
        fake_write_pending_batches,
    )

    load_id = load_schedule(
        config=config,
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=write_client,  # pyright: ignore[reportArgumentType]
    )

    assert load_id == "load-1"
    assert written_batch is not None
    assert written_batch.num_rows == 2
