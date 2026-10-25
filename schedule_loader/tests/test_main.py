from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from google.cloud import bigquery

import schedule_loader.__main__ as loader_main
from schedule_loader.__main__ import load_schedule
from schedule_loader.config import LoadConfig
from schedule_loader.schema import SchemaMismatchError
from schedule_loader.source import (
    RecordType,
    SourceMetadata,
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
]


SOURCE_METADATA = SourceMetadata(
    publication_sequence=5251,
    extract_type="full",
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

    def fake_write_pending_batch(
        *,
        client,
        table: str,
        batch: pa.RecordBatch,
    ) -> None:
        written["client"] = client
        written["table"] = table
        written["batch"] = batch

    monkeypatch.setattr(
        loader_main,
        "write_pending_batch",
        fake_write_pending_batch,
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

    batch: pa.RecordBatch = written["batch"]

    assert batch.num_rows == 1

    row = batch.to_pylist()[0]

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


def test_load_schedule_shapes_tiploc_records(
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

    def fake_write_pending_batch(
        *,
        client,
        table: str,
        batch: pa.RecordBatch,
    ) -> None:
        written["client"] = client
        written["table"] = table
        written["batch"] = batch

    monkeypatch.setattr(
        loader_main,
        "write_pending_batch",
        fake_write_pending_batch,
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

    batch: pa.RecordBatch = written["batch"]

    assert batch.num_rows == 2

    assert batch.to_pylist() == [
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
        "write_pending_batch",
        lambda **kwargs: None,
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
        "write_pending_batch",
        lambda **kwargs: None,
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

    written_batch: pa.RecordBatch | None = None

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

    def fake_write_pending_batch(
        *,
        client,
        table,
        batch,
    ) -> None:
        nonlocal written_batch
        written_batch = batch

    monkeypatch.setattr(
        loader_main,
        "write_pending_batch",
        fake_write_pending_batch,
    )

    load_schedule(
        config=config,
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        write_client=write_client,  # pyright: ignore[reportArgumentType]
    )

    assert written_batch is not None

    row = written_batch.to_pylist()[0]

    assert row["publication_sequence"] == 7777
    assert row["extract_type"] == "update"


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

    def fake_write_pending_batch(**kwargs):
        nonlocal writer_was_called
        writer_was_called = True

    monkeypatch.setattr(
        loader_main,
        "write_pending_batch",
        fake_write_pending_batch,
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
