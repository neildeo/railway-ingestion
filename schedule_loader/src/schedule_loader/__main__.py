from __future__ import annotations
from google.cloud import bigquery, bigquery_storage_v1, storage
from datetime import datetime, timezone
import secrets

from schedule_loader.config import LoadConfig
from schedule_loader.schema import load_bigquery_schema, assert_schemas_equal
from schedule_loader.arrow import batch_rows_as_arrow
from schedule_loader.source import read_schedule_metadata, iter_schedule_records, RecordType
from schedule_loader.row import LoadContext, attach_load_context, publication_row, tiploc_row
from schedule_loader.writer import write_pending_batches


def load_schedule(
    *,
    config: LoadConfig,
    storage_client: storage.Client,
    bigquery_client: bigquery.Client,
    write_client: bigquery_storage_v1.BigQueryWriteClient,
) -> str:
    bigquery_table_id = (
        f"{config.project}."
        f"{config.dataset}."
        f"{config.table_name}"
    )

    storage_write_table = (
        f"projects/{config.project}/"
        f"datasets/{config.dataset}/"
        f"tables/{config.table_name}"
    )

    # Get both schemas and check schema match
    local_schema = load_bigquery_schema(config.schema_path)
    remote_schema = bigquery_client.get_table(bigquery_table_id).schema

    assert_schemas_equal(expected=local_schema, actual=remote_schema)

    # Get object metadata
    metadata = read_schedule_metadata(
        client=storage_client,
        bucket_name=config.source_bucket,
        object_name=config.source_object,
        generation=config.source_generation,
    )

    # Generate load context
    load_context = LoadContext(
        load_id=generate_load_id(),
        source_object=config.source_object,
        source_generation=config.source_generation,
        publication_sequence=metadata.publication_sequence,
        extract_type=metadata.extract_type,
    )

    # Get header
    records = iter_schedule_records(
        client=storage_client,
        bucket_name=config.source_bucket,
        object_name=config.source_object,
        generation=config.source_generation,
        record_type=config.record_type,
    )

    # Convert to rows object and attach load context
    match config.record_type:
        case RecordType.PUBLICATION:
            rows = [publication_row(r) for r in records]
        case RecordType.TIPLOC:
            rows = [tiploc_row(r) for r in records]
        case _:
            raise NotImplementedError(
                f"Record type {config.record_type} not recognised")
    rows_with_ctx = [attach_load_context(
        r, context=load_context) for r in rows]

    # Prepare RecordBatch for BQ write
    # Already confirmed the local schema matches the landing table
    batch = batch_rows_as_arrow(
        rows_with_ctx,
        bq_schema=local_schema,
    )

    # Write to BQ
    write_pending_batches(
        client=write_client,
        table=storage_write_table,
        batches=[batch].__iter__(),
    )

    return load_context.load_id


def generate_load_id() -> str:
    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%S.%fZ")

    suffix = secrets.token_hex(3)

    return f"{timestamp}-{suffix}"


def main() -> None:
    """
    Load one SCHEDULE record family from an exact immutable source object.

    Phase 1 implements publications only.
    """
    raise NotImplementedError


if __name__ == "__main__":
    main()
