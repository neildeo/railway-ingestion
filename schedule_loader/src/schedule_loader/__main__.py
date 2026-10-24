from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from google.cloud import bigquery, bigquery_storage_v1, storage
from datetime import datetime, timezone
import secrets

from schedule_loader.config import PublicationLoadConfig
from schedule_loader.schema import load_bigquery_schema, assert_schemas_equal
from schedule_loader.arrow import batch_rows_as_arrow
from schedule_loader.source import read_schedule_header
from schedule_loader.row import LoadContext, attach_load_context, publication_row
from schedule_loader.writer import write_pending_batch


def load_schedule_publication(
    *,
    config: PublicationLoadConfig,
    storage_client: storage.Client,
    bigquery_client: bigquery.Client,
    write_client: bigquery_storage_v1.BigQueryWriteClient,
) -> None:
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

    # Get header
    publication_header = read_schedule_header(
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
        publication_sequence=publication_header['Metadata']['sequence'],
        extract_type=publication_header['Metadata']['type'],
    )

    # Convert to row object and attach load context
    row = attach_load_context(
        publication_row(publication_header),
        context=load_context,
    )

    # Prepare RecordBatch for BQ write
    # Already confirmed the local schema matches the landing table
    batch = batch_rows_as_arrow(
        [row],
        bq_schema=local_schema,
    )

    # Write to BQ
    write_pending_batch(
        client=write_client,
        table=storage_write_table,
        batch=batch,
    )


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
