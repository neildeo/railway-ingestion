from __future__ import annotations
from typing import Any
from google.cloud import bigquery, bigquery_storage_v1, storage
from datetime import UTC, datetime
import secrets
from functools import partial
from dataclasses import dataclass
from pathlib import Path
import logging

from schedule_loader.config import AUDIT_SCHEMA_PATH, LoadConfig, load_config_from_env
from schedule_loader.schema import load_bigquery_schema, assert_schemas_equal
from schedule_loader.arrow import iter_rows_as_arrow_batches
from schedule_loader.source import read_schedule_metadata, iter_schedule_records, RecordType
from schedule_loader.row import LoadContext, attach_load_context, publication_row, tiploc_row, association_row, schedule_row
from schedule_loader.writer import write_pending_batches
from schedule_loader.logging_config import configure_logging

logger = logging.getLogger(__name__)

ARROW_RECORD_BATCH_SIZE = 1000


@dataclass(frozen=True)
class LoadResult:
    publication_sequence: int
    extract_type: str
    row_count: int


def load_schedule(
    *,
    config: LoadConfig,
    load_id: str,
    storage_client: storage.Client,
    bigquery_client: bigquery.Client,
    write_client: bigquery_storage_v1.BigQueryWriteClient,
) -> LoadResult:
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
        load_id=load_id,
        source_object=config.source_object,
        source_generation=config.source_generation,
        publication_sequence=metadata.publication_sequence,
        extract_type=metadata.extract_type,
    )

    # Get records from bucket
    records = iter_schedule_records(
        client=storage_client,
        bucket_name=config.source_bucket,
        object_name=config.source_object,
        generation=config.source_generation,
        record_type=config.record_type,
    )

    # Convert to rows object and attach load context
    match config.record_type:
        case RecordType.ASSOCIATION:
            rows = map(association_row, records)
        case RecordType.PUBLICATION:
            rows = map(publication_row, records)
        case RecordType.SCHEDULE:
            rows = map(schedule_row, records)
        case RecordType.TIPLOC:
            rows = map(tiploc_row, records)
        case _:
            raise NotImplementedError(
                f"Record type {config.record_type} not recognised")

    attach_ctx = partial(attach_load_context, context=load_context)
    rows_with_ctx = map(attach_ctx, rows)

    # Prepare RecordBatch for BQ write
    # Already confirmed the local schema matches the landing table
    batches = iter_rows_as_arrow_batches(
        rows_with_ctx,
        bq_schema=local_schema,
        batch_size=ARROW_RECORD_BATCH_SIZE,
    )

    # Write to BQ
    rows_written = write_pending_batches(
        client=write_client,
        table=storage_write_table,
        batches=batches,
    )

    return LoadResult(
        publication_sequence=metadata.publication_sequence,
        extract_type=metadata.extract_type,
        row_count=rows_written,
    )


def generate_load_id(timestamp: datetime) -> str:
    timestamp_str = timestamp.astimezone(UTC).strftime("%Y%m%dT%H%M%S.%fZ")

    suffix = secrets.token_hex(3)

    return f"{timestamp_str}-{suffix}"


def write_load_audit(
    client: bigquery.Client,
    schema_path: Path,
    project: str,
    dataset: str,
    row: dict[str, Any],
):
    """
    Checks load audit schema matches local contract, then writes an audit
    row to the table.
    """
    bigquery_table_id = f"{project}.{dataset}.load_audit"

    local_schema = load_bigquery_schema(schema_path)
    remote_schema = client.get_table(bigquery_table_id).schema

    assert_schemas_equal(expected=local_schema, actual=remote_schema)

    insert_errors = client.insert_rows_json(
        bigquery_table_id,
        [row],
        row_ids=[row["load_id"]],
    )

    if insert_errors:
        raise RuntimeError(
            f"Failed to write load audit row for load {row["load_id"]}")


def main() -> None:
    """
    Load one SCHEDULE record family from an exact immutable source object.
    """
    configure_logging()

    config = load_config_from_env()
    start_time = datetime.now(UTC)
    load_id = generate_load_id(start_time)

    storage_client = storage.Client(project=config.project)
    bigquery_client = bigquery.Client(project=config.project)
    write_client = bigquery_storage_v1.BigQueryWriteClient()

    logger.info(
        "SCHEDULE load started",
        extra={
            "event": "schedule_load_started",
            "load_id": load_id,
            "source_feed": "schedule",
            "record_family": config.record_type.name.lower(),
            "source_object": config.source_object,
            "source_generation": config.source_generation,
            "dataset": config.dataset,
            "table": config.table_name,
        },
    )

    try:
        load_result = load_schedule(
            config=config,
            load_id=load_id,
            storage_client=storage_client,
            bigquery_client=bigquery_client,
            write_client=write_client
        )
    except Exception as load_exc:
        logger.exception(
            "SCHEDULE load failed",
            extra={
                "event": "schedule_load_failed",
                "load_id": load_id,
                "source_feed": "schedule",
                "record_family": config.record_type.name.lower(),
                "source_object": config.source_object,
                "source_generation": config.source_generation,
                "dataset": config.dataset,
                "table": config.table_name,
            },
        )

        # Write failure audit row
        audit_row = {
            "load_id": load_id,
            "source_feed": "schedule",
            "record_family": config.record_type.name.lower(),
            "source_object": config.source_object,
            "source_generation": config.source_generation,
            "publication_sequence": None,
            "extract_type": None,
            "status": "FAILED",
            "row_count": None,
            "started_at": start_time.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "error": str(load_exc),
        }

        try:
            write_load_audit(
                client=bigquery_client,
                schema_path=AUDIT_SCHEMA_PATH,
                project=config.project,
                dataset=config.dataset,
                row=audit_row,
            )
        except Exception:
            logger.exception(
                "Failed to write audit record for failed load",
                extra={
                    "event": "schedule_load_audit_write_failed",
                    "load_id": load_id,
                    "record_family": config.record_type.name.lower(),
                    "load_status": "FAILED",
                },
            )
        raise

    # Success audit row
    audit_row = {
        "load_id": load_id,
        "source_feed": "schedule",
        "record_family": config.record_type.name.lower(),
        "source_object": config.source_object,
        "source_generation": config.source_generation,
        "publication_sequence": load_result.publication_sequence,
        "extract_type": load_result.extract_type,
        "status": "SUCCEEDED",
        "row_count": load_result.row_count,
        "started_at": start_time.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "error": None,
    }

    try:
        write_load_audit(
            client=bigquery_client,
            schema_path=AUDIT_SCHEMA_PATH,
            project=config.project,
            dataset=config.dataset,
            row=audit_row,
        )
    except Exception:
        logger.exception(
            "Failed to write audit row for successful SCHEDULE load",
            extra={
                "event": "schedule_load_audit_write_failed",
                "load_id": load_id,
                "record_family": config.record_type.name.lower(),
                "load_status": "SUCCEEDED",
            },
        )
        raise

    logger.info(
        "SCHEDULE load completed successfully",
        extra={
            "event": "schedule_load_completed",
            "load_id": load_id,
            "source_feed": "schedule",
            "record_family": config.record_type.name.lower(),
            "publication_sequence": load_result.publication_sequence,
            "extract_type": load_result.extract_type,
            "row_count": load_result.row_count,
            "dataset": config.dataset,
            "table": config.table_name,
        },
    )


if __name__ == "__main__":
    main()
