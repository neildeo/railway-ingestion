from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from google.cloud import bigquery, bigquery_storage_v1, storage
from datetime import datetime, timezone
import secrets

from schedule_loader.schema import load_bigquery_schema
from schedule_loader.source import read_schedule_header
from schedule_loader.writer import write_pending_batch


@dataclass(frozen=True)
class PublicationLoadConfig:
    source_bucket: str
    source_object: str
    source_generation: int
    project: str
    dataset: str
    table_name: str
    schema_path: Path


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

    raise NotImplementedError


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
