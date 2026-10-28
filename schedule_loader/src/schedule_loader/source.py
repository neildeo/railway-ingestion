from __future__ import annotations

from typing import Any, Iterator
from enum import StrEnum
from dataclasses import dataclass

from google.cloud import storage
import gzip
import json


class RecordType(StrEnum):
    PUBLICATION = "JsonTimetableV1"
    TIPLOC = "TiplocV1"
    ASSOCIATION = "JsonAssociationV1"
    SCHEDULE = "JsonScheduleV1"


@dataclass(frozen=True)
class SourceMetadata:
    publication_sequence: int
    extract_type: str


def read_schedule_metadata(
    *,
    client: storage.Client,
    bucket_name: str,
    object_name: str,
    generation: int,
) -> SourceMetadata:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name, generation=generation)
    blob.reload()

    metadata = blob.metadata
    if not metadata:
        raise ValueError(
            f"Object {object_name} does not exist or has no metadata")

    try:
        sequence = int(metadata["source_sequence"])
        file_type = metadata["source_type"]
    except KeyError as e:
        raise ValueError(
            f"Object {object_name} does not have required metadata") from e

    return SourceMetadata(
        publication_sequence=sequence,
        extract_type=file_type,
    )


def iter_schedule_records(
    *,
    client: storage.Client,
    bucket_name: str,
    object_name: str,
    generation: int,
    record_type: RecordType,
) -> Iterator[dict[str, Any]]:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name, generation=generation)

    with blob.open(mode="rb") as raw_stream:
        with gzip.GzipFile(
            fileobj=raw_stream  # pyright: ignore[reportArgumentType]
        ) as f:
            record_type_seen = False

            for line in f:
                record: dict[str, Any] = json.loads(line)
                key = list(record.keys())[0]

                if key == record_type:
                    record_type_seen = True
                    yield record[key]
                elif record_type_seen:
                    return
