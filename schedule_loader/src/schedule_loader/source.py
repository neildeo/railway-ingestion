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
    ASSOCIATION = "AssociationV1"
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
    raise NotImplementedError


def iter_schedule_records(
    *,
    client: storage.Client,
    bucket_name: str,
    object_name: str,
    generation: int,
    record_type: RecordType,
) -> Iterator[dict[str, Any]]:
    raise NotImplementedError


def read_schedule_header(
    *,
    client: storage.Client,
    bucket_name: str,
    object_name: str,
    generation: int,
) -> dict[str, Any]:
    """
    Open one exact immutable GCS object generation and return its
    JsonTimetableV1 header record, the first line of the NDJSON file.
    """
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name, generation=generation)

    with blob.open(mode="rb") as raw_stream:
        with gzip.GzipFile(
            fileobj=raw_stream  # pyright: ignore[reportArgumentType]
        ) as f:
            line_bytes = f.readline()

    if not line_bytes:
        raise ValueError("Object is empty")

    try:
        return json.loads(line_bytes)['JsonTimetableV1']
    except KeyError as e:
        raise ValueError(
            "Expected JsonTimetableV1 record but no such record was found"
        ) from e
