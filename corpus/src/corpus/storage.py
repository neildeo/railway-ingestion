from corpus.core import ObjectState
from google.cloud import storage
import google_crc32c
import base64
from google.api_core import exceptions as google_exceptions
from typing import cast
from google.api_core.retry import Retry
from google.cloud.storage.retry import DEFAULT_RETRY_IF_GENERATION_SPECIFIED


def get_raw_bucket_name() -> str:
    return "railway-analytics-508615-network-rail-open-data-raw"


def crc32c_checksum(data: bytes) -> str:
    value = google_crc32c.value(data)

    checksum = base64.b64encode(
        value.to_bytes(4, byteorder="big")
    ).decode("ascii")

    return checksum


def get_object_state(name: str) -> ObjectState | None:
    client = storage.Client()
    bucket = client.bucket(get_raw_bucket_name())
    blob = bucket.blob(name)

    try:
        blob.reload()
    except google_exceptions.NotFound:
        return None

    if blob.crc32c is None:
        raise ValueError("GCS object has no CRC32C metadata")

    if blob.generation is None:
        raise ValueError("GCS object has no generation metadata")

    return ObjectState(
        crc32c=blob.crc32c,
        generation=blob.generation,
    )


def upload_object(
    name: str,
    data: bytes,
    expected_generation: int,
) -> None:
    client = storage.Client()
    bucket = client.bucket(get_raw_bucket_name())
    blob = bucket.blob(name)

    blob.upload_from_string(
        data,
        if_generation_match=expected_generation,
        retry=cast(Retry, DEFAULT_RETRY_IF_GENERATION_SPECIFIED),
    )
