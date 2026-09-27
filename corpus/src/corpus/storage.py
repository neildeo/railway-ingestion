from corpus.core import ObjectState
from google.cloud import storage


def get_raw_bucket_name() -> str:
    # return "railway-analytics-508615-network-rail-open-data-raw"
    raise NotImplementedError


def crc32c_checksum(data: bytes) -> str:
    raise NotImplementedError


def get_object_state(name: str) -> ObjectState | None:
    raise NotImplementedError


def upload_object(
    name: str,
    data: bytes,
    expected_generation: int,
) -> None:
    raise NotImplementedError
