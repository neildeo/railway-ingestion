from __future__ import annotations

from schedule.core import (
    SchedulePublicationInfo,
    ScheduleRequest,
    FullSnapshotRequest,
    UpdateRequest,
    StoredObjectState,
    SourceMetadata,
)

import requests
import zlib
import json
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from google.cloud import storage


MAX_RETRIES = 5

RETRYABLE_STATUS_CODES = {
    429,
    500,
    502,
    503,
    504,
}

MAX_HEADER_BYTES = 1_024 * 1_024  # 1 MiB


def create_requests_session() -> requests.Session:
    retry_policy = Retry(
        total=MAX_RETRIES,
        allowed_methods=frozenset({"GET"}),
        status_forcelist=RETRYABLE_STATUS_CODES,
    )
    s = requests.Session()
    s.mount(
        prefix="https://",
        adapter=HTTPAdapter(max_retries=retry_policy),
    )

    return s


def get_endpoint_url(schedule_request: ScheduleRequest) -> str:
    match schedule_request:
        case FullSnapshotRequest():
            return "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full"
        case UpdateRequest(day):
            return f"https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day={day.value}"


def fetch_header_row(
    schedule_request: ScheduleRequest,
) -> SchedulePublicationInfo | None:
    """
    Make the short probe request.

    Intended responsibilities:
    - build the Network Rail URL for `schedule_request`
    - authenticate and follow the redirect to S3
    - capture S3 response metadata
    - consume only enough compressed bytes to obtain the first NDJSON row
    - partially decompress and parse JsonTimetableV1
    - close the HTTP response before returning

    Return None only for the normal "publication not available yet" outcome.
    Transport/auth/parsing failures should raise.
    """
    url = get_endpoint_url(schedule_request)
    session = create_requests_session()

    # Need to inject auth info here

    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    buffer = bytearray()
    header_bytes = bytes()

    with session.get(url, stream=True) as response:
        # Grab header info - how?
        for compressed_chunk in response.iter_content(chunk_size=1_024):
            if not compressed_chunk:
                continue

            buffer.extend(decompressor.decompress(compressed_chunk))

            newline = buffer.find(b"\n")

            if newline != -1:
                if newline > MAX_HEADER_BYTES:
                    raise ValueError(...)
                header_bytes = bytes(buffer[:newline])
                break

            if len(buffer) > MAX_HEADER_BYTES:
                raise ValueError(...)

    pub_info = json.loads(header_bytes)


def get_object_state(object_name: str) -> StoredObjectState | None:
    """
    Return the existing GCS object's stored source provenance and generation,
    or None when the object does not exist.
    """
    raise NotImplementedError


def upload_schedule(
    schedule_request: ScheduleRequest,
    object_name: str,
    expected_metadata: SchedulePublicationInfo,
) -> None:
    """
    Perform a fresh full source request and stream the exact compressed bytes
    into the normal GCS object.

    Before transferring the body, verify the new publication info and S3
    response metadata still match `expected_metadata`.

    The final GCS write should be create-only (`if_generation_match=0`).
    """
    raise NotImplementedError


def quarantine_schedule(
    schedule_request: ScheduleRequest,
    object_name: str,
    expected_metadata: SchedulePublicationInfo,
) -> None:
    """
    Perform a fresh full source request and preserve the conflicting artefact
    under the supplied quarantine object name.

    The new source response metadata should still be checked against the probe
    metadata before the transfer begins.
    """
    raise NotImplementedError
