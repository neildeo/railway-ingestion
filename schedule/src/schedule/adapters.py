from __future__ import annotations

from schedule.core import (
    SchedulePublicationInfo,
    ScheduleRequest,
    FullSnapshotRequest,
    UpdateRequest,
    StoredObjectState,
    ScheduleHeader,
    SourceMetadata,
    ExtractType,
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

_MAX_HEADER_BYTES = 1_024 * 1_024  # 1 MiB

_MAX_PROBE_ATTEMPTS = 3

_PROBE_CHUNK_SIZE = 1024

_TRANSIENT_REQUEST_ERRORS = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


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
) -> SchedulePublicationInfo:
    url = get_endpoint_url(schedule_request)
    session = create_requests_session()

    for attempt in range(_MAX_PROBE_ATTEMPTS):
        try:
            with session.get(
                url,
                stream=True,
            ) as response:
                response.raise_for_status()

                source_metadata = (
                    _extract_source_metadata(
                        response
                    )
                )

                header_bytes = (
                    _read_header_bytes(
                        response
                    )
                )

            return SchedulePublicationInfo(
                header=_parse_schedule_header(
                    header_bytes
                ),
                source_metadata=source_metadata,
            )

        except _TRANSIENT_REQUEST_ERRORS:
            if attempt == _MAX_PROBE_ATTEMPTS - 1:
                raise

    raise AssertionError("unreachable")


def _extract_source_metadata(
    response: requests.Response,
) -> SourceMetadata:
    try:
        etag = response.headers["ETag"]
        last_modified = response.headers["Last-Modified"]
        content_length_raw = response.headers[
            "Content-Length"
        ]
    except KeyError as exc:
        raise ValueError(
            f"Required source header missing: {exc.args[0]}"
        ) from exc

    try:
        content_length = int(content_length_raw)
    except ValueError as exc:
        raise ValueError(
            "Source Content-Length is not an integer"
        ) from exc

    return SourceMetadata(
        etag=etag,
        last_modified=last_modified,
        content_length=content_length,
    )


def _read_header_bytes(
    response: requests.Response,
) -> bytes:
    decompressor = zlib.decompressobj(
        16 + zlib.MAX_WBITS
    )
    buffer = bytearray()

    for compressed_chunk in response.iter_content(
        chunk_size=_PROBE_CHUNK_SIZE
    ):
        if not compressed_chunk:
            continue

        try:
            buffer.extend(
                decompressor.decompress(
                    compressed_chunk
                )
            )
        except zlib.error as exc:
            raise ValueError(
                "SCHEDULE response is not valid gzip"
            ) from exc

        newline = buffer.find(b"\n")

        if newline != -1:
            if newline > _MAX_HEADER_BYTES:
                raise ValueError(
                    "SCHEDULE header exceeds maximum size"
                )

            return bytes(buffer[:newline])

        if len(buffer) > _MAX_HEADER_BYTES:
            raise ValueError(
                "SCHEDULE header exceeds maximum size"
            )

    raise ValueError(
        "SCHEDULE response ended before first NDJSON row"
    )


def _parse_schedule_header(
    header_bytes: bytes,
) -> ScheduleHeader:
    try:
        parsed = json.loads(header_bytes)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "SCHEDULE header is not valid JSON"
        ) from exc

    if not isinstance(parsed, dict):
        raise ValueError(
            "SCHEDULE header must be a JSON object"
        )

    try:
        timetable = parsed["JsonTimetableV1"]
        metadata = timetable["Metadata"]

        extract_type_raw = metadata["type"]
        sequence = metadata["sequence"]
        timestamp = timetable["timestamp"]

    except (KeyError, TypeError) as exc:
        raise ValueError(
            "SCHEDULE header does not have "
            "the expected structure"
        ) from exc

    if not isinstance(extract_type_raw, str):
        raise ValueError(
            "SCHEDULE extract type must be a string"
        )

    if not isinstance(sequence, int):
        raise ValueError(
            "SCHEDULE sequence must be an integer"
        )

    if not isinstance(timestamp, int):
        raise ValueError(
            "SCHEDULE timestamp must be an integer"
        )

    try:
        extract_type = ExtractType(
            extract_type_raw
        )
    except ValueError as exc:
        raise ValueError(
            f"Unknown SCHEDULE extract type: "
            f"{extract_type_raw!r}"
        ) from exc

    return ScheduleHeader(
        extract_type=extract_type,
        sequence=sequence,
        timestamp=timestamp,
    )


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
