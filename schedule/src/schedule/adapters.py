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
    SourceMutationError,
)

import requests
import zlib
import json
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from google.cloud import storage, secretmanager
from google.api_core import exceptions as google_exceptions, retry as google_retry

from dataclasses import dataclass
from json import JSONDecoder


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

_MAX_TRANSFER_ATTEMPTS = 3

_TRANSFER_CHUNK_SIZE = 1024 * 1024  # 1 MiB


@dataclass(frozen=True)
class NetworkRailCredentials:
    username: str
    password: str


def get_network_rail_credentials_from_secret_manager(
    project_id: str,
    secret_id: str,
) -> NetworkRailCredentials:
    client = secretmanager.SecretManagerServiceClient()

    secret_name = (
        f"projects/{project_id}/secrets/"
        f"{secret_id}/versions/latest"
    )

    secret = client.access_secret_version(
        name=secret_name,
        retry=google_retry.Retry(
            initial=1.0,
            maximum=8.0,
            multiplier=2.0,
            timeout=30.0,
        ),
    )

    payload: dict[str, str] = JSONDecoder().decode(
        secret.payload.data.decode()
    )

    try:
        username = payload["username"]
        password = payload["password"]
    except KeyError as exc:
        raise ValueError(
            f"Secret payload is missing {exc.args[0]}"
        ) from exc

    return NetworkRailCredentials(
        username=username,
        password=password,
    )


def create_requests_session(
    credentials: NetworkRailCredentials,
) -> requests.Session:
    retry_policy = Retry(
        total=MAX_RETRIES,
        allowed_methods=frozenset({"GET"}),
        status_forcelist=RETRYABLE_STATUS_CODES,
    )

    s = requests.Session()

    s.auth = (
        credentials.username,
        credentials.password,
    )

    s.mount(
        prefix="https://",
        adapter=HTTPAdapter(max_retries=retry_policy),
    )

    return s


def get_endpoint_url(schedule_request: ScheduleRequest) -> str:
    match schedule_request:
        case FullSnapshotRequest():
            return (
                "https://publicdatafeeds.networkrail.co.uk/ntrod/"
                "CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full"
            )
        case UpdateRequest(day):
            return (
                "https://publicdatafeeds.networkrail.co.uk/ntrod/"
                "CifFileAuthenticate?"
                f"type=CIF_ALL_UPDATE_DAILY&day=toc-update-{day.value}"
            )


def fetch_header_row(
    schedule_request: ScheduleRequest,
    *,
    session: requests.Session,
) -> SchedulePublicationInfo:
    url = get_endpoint_url(schedule_request)

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


def get_object_state(
    object_name: str,
) -> StoredObjectState | None:
    """
    Return the existing GCS object's stored source provenance and generation,
    or None when the object does not exist.
    """
    client = storage.Client()
    bucket = client.bucket(_get_raw_bucket_name())
    blob = bucket.blob(object_name)

    try:
        blob.reload()
    except google_exceptions.NotFound:
        return None

    blob_metadata = blob.metadata
    if blob_metadata is None:
        raise ValueError("GCS object has no metadata")

    try:
        source_etag = blob_metadata["source_etag"]
        source_last_modified = blob_metadata[
            "source_last_modified"
        ]
        source_content_length = int(
            blob_metadata["source_content_length"]
        )
    except KeyError as exc:
        raise ValueError(
            f"GCS object has no metadata field: {exc.args[0]}"
        ) from exc
    except ValueError as exc:
        raise ValueError(
            "GCS object source_content_length is not an integer"
        ) from exc

    if blob.generation is None:
        raise ValueError(
            "GCS object has no generation metadata"
        )

    return StoredObjectState(
        source_metadata=SourceMetadata(
            etag=source_etag,
            last_modified=source_last_modified,
            content_length=source_content_length,
        ),
        generation=blob.generation,
    )


def _get_raw_bucket_name() -> str:
    return "railway-analytics-508615-network-rail-open-data-raw"


def upload_schedule(
    schedule_request: ScheduleRequest,
    object_name: str,
    expected_metadata: SchedulePublicationInfo,
    *,
    session: requests.Session,
) -> None:
    """
    Perform a fresh full source request and stream the exact compressed bytes
    into the normal GCS object.

    Before transferring the body, verify the new publication info and S3
    response metadata still match `expected_metadata`.

    The final GCS write should be create-only (`if_generation_match=0`).
    """
    _stream_schedule_to_gcs(
        schedule_request=schedule_request,
        object_name=object_name,
        expected_metadata=expected_metadata,
        resolve_concurrent_create=True,
        session=session,
    )


def quarantine_schedule(
    schedule_request: ScheduleRequest,
    object_name: str,
    expected_metadata: SchedulePublicationInfo,
    *,
    session: requests.Session,
) -> None:
    """
    Perform a fresh full source request and preserve the conflicting artefact
    under the supplied quarantine object name.

    The new source response metadata should still be checked against the probe
    metadata before the transfer begins.
    """
    _stream_schedule_to_gcs(
        schedule_request=schedule_request,
        object_name=object_name,
        expected_metadata=expected_metadata,
        resolve_concurrent_create=False,
        session=session,
    )


def _gcs_metadata(
    publication_info: SchedulePublicationInfo,
) -> dict[str, str]:
    header = publication_info.header
    source = publication_info.source_metadata

    return {
        "source_type": header.extract_type.value,
        "source_sequence": str(header.sequence),
        "source_timestamp": str(header.timestamp),
        "source_etag": source.etag,
        "source_last_modified": source.last_modified,
        "source_content_length": str(source.content_length),
    }


def _is_transient_transfer_error(exc: Exception) -> bool:
    return (
        isinstance(exc, _TRANSIENT_REQUEST_ERRORS)
        or google_retry.if_transient_error(exc)
    )


def _stream_schedule_to_gcs(
    *,
    schedule_request: ScheduleRequest,
    object_name: str,
    expected_metadata: SchedulePublicationInfo,
    resolve_concurrent_create: bool,
    session: requests.Session,
) -> None:
    url = get_endpoint_url(schedule_request)

    client = storage.Client()
    bucket = client.bucket(_get_raw_bucket_name())
    blob = bucket.blob(object_name)

    blob.metadata = _gcs_metadata(expected_metadata)

    for attempt in range(_MAX_TRANSFER_ATTEMPTS):
        try:
            with session.get(
                url,
                stream=True,
            ) as response:
                response.raise_for_status()

                actual_source_metadata = (
                    _extract_source_metadata(response)
                )

                if (
                    actual_source_metadata
                    != expected_metadata.source_metadata
                ):
                    raise SourceMutationError(
                        "SCHEDULE source changed between probe "
                        "and transfer"
                    )

                with blob.open(
                    "wb",
                    if_generation_match=0,
                ) as writer:
                    for chunk in response.iter_content(
                        chunk_size=_TRANSFER_CHUNK_SIZE
                    ):
                        if chunk:
                            writer.write(chunk)

            return

        except google_exceptions.PreconditionFailed:
            if not resolve_concurrent_create:
                raise

            winning_state = get_object_state(object_name)

            if (
                winning_state is not None
                and winning_state.source_metadata
                == expected_metadata.source_metadata
            ):
                return

            raise SourceMutationError(
                "Concurrent writer created a SCHEDULE object "
                "with conflicting provenance"
            )

        except Exception as exc:
            if (
                not _is_transient_transfer_error(exc)
                or attempt == _MAX_TRANSFER_ATTEMPTS - 1
            ):
                raise

    raise AssertionError("unreachable")
