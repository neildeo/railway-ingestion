from __future__ import annotations

import gzip
import json
from collections.abc import Iterator
from io import BytesIO
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests
from google.api_core import exceptions as google_exceptions

from schedule.adapters import (
    fetch_header_row,
    get_object_state,
    quarantine_schedule,
    upload_schedule,
)
from schedule.core import (
    ExtractType,
    ScheduleHeader,
    SchedulePublicationInfo,
    SourceMetadata,
    SourceMutationError,
    StoredObjectState,
    UpdateRequest,
    Weekday,
)


SOURCE_METADATA = SourceMetadata(
    etag='"abc123"',
    last_modified="Sat, 03 Oct 2026 00:32:35 GMT",
    content_length=123_456,
)

PUBLICATION_INFO = SchedulePublicationInfo(
    header=ScheduleHeader(
        extract_type=ExtractType.UPDATE,
        sequence=5246,
        timestamp=1790987555,
    ),
    source_metadata=SOURCE_METADATA,
)

SCHEDULE_REQUEST = UpdateRequest(day=Weekday.WED)

OBJECT_NAME = (
    "schedules/update/sequence=5246/schedules.json.gz"
)

QUARANTINE_OBJECT_NAME = (
    "schedules/quarantine/update/sequence=5246/"
    "2026-10-03T10-03-42.381927Z.json.gz"
)


def timetable_header_bytes(
    *,
    extract_type: str = "update",
    sequence: int = 5246,
    timestamp: int = 1790987555,
) -> bytes:
    header = {
        "JsonTimetableV1": {
            "classification": "public",
            "timestamp": timestamp,
            "owner": "Network Rail",
            "Sender": {
                "organisation": "Rockshore",
                "application": "NTROD",
                "component": "SCHEDULE",
            },
            "Metadata": {
                "type": extract_type,
                "sequence": sequence,
            },
        }
    }

    return json.dumps(header).encode() + b"\n"


def compressed_schedule_bytes() -> bytes:
    body = (
        timetable_header_bytes()
        + b'{"JsonScheduleV1":{"CIF_train_uid":"A12345"}}\n'
        + b'{"JsonScheduleV1":{"CIF_train_uid":"B12345"}}\n'
    )

    return gzip.compress(body)


def response_headers(
    metadata: SourceMetadata = SOURCE_METADATA,
) -> dict[str, str]:
    return {
        "ETag": metadata.etag,
        "Last-Modified": metadata.last_modified,
        "Content-Length": str(metadata.content_length),
    }


class FakeResponse:
    def __init__(
        self,
        *,
        body: bytes,
        headers: dict[str, str],
        status_code: int = 200,
        chunks: list[bytes] | None = None,
        stream_error: Exception | None = None,
        fail_after_chunks: int | None = None,
    ) -> None:
        self.body = body
        self.headers = headers
        self.status_code = status_code
        self._chunks = chunks
        self._stream_error = stream_error
        self._fail_after_chunks = fail_after_chunks
        self.closed = False

    def raise_for_status(self) -> None:
        if 400 <= self.status_code:
            raise requests.HTTPError(
                response=Mock(status_code=self.status_code)
            )

    def iter_content(
        self,
        chunk_size: int = 8192,
    ) -> Iterator[bytes]:
        chunks = self._chunks

        if chunks is None:
            chunks = [
                self.body[i:i + chunk_size]
                for i in range(0, len(self.body), chunk_size)
            ]

        for index, chunk in enumerate(chunks):
            if (
                self._stream_error is not None
                and self._fail_after_chunks == index
            ):
                raise self._stream_error

            yield chunk

        if (
            self._stream_error is not None
            and self._fail_after_chunks == len(chunks)
        ):
            raise self._stream_error

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class FakeWriter(BytesIO):
    def __init__(
        self,
        *,
        write_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self.write_error = write_error
        self.closed_cleanly = False

    def write(self, data: bytes) -> int:
        if self.write_error is not None:
            raise self.write_error

        return super().write(data)

    def __enter__(self) -> FakeWriter:
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        if exc_type is None:
            self.closed_cleanly = True


# ---------------------------------------------------------------------------
# fetch_header_row
# ---------------------------------------------------------------------------


def test_fetch_header_row_parses_header_and_source_metadata() -> None:
    compressed = compressed_schedule_bytes()

    response = FakeResponse(
        body=compressed,
        headers=response_headers(),
    )

    session = Mock()
    session.get.return_value = response

    with patch(
        "schedule.adapters.create_requests_session",
        return_value=session,
    ):
        result = fetch_header_row(SCHEDULE_REQUEST)

    assert result == PUBLICATION_INFO
    assert response.closed


def test_fetch_header_row_handles_header_spanning_multiple_compressed_chunks(
) -> None:
    compressed = compressed_schedule_bytes()

    chunks = [
        compressed[:5],
        compressed[5:11],
        compressed[11:19],
        compressed[19:31],
        compressed[31:],
    ]

    response = FakeResponse(
        body=compressed,
        headers=response_headers(),
        chunks=chunks,
    )

    session = Mock()
    session.get.return_value = response

    with patch(
        "schedule.adapters.create_requests_session",
        return_value=session,
    ):
        result = fetch_header_row(SCHEDULE_REQUEST)

    assert result == PUBLICATION_INFO


def test_fetch_header_row_stops_once_first_ndjson_row_is_available() -> None:
    body = (
        timetable_header_bytes()
        + b"x" * 5_000_000
    )
    compressed = gzip.compress(body)

    first = compressed[:200]
    remainder = compressed[200:]

    response = FakeResponse(
        body=compressed,
        headers=response_headers(),
        chunks=[first, remainder],
    )

    session = Mock()
    session.get.return_value = response

    with patch(
        "schedule.adapters.create_requests_session",
        return_value=session,
    ):
        result = fetch_header_row(SCHEDULE_REQUEST)

    assert result == PUBLICATION_INFO
    assert response.closed


@pytest.mark.parametrize(
    "missing_header",
    [
        "ETag",
        "Last-Modified",
        "Content-Length",
    ],
)
def test_fetch_header_row_rejects_missing_source_metadata(
    missing_header: str,
) -> None:
    headers = response_headers()
    del headers[missing_header]

    response = FakeResponse(
        body=compressed_schedule_bytes(),
        headers=headers,
    )

    session = Mock()
    session.get.return_value = response

    with patch(
        "schedule.adapters.create_requests_session",
        return_value=session,
    ):
        with pytest.raises(ValueError):
            fetch_header_row(SCHEDULE_REQUEST)


def test_fetch_header_row_rejects_invalid_content_length() -> None:
    headers = response_headers()
    headers["Content-Length"] = "not-an-integer"

    response = FakeResponse(
        body=compressed_schedule_bytes(),
        headers=headers,
    )

    session = Mock()
    session.get.return_value = response

    with patch(
        "schedule.adapters.create_requests_session",
        return_value=session,
    ):
        with pytest.raises(ValueError):
            fetch_header_row(SCHEDULE_REQUEST)


def test_fetch_header_row_rejects_invalid_gzip() -> None:
    response = FakeResponse(
        body=b"this is not gzip",
        headers=response_headers(),
    )

    session = Mock()
    session.get.return_value = response

    with patch(
        "schedule.adapters.create_requests_session",
        return_value=session,
    ):
        with pytest.raises(ValueError):
            fetch_header_row(SCHEDULE_REQUEST)


def test_fetch_header_row_rejects_invalid_json() -> None:
    compressed = gzip.compress(
        b'{"JsonTimetableV1": definitely-not-json}\n'
    )

    response = FakeResponse(
        body=compressed,
        headers=response_headers(),
    )

    session = Mock()
    session.get.return_value = response

    with patch(
        "schedule.adapters.create_requests_session",
        return_value=session,
    ):
        with pytest.raises(ValueError):
            fetch_header_row(SCHEDULE_REQUEST)


@pytest.mark.parametrize(
    "header",
    [
        {},
        {"JsonTimetableV1": {}},
        {
            "JsonTimetableV1": {
                "timestamp": 1790987555,
                "Metadata": {},
            }
        },
        {
            "JsonTimetableV1": {
                "timestamp": 1790987555,
                "Metadata": {
                    "type": "update",
                },
            }
        },
    ],
)
def test_fetch_header_row_rejects_incomplete_timetable_header(
    header: dict[str, object],
) -> None:
    compressed = gzip.compress(
        json.dumps(header).encode() + b"\n"
    )

    response = FakeResponse(
        body=compressed,
        headers=response_headers(),
    )

    session = Mock()
    session.get.return_value = response

    with patch(
        "schedule.adapters.create_requests_session",
        return_value=session,
    ):
        with pytest.raises(ValueError):
            fetch_header_row(SCHEDULE_REQUEST)


def test_fetch_header_row_rejects_header_larger_than_one_mib() -> None:
    oversized_first_line = (
        b'{"padding":"'
        + b"x" * (1024 * 1024)
        + b'"}\n'
    )

    response = FakeResponse(
        body=gzip.compress(oversized_first_line),
        headers=response_headers(),
    )

    session = Mock()
    session.get.return_value = response

    with patch(
        "schedule.adapters.create_requests_session",
        return_value=session,
    ):
        with pytest.raises(ValueError):
            fetch_header_row(SCHEDULE_REQUEST)


def test_fetch_header_row_retries_transient_stream_failure_from_start() -> None:
    compressed = compressed_schedule_bytes()

    first_response = FakeResponse(
        body=compressed,
        headers=response_headers(),
        chunks=[
            compressed[:20],
            compressed[20:],
        ],
        stream_error=requests.ConnectionError("connection reset"),
        fail_after_chunks=1,
    )

    second_response = FakeResponse(
        body=compressed,
        headers=response_headers(),
    )

    session = Mock()
    session.get.side_effect = [
        first_response,
        second_response,
    ]

    with patch(
        "schedule.adapters.create_requests_session",
        return_value=session,
    ):
        result = fetch_header_row(SCHEDULE_REQUEST)

    assert result == PUBLICATION_INFO
    assert session.get.call_count == 2


# ---------------------------------------------------------------------------
# get_object_state
# ---------------------------------------------------------------------------


def test_get_object_state_returns_none_when_object_does_not_exist() -> None:
    blob = Mock()
    blob.reload.side_effect = google_exceptions.NotFound(
        "object not found"
    )

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with patch(
        "schedule.adapters.storage.Client",
        return_value=client,
    ):
        result = get_object_state(OBJECT_NAME)

    assert result is None


def test_get_object_state_returns_source_metadata_and_generation() -> None:
    blob = Mock()
    blob.generation = 17
    blob.metadata = {
        "source_etag": SOURCE_METADATA.etag,
        "source_last_modified": SOURCE_METADATA.last_modified,
        "source_content_length": str(
            SOURCE_METADATA.content_length
        ),
    }

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with patch(
        "schedule.adapters.storage.Client",
        return_value=client,
    ):
        result = get_object_state(OBJECT_NAME)

    assert result == StoredObjectState(
        source_metadata=SOURCE_METADATA,
        generation=17,
    )


@pytest.mark.parametrize(
    "missing_field",
    [
        "source_etag",
        "source_last_modified",
        "source_content_length",
    ],
)
def test_get_object_state_rejects_missing_source_metadata(
    missing_field: str,
) -> None:
    metadata = {
        "source_etag": SOURCE_METADATA.etag,
        "source_last_modified": SOURCE_METADATA.last_modified,
        "source_content_length": str(
            SOURCE_METADATA.content_length
        ),
    }
    del metadata[missing_field]

    blob = Mock()
    blob.generation = 17
    blob.metadata = metadata

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with patch(
        "schedule.adapters.storage.Client",
        return_value=client,
    ):
        with pytest.raises(ValueError):
            get_object_state(OBJECT_NAME)


def test_get_object_state_rejects_missing_generation() -> None:
    blob = Mock()
    blob.generation = None
    blob.metadata = {
        "source_etag": SOURCE_METADATA.etag,
        "source_last_modified": SOURCE_METADATA.last_modified,
        "source_content_length": str(
            SOURCE_METADATA.content_length
        ),
    }

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with patch(
        "schedule.adapters.storage.Client",
        return_value=client,
    ):
        with pytest.raises(ValueError):
            get_object_state(OBJECT_NAME)


def test_get_object_state_propagates_non_not_found_gcs_error() -> None:
    blob = Mock()
    blob.reload.side_effect = google_exceptions.Forbidden(
        "permission denied"
    )

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with patch(
        "schedule.adapters.storage.Client",
        return_value=client,
    ):
        with pytest.raises(google_exceptions.Forbidden):
            get_object_state(OBJECT_NAME)


# ---------------------------------------------------------------------------
# uploads
# ---------------------------------------------------------------------------


def make_gcs_client(
    writer: FakeWriter,
) -> tuple[Mock, Mock]:
    blob = Mock()
    blob.open.return_value = writer

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    return client, blob


def test_upload_schedule_streams_exact_compressed_bytes_to_gcs() -> None:
    compressed = compressed_schedule_bytes()

    response = FakeResponse(
        body=compressed,
        headers=response_headers(),
        chunks=[
            compressed[:17],
            compressed[17:43],
            compressed[43:],
        ],
    )

    session = Mock()
    session.get.return_value = response

    writer = FakeWriter()
    client, blob = make_gcs_client(writer)

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        upload_schedule(
            SCHEDULE_REQUEST,
            OBJECT_NAME,
            PUBLICATION_INFO,
        )

    assert writer.getvalue() == compressed
    assert writer.closed_cleanly

    blob.open.assert_called_once()
    assert (
        blob.open.call_args.kwargs["if_generation_match"]
        == 0
    )


def test_upload_schedule_persists_publication_metadata() -> None:
    compressed = compressed_schedule_bytes()

    response = FakeResponse(
        body=compressed,
        headers=response_headers(),
    )

    session = Mock()
    session.get.return_value = response

    writer = FakeWriter()
    client, blob = make_gcs_client(writer)

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        upload_schedule(
            SCHEDULE_REQUEST,
            OBJECT_NAME,
            PUBLICATION_INFO,
        )

    assert blob.metadata == {
        "source_type": "update",
        "source_sequence": "5246",
        "source_timestamp": "1790987555",
        "source_etag": '"abc123"',
        "source_last_modified": (
            "Sat, 03 Oct 2026 00:32:35 GMT"
        ),
        "source_content_length": "123456",
    }


@pytest.mark.parametrize(
    "actual_metadata",
    [
        SourceMetadata(
            etag='"different"',
            last_modified=SOURCE_METADATA.last_modified,
            content_length=SOURCE_METADATA.content_length,
        ),
        SourceMetadata(
            etag=SOURCE_METADATA.etag,
            last_modified="Sat, 03 Oct 2026 01:00:00 GMT",
            content_length=SOURCE_METADATA.content_length,
        ),
        SourceMetadata(
            etag=SOURCE_METADATA.etag,
            last_modified=SOURCE_METADATA.last_modified,
            content_length=999_999,
        ),
    ],
)
def test_upload_schedule_rejects_source_change_before_transfer(
    actual_metadata: SourceMetadata,
) -> None:
    response = FakeResponse(
        body=compressed_schedule_bytes(),
        headers=response_headers(actual_metadata),
    )

    session = Mock()
    session.get.return_value = response

    writer = FakeWriter()
    client, blob = make_gcs_client(writer)

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        with pytest.raises(SourceMutationError):
            upload_schedule(
                SCHEDULE_REQUEST,
                OBJECT_NAME,
                PUBLICATION_INFO,
            )

    blob.open.assert_not_called()


def test_upload_schedule_retries_whole_transfer_after_transient_source_failure(
) -> None:
    compressed = compressed_schedule_bytes()

    first_response = FakeResponse(
        body=compressed,
        headers=response_headers(),
        chunks=[
            compressed[:20],
            compressed[20:],
        ],
        stream_error=requests.ConnectionError(
            "connection reset"
        ),
        fail_after_chunks=1,
    )

    second_response = FakeResponse(
        body=compressed,
        headers=response_headers(),
    )

    session = Mock()
    session.get.side_effect = [
        first_response,
        second_response,
    ]

    first_writer = FakeWriter()
    second_writer = FakeWriter()

    blob = Mock()
    blob.open.side_effect = [
        first_writer,
        second_writer,
    ]

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        upload_schedule(
            SCHEDULE_REQUEST,
            OBJECT_NAME,
            PUBLICATION_INFO,
        )

    assert session.get.call_count == 2
    assert blob.open.call_count == 2
    assert second_writer.getvalue() == compressed


def test_upload_schedule_retries_whole_transfer_after_transient_gcs_failure(
) -> None:
    compressed = compressed_schedule_bytes()

    responses = [
        FakeResponse(
            body=compressed,
            headers=response_headers(),
        ),
        FakeResponse(
            body=compressed,
            headers=response_headers(),
        ),
    ]

    session = Mock()
    session.get.side_effect = responses

    first_writer = FakeWriter(
        write_error=google_exceptions.ServiceUnavailable(
            "GCS unavailable"
        )
    )
    second_writer = FakeWriter()

    blob = Mock()
    blob.open.side_effect = [
        first_writer,
        second_writer,
    ]

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        upload_schedule(
            SCHEDULE_REQUEST,
            OBJECT_NAME,
            PUBLICATION_INFO,
        )

    assert session.get.call_count == 2
    assert blob.open.call_count == 2
    assert second_writer.getvalue() == compressed


def test_upload_schedule_stops_after_three_transient_failures() -> None:
    compressed = compressed_schedule_bytes()

    responses = [
        FakeResponse(
            body=compressed,
            headers=response_headers(),
            stream_error=requests.ConnectionError(
                "connection reset"
            ),
            fail_after_chunks=0,
        )
        for _ in range(3)
    ]

    session = Mock()
    session.get.side_effect = responses

    writer = FakeWriter()
    client, blob = make_gcs_client(writer)

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        with pytest.raises(requests.ConnectionError):
            upload_schedule(
                SCHEDULE_REQUEST,
                OBJECT_NAME,
                PUBLICATION_INFO,
            )

    assert session.get.call_count == 3


def test_upload_schedule_does_not_retry_non_transient_source_error() -> None:
    response = FakeResponse(
        body=b"",
        headers=response_headers(),
        status_code=403,
    )

    session = Mock()
    session.get.return_value = response

    writer = FakeWriter()
    client, blob = make_gcs_client(writer)

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        with pytest.raises(requests.HTTPError):
            upload_schedule(
                SCHEDULE_REQUEST,
                OBJECT_NAME,
                PUBLICATION_INFO,
            )

    assert session.get.call_count == 1
    blob.open.assert_not_called()


def test_upload_schedule_lost_race_with_matching_winner_is_success() -> None:
    compressed = compressed_schedule_bytes()

    response = FakeResponse(
        body=compressed,
        headers=response_headers(),
    )

    session = Mock()
    session.get.return_value = response

    blob = Mock()
    blob.open.side_effect = google_exceptions.PreconditionFailed(
        "object already exists"
    )

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    winning_state = StoredObjectState(
        source_metadata=SOURCE_METADATA,
        generation=18,
    )

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
        patch(
            "schedule.adapters.get_object_state",
            return_value=winning_state,
        ),
    ):
        upload_schedule(
            SCHEDULE_REQUEST,
            OBJECT_NAME,
            PUBLICATION_INFO,
        )


def test_upload_schedule_lost_race_with_conflicting_winner_fails() -> None:
    compressed = compressed_schedule_bytes()

    response = FakeResponse(
        body=compressed,
        headers=response_headers(),
    )

    session = Mock()
    session.get.return_value = response

    blob = Mock()
    blob.open.side_effect = google_exceptions.PreconditionFailed(
        "object already exists"
    )

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    winning_state = StoredObjectState(
        source_metadata=SourceMetadata(
            etag='"different"',
            last_modified="Sat, 03 Oct 2026 01:00:00 GMT",
            content_length=999_999,
        ),
        generation=18,
    )

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
        patch(
            "schedule.adapters.get_object_state",
            return_value=winning_state,
        ),
    ):
        with pytest.raises(SourceMutationError):
            upload_schedule(
                SCHEDULE_REQUEST,
                OBJECT_NAME,
                PUBLICATION_INFO,
            )


# ---------------------------------------------------------------------------
# quarantine
# ---------------------------------------------------------------------------


def test_quarantine_schedule_streams_exact_compressed_bytes() -> None:
    compressed = compressed_schedule_bytes()

    response = FakeResponse(
        body=compressed,
        headers=response_headers(),
    )

    session = Mock()
    session.get.return_value = response

    writer = FakeWriter()
    client, blob = make_gcs_client(writer)

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        quarantine_schedule(
            SCHEDULE_REQUEST,
            QUARANTINE_OBJECT_NAME,
            PUBLICATION_INFO,
        )

    assert writer.getvalue() == compressed
    assert writer.closed_cleanly


def test_quarantine_schedule_uses_supplied_quarantine_object_name() -> None:
    response = FakeResponse(
        body=compressed_schedule_bytes(),
        headers=response_headers(),
    )

    session = Mock()
    session.get.return_value = response

    writer = FakeWriter()
    client, blob = make_gcs_client(writer)

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        quarantine_schedule(
            SCHEDULE_REQUEST,
            QUARANTINE_OBJECT_NAME,
            PUBLICATION_INFO,
        )

    client.bucket.assert_called_once()
    client.bucket.return_value.blob.assert_called_once_with(
        QUARANTINE_OBJECT_NAME
    )


def test_quarantine_schedule_rejects_source_change_before_transfer() -> None:
    changed_metadata = SourceMetadata(
        etag='"changed"',
        last_modified=SOURCE_METADATA.last_modified,
        content_length=SOURCE_METADATA.content_length,
    )

    response = FakeResponse(
        body=compressed_schedule_bytes(),
        headers=response_headers(changed_metadata),
    )

    session = Mock()
    session.get.return_value = response

    writer = FakeWriter()
    client, blob = make_gcs_client(writer)

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        with pytest.raises(SourceMutationError):
            quarantine_schedule(
                SCHEDULE_REQUEST,
                QUARANTINE_OBJECT_NAME,
                PUBLICATION_INFO,
            )

    blob.open.assert_not_called()


def test_quarantine_schedule_retries_transient_failure_from_start() -> None:
    compressed = compressed_schedule_bytes()

    first_response = FakeResponse(
        body=compressed,
        headers=response_headers(),
        stream_error=requests.ConnectionError(
            "connection reset"
        ),
        fail_after_chunks=0,
    )

    second_response = FakeResponse(
        body=compressed,
        headers=response_headers(),
    )

    session = Mock()
    session.get.side_effect = [
        first_response,
        second_response,
    ]

    first_writer = FakeWriter()
    second_writer = FakeWriter()

    blob = Mock()
    blob.open.side_effect = [
        first_writer,
        second_writer,
    ]

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with (
        patch(
            "schedule.adapters.create_requests_session",
            return_value=session,
        ),
        patch(
            "schedule.adapters.storage.Client",
            return_value=client,
        ),
    ):
        quarantine_schedule(
            SCHEDULE_REQUEST,
            QUARANTINE_OBJECT_NAME,
            PUBLICATION_INFO,
        )

    assert session.get.call_count == 2
    assert blob.open.call_count == 2
    assert second_writer.getvalue() == compressed
