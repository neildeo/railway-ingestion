from __future__ import annotations

import gzip
import io
import json

import pytest

from schedule_loader.source import (
    RecordType,
    SourceMetadata,
    iter_schedule_records,
    read_schedule_metadata,
)


def _gzip_ndjson(*records: dict) -> bytes:
    payload = b"".join(
        json.dumps(record).encode("utf-8") + b"\n"
        for record in records
    )
    return gzip.compress(payload)


class FakeBlob:
    def __init__(
        self,
        content: bytes,
        *,
        metadata: dict[str, str] | None = None,
    ) -> None:
        self.content = content
        self.metadata = metadata
        self.reload_called = False

    def open(self, mode: str):
        assert mode == "rb"
        return io.BytesIO(self.content)

    def reload(self) -> None:
        self.reload_called = True


class FakeBucket:
    def __init__(
        self,
        content: bytes,
        *,
        metadata: dict[str, str] | None = None,
    ) -> None:
        self.blob_instance = FakeBlob(
            content,
            metadata=metadata,
        )
        self.requested_object_name: str | None = None
        self.requested_generation: int | None = None

    def blob(
        self,
        object_name: str,
        *,
        generation: int,
    ) -> FakeBlob:
        self.requested_object_name = object_name
        self.requested_generation = generation

        return self.blob_instance


class FakeStorageClient:
    def __init__(
        self,
        content: bytes,
        *,
        metadata: dict[str, str] | None = None,
    ) -> None:
        self.bucket_instance = FakeBucket(
            content,
            metadata=metadata,
        )
        self.requested_bucket_name: str | None = None

    def bucket(self, bucket_name: str) -> FakeBucket:
        self.requested_bucket_name = bucket_name
        return self.bucket_instance


HEADER = {
    "Metadata": {
        "sequence": 5251,
        "type": "full",
    },
    "Sender": {
        "application": "NTROD",
        "component": "SCHEDULE",
        "organisation": "Rockshore",
    },
    "classification": "public",
    "owner": "Network Rail",
    "timestamp": 1791246721,
}


def test_read_schedule_metadata_returns_source_metadata() -> None:
    client = FakeStorageClient(
        b"",
        metadata={
            "source_sequence": "5251",
            "source_type": "full",
        },
    )

    result = read_schedule_metadata(
        client=client,  # pyright: ignore[reportArgumentType]
        bucket_name="railway-raw",
        object_name=(
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
        generation=123456789,
    )

    assert result == SourceMetadata(
        publication_sequence=5251,
        extract_type="full",
    )


def test_read_schedule_metadata_uses_exact_object_generation() -> None:
    client = FakeStorageClient(
        b"",
        metadata={
            "source_sequence": "5251",
            "source_type": "full",
        },
    )

    read_schedule_metadata(
        client=client,  # pyright: ignore[reportArgumentType]
        bucket_name="railway-raw",
        object_name="path/to/source.json.gz",
        generation=987654321,
    )

    assert client.requested_bucket_name == "railway-raw"
    assert (
        client.bucket_instance.requested_object_name
        == "path/to/source.json.gz"
    )
    assert (
        client.bucket_instance.requested_generation
        == 987654321
    )


def test_read_schedule_metadata_reloads_blob_metadata() -> None:
    client = FakeStorageClient(
        b"",
        metadata={
            "source_sequence": "5251",
            "source_type": "full",
        },
    )

    read_schedule_metadata(
        client=client,  # pyright: ignore[reportArgumentType]
        bucket_name="railway-raw",
        object_name="source.json.gz",
        generation=123,
    )

    assert (
        client.bucket_instance.blob_instance.reload_called
    )


@pytest.mark.parametrize(
    "metadata",
    [
        {
            "source_type": "full",
        },
        {
            "source_sequence": "5251",
        },
        {},
        None,
    ],
)
def test_read_schedule_metadata_requires_expected_fields(
    metadata: dict[str, str] | None,
) -> None:
    client = FakeStorageClient(
        b"",
        metadata=metadata,
    )

    with pytest.raises(
        (KeyError, TypeError),
    ):
        read_schedule_metadata(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name="source.json.gz",
            generation=123,
        )


def test_read_schedule_metadata_rejects_invalid_sequence() -> None:
    client = FakeStorageClient(
        b"",
        metadata={
            "source_sequence": "not-an-integer",
            "source_type": "full",
        },
    )

    with pytest.raises(ValueError):
        read_schedule_metadata(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name="source.json.gz",
            generation=123,
        )


def test_iter_schedule_records_returns_requested_record_type() -> None:
    content = _gzip_ndjson(
        {"JsonTimetableV1": HEADER},
        {
            "TiplocV1": {
                "tiploc_code": "AAA",
            }
        },
    )

    client = FakeStorageClient(content)

    result = list(
        iter_schedule_records(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name=(
                "schedules/full/sequence=5251/"
                "schedules.json.gz"
            ),
            generation=123456789,
            record_type=RecordType.PUBLICATION,
        )
    )

    assert result == [HEADER]


def test_iter_schedule_records_skips_records_before_target() -> None:
    first_tiploc = {
        "tiploc_code": "AAA",
    }
    second_tiploc = {
        "tiploc_code": "BBB",
    }

    content = _gzip_ndjson(
        {"JsonTimetableV1": HEADER},
        {"TiplocV1": first_tiploc},
        {"TiplocV1": second_tiploc},
        {
            "AssociationV1": {
                "main_train_uid": "A12345",
            }
        },
    )

    client = FakeStorageClient(content)

    result = list(
        iter_schedule_records(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name="schedule.json.gz",
            generation=123,
            record_type=RecordType.TIPLOC,
        )
    )

    assert result == [
        first_tiploc,
        second_tiploc,
    ]


def test_iter_schedule_records_stops_after_target_section() -> None:
    content = _gzip_ndjson(
        {"JsonTimetableV1": HEADER},
        {
            "TiplocV1": {
                "tiploc_code": "AAA",
            }
        },
        {
            "AssociationV1": {
                "main_train_uid": "A12345",
            }
        },
        {
            "TiplocV1": {
                "tiploc_code": "SHOULD_NOT_BE_READ",
            }
        },
    )

    client = FakeStorageClient(content)

    result = list(
        iter_schedule_records(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name="schedule.json.gz",
            generation=123,
            record_type=RecordType.TIPLOC,
        )
    )

    assert result == [
        {
            "tiploc_code": "AAA",
        }
    ]


def test_iter_schedule_records_uses_exact_object_generation() -> None:
    content = _gzip_ndjson(
        {"JsonTimetableV1": HEADER},
    )

    client = FakeStorageClient(content)

    list(
        iter_schedule_records(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name="path/to/source.json.gz",
            generation=987654321,
            record_type=RecordType.PUBLICATION,
        )
    )

    assert client.requested_bucket_name == "railway-raw"
    assert (
        client.bucket_instance.requested_object_name
        == "path/to/source.json.gz"
    )
    assert (
        client.bucket_instance.requested_generation
        == 987654321
    )


def test_iter_schedule_records_returns_empty_for_empty_object() -> None:
    client = FakeStorageClient(
        gzip.compress(b"")
    )

    result = list(
        iter_schedule_records(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name="empty.json.gz",
            generation=123,
            record_type=RecordType.PUBLICATION,
        )
    )

    assert result == []


def test_iter_schedule_records_errors_on_invalid_json() -> None:
    client = FakeStorageClient(
        gzip.compress(
            b'{"JsonTimetableV1": definitely-not-json}\n'
        )
    )

    with pytest.raises(json.JSONDecodeError):
        list(
            iter_schedule_records(
                client=client,  # pyright: ignore[reportArgumentType]
                bucket_name="railway-raw",
                object_name="invalid.json.gz",
                generation=123,
                record_type=RecordType.PUBLICATION,
            )
        )


def test_iter_schedule_records_returns_empty_when_type_absent() -> None:
    client = FakeStorageClient(
        _gzip_ndjson(
            {"JsonTimetableV1": HEADER},
            {
                "TiplocV1": {
                    "tiploc_code": "AAA",
                }
            },
        )
    )

    result = list(
        iter_schedule_records(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name="schedule.json.gz",
            generation=123,
            record_type=RecordType.ASSOCIATION,
        )
    )

    assert result == []
