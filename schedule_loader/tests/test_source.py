from __future__ import annotations

import gzip
import io
import json

import pytest

from schedule_loader.source import read_schedule_header


def _gzip_ndjson(*records: dict) -> bytes:
    payload = b"".join(
        json.dumps(record).encode("utf-8") + b"\n"
        for record in records
    )
    return gzip.compress(payload)


class FakeBlob:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def open(self, mode: str):
        assert mode == "rb"
        return io.BytesIO(self.content)


class FakeBucket:
    def __init__(self, content: bytes) -> None:
        self.content = content
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

        return FakeBlob(self.content)


class FakeStorageClient:
    def __init__(self, content: bytes) -> None:
        self.bucket_instance = FakeBucket(content)
        self.requested_bucket_name: str | None = None

    def bucket(self, bucket_name: str) -> FakeBucket:
        self.requested_bucket_name = bucket_name
        return self.bucket_instance


def test_read_schedule_header_returns_json_timetable_header() -> None:
    header = {
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

    content = _gzip_ndjson(
        {"JsonTimetableV1": header},
        {
            "TiplocV1": {
                "tiploc_code": "AAA",
            }
        },
    )

    client = FakeStorageClient(content)

    result = read_schedule_header(
        client=client,  # pyright: ignore[reportArgumentType]
        bucket_name="railway-raw",
        object_name=(
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
        generation=123456789,
    )

    assert result == header


def test_read_schedule_header_uses_exact_object_generation() -> None:
    content = _gzip_ndjson(
        {
            "JsonTimetableV1": {
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
        }
    )

    client = FakeStorageClient(content)

    read_schedule_header(
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


def test_read_schedule_header_rejects_empty_object() -> None:
    client = FakeStorageClient(
        gzip.compress(b"")
    )

    with pytest.raises(
        ValueError,
        match="empty",
    ):
        read_schedule_header(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name="empty.json.gz",
            generation=123,
        )


def test_read_schedule_header_rejects_invalid_json() -> None:
    client = FakeStorageClient(
        gzip.compress(
            b'{"JsonTimetableV1": definitely-not-json}\n'
        )
    )

    with pytest.raises(json.JSONDecodeError):
        read_schedule_header(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name="invalid.json.gz",
            generation=123,
        )


def test_read_schedule_header_rejects_wrong_record_type() -> None:
    client = FakeStorageClient(
        _gzip_ndjson(
            {
                "TiplocV1": {
                    "tiploc_code": "AAA",
                }
            }
        )
    )

    with pytest.raises(
        ValueError,
        match="JsonTimetableV1",
    ):
        read_schedule_header(
            client=client,  # pyright: ignore[reportArgumentType]
            bucket_name="railway-raw",
            object_name="wrong-record.json.gz",
            generation=123,
        )
