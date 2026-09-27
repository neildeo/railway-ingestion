from unittest.mock import Mock, patch

import pytest
from google.api_core import exceptions as google_exceptions
from google.cloud.storage.retry import DEFAULT_RETRY_IF_GENERATION_SPECIFIED

from corpus.core import ObjectState
from corpus.storage import (
    crc32c_checksum,
    get_object_state,
    get_raw_bucket_name,
    upload_object,
)


EXPECTED_RAW_BUCKET_NAME = "railway-analytics-508615-network-rail-open-data-raw"


def test_get_raw_bucket_name_returns_expected_bucket() -> None:
    assert get_raw_bucket_name() == EXPECTED_RAW_BUCKET_NAME


def test_crc32c_checksum_returns_gcs_base64_format() -> None:
    # Standard CRC32C check vector:
    # CRC32C("123456789") == 0xe3069283
    # GCS exposes CRC32C as the base64 encoding of those four bytes.
    assert crc32c_checksum(b"123456789") == "4waSgw=="


def test_get_object_state_returns_checksum_and_generation() -> None:
    blob = Mock()
    blob.crc32c = "4waSgw=="
    blob.generation = 123

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with patch(
        "corpus.storage.storage.Client",
        return_value=client,
    ):
        result = get_object_state(
            "corpus/acquired_date=2026-09-27/corpus.json"
        )

    assert result == ObjectState(
        crc32c="4waSgw==",
        generation=123,
    )

    client.bucket.assert_called_once_with(EXPECTED_RAW_BUCKET_NAME)
    bucket.blob.assert_called_once_with(
        "corpus/acquired_date=2026-09-27/corpus.json"
    )
    blob.reload.assert_called_once_with()


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
        "corpus.storage.storage.Client",
        return_value=client,
    ):
        result = get_object_state(
            "corpus/acquired_date=2026-09-27/corpus.json"
        )

    assert result is None


def test_get_object_state_propagates_non_not_found_errors() -> None:
    blob = Mock()
    blob.reload.side_effect = google_exceptions.Forbidden(
        "permission denied"
    )

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with patch(
        "corpus.storage.storage.Client",
        return_value=client,
    ):
        with pytest.raises(google_exceptions.Forbidden):
            get_object_state(
                "corpus/acquired_date=2026-09-27/corpus.json"
            )


def test_upload_object_uploads_exact_bytes_with_generation_precondition() -> None:
    data = b'{"TIPLOCDATA":[]}'

    blob = Mock()

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with patch(
        "corpus.storage.storage.Client",
        return_value=client,
    ):
        upload_object(
            "corpus/acquired_date=2026-09-27/corpus.json",
            data,
            expected_generation=123,
        )

    client.bucket.assert_called_once_with(EXPECTED_RAW_BUCKET_NAME)

    bucket.blob.assert_called_once_with(
        "corpus/acquired_date=2026-09-27/corpus.json"
    )

    blob.upload_from_string.assert_called_once_with(
        data,
        if_generation_match=123,
        retry=DEFAULT_RETRY_IF_GENERATION_SPECIFIED,
    )


def test_upload_object_uses_generation_zero_for_create_only() -> None:
    data = b'{"TIPLOCDATA":[]}'

    blob = Mock()

    bucket = Mock()
    bucket.blob.return_value = blob

    client = Mock()
    client.bucket.return_value = bucket

    with patch(
        "corpus.storage.storage.Client",
        return_value=client,
    ):
        upload_object(
            "corpus/acquired_date=2026-09-27/corpus.json",
            data,
            expected_generation=0,
        )

    blob.upload_from_string.assert_called_once_with(
        data,
        if_generation_match=0,
        retry=DEFAULT_RETRY_IF_GENERATION_SPECIFIED,
    )
