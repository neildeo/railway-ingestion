from datetime import date
from unittest.mock import Mock

from corpus.core import ObjectState, object_name, sync_corpus


def test_object_name_uses_acquisition_date() -> None:
    assert (
        object_name(date(2026, 9, 25))
        == "corpus/acquired_date=2026-09-25/corpus.json"
    )


def test_matching_object_is_not_uploaded() -> None:
    content = b'{"foo":"bar"}'

    fetch_latest_corpus_file = Mock(return_value=content)
    crc32c_checksum = Mock(return_value="abc")
    get_object_state = Mock(
        return_value=ObjectState(
            crc32c="abc",
            generation=123,
        )
    )
    upload_object = Mock()

    sync_corpus(
        acquisition_date=date(2026, 9, 25),
        fetch_latest_corpus_file=fetch_latest_corpus_file,
        crc32c_checksum=crc32c_checksum,
        get_object_state=get_object_state,
        upload_object=upload_object,
    )

    upload_object.assert_not_called()


def test_missing_object_is_uploaded() -> None:
    content = b'{"foo":"bar"}'

    fetch_latest_corpus_file = Mock(return_value=content)
    crc32c_checksum = Mock(return_value="abc")
    get_object_state = Mock(return_value=None)
    upload_object = Mock()

    sync_corpus(
        acquisition_date=date(2026, 9, 25),
        fetch_latest_corpus_file=fetch_latest_corpus_file,
        crc32c_checksum=crc32c_checksum,
        get_object_state=get_object_state,
        upload_object=upload_object,
    )

    upload_object.assert_called_once_with(
        "corpus/acquired_date=2026-09-25/corpus.json",
        content,
        0,
    )


def test_changed_object_is_uploaded_against_existing_generation() -> None:
    content = b'{"foo":"new"}'

    fetch_latest_corpus_file = Mock(return_value=content)
    crc32c_checksum = Mock(return_value="new-crc")
    get_object_state = Mock(
        return_value=ObjectState(
            crc32c="old-crc",
            generation=456,
        )
    )
    upload_object = Mock()

    sync_corpus(
        acquisition_date=date(2026, 9, 25),
        fetch_latest_corpus_file=fetch_latest_corpus_file,
        crc32c_checksum=crc32c_checksum,
        get_object_state=get_object_state,
        upload_object=upload_object,
    )

    upload_object.assert_called_once_with(
        "corpus/acquired_date=2026-09-25/corpus.json",
        content,
        456,
    )
