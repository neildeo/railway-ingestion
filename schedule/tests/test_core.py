from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from schedule.core import (
    ExtractType,
    FullSnapshotRequest,
    PublicationNotAvailableError,
    ScheduleHeader,
    SchedulePublicationInfo,
    SourceMetadata,
    SourceMutationError,
    StoredObjectState,
    PublicationMismatchError,
    UpdateRequest,
    Weekday,
    fetch_and_upload_schedule,
    object_name,
    quarantine_object_name,
    validate_header_matches_schedule_request,
)


NOW = datetime(2026, 10, 3, 10, 3, 42, 381927, tzinfo=timezone.utc)

SOURCE_METADATA = SourceMetadata(
    etag='"abc123"',
    last_modified="Sat, 03 Oct 2026 00:32:35 GMT",
    content_length=123_456,
)


def publication_info(
    *,
    extract_type: ExtractType = ExtractType.UPDATE,
    sequence: int = 5246,
    source_metadata: SourceMetadata = SOURCE_METADATA,
) -> SchedulePublicationInfo:
    return SchedulePublicationInfo(
        header=ScheduleHeader(
            extract_type=extract_type,
            sequence=sequence,
            timestamp=1790987555,
        ),
        source_metadata=source_metadata,
    )


def dependencies(
    *,
    fetched: SchedulePublicationInfo | None,
    existing: StoredObjectState | None,
) -> dict[str, Mock]:
    return {
        "fetch_header_row": Mock(return_value=fetched),
        "get_object_state": Mock(return_value=existing),
        "upload_schedule": Mock(),
        "quarantine_schedule": Mock(),
        "utc_now": Mock(return_value=NOW),
    }


def test_object_name_uses_type_and_sequence() -> None:
    assert (
        object_name(FullSnapshotRequest(), 5245)
        == "schedules/full/sequence=5245/schedules.json.gz"
    )
    assert (
        object_name(UpdateRequest(day=Weekday.WED), 5246)
        == "schedules/update/sequence=5246/schedules.json.gz"
    )


def test_quarantine_object_name_uses_type_sequence_and_microsecond_timestamp() -> None:
    assert quarantine_object_name(
        UpdateRequest(day=Weekday.WED),
        5246,
        NOW,
    ) == (
        "schedules/quarantine/update/sequence=5246/"
        "2026-10-03T10-03-42.381927Z.json.gz"
    )


def test_validate_header_accepts_matching_extract_type() -> None:
    validate_header_matches_schedule_request(
        schedule_request=UpdateRequest(day=Weekday.WED),
        header=ScheduleHeader(
            extract_type=ExtractType.UPDATE,
            sequence=5246,
            timestamp=1790987555,
        ),
    )


def test_validate_header_rejects_wrong_extract_type() -> None:
    with pytest.raises(PublicationMismatchError):
        validate_header_matches_schedule_request(
            schedule_request=UpdateRequest(day=Weekday.WED),
            header=ScheduleHeader(
                extract_type=ExtractType.FULL,
                sequence=5246,
                timestamp=1790987555,
            ),
        )


def test_new_publication_is_uploaded() -> None:
    schedule_request = UpdateRequest(day=Weekday.WED)
    fetched = publication_info()
    deps = dependencies(fetched=fetched, existing=None)

    fetch_and_upload_schedule(
        schedule_request=schedule_request,
        require_publication=False,
        **deps,
    )

    deps["upload_schedule"].assert_called_once_with(
        schedule_request,
        "schedules/update/sequence=5246/schedules.json.gz",
        fetched,
    )
    deps["quarantine_schedule"].assert_not_called()


def test_existing_publication_with_matching_metadata_is_noop() -> None:
    schedule_request = UpdateRequest(day=Weekday.WED)
    fetched = publication_info()
    deps = dependencies(
        fetched=fetched,
        existing=StoredObjectState(
            source_metadata=SOURCE_METADATA,
            generation=17,
        ),
    )

    fetch_and_upload_schedule(
        schedule_request=schedule_request,
        require_publication=True,
        **deps,
    )

    deps["upload_schedule"].assert_not_called()
    deps["quarantine_schedule"].assert_not_called()


def test_existing_publication_with_different_metadata_is_quarantined_and_fails() -> None:
    schedule_request = UpdateRequest(day=Weekday.WED)
    fetched = publication_info()
    deps = dependencies(
        fetched=fetched,
        existing=StoredObjectState(
            source_metadata=SourceMetadata(
                etag='"different"',
                last_modified="Sat, 03 Oct 2026 01:15:00 GMT",
                content_length=123_999,
            ),
            generation=17,
        ),
    )

    with pytest.raises(SourceMutationError):
        fetch_and_upload_schedule(
            schedule_request=schedule_request,
            require_publication=False,
            **deps,
        )

    deps["upload_schedule"].assert_not_called()
    deps["quarantine_schedule"].assert_called_once_with(
        schedule_request,
        (
            "schedules/quarantine/update/sequence=5246/"
            "2026-10-03T10-03-42.381927Z.json.gz"
        ),
        fetched,
    )


def test_missing_publication_is_tolerated_when_not_required() -> None:
    deps = dependencies(fetched=None, existing=None)

    fetch_and_upload_schedule(
        schedule_request=UpdateRequest(day=Weekday.WED),
        require_publication=False,
        **deps,
    )

    deps["get_object_state"].assert_not_called()
    deps["upload_schedule"].assert_not_called()
    deps["quarantine_schedule"].assert_not_called()


def test_missing_publication_fails_when_required() -> None:
    deps = dependencies(fetched=None, existing=None)

    with pytest.raises(PublicationNotAvailableError):
        fetch_and_upload_schedule(
            schedule_request=UpdateRequest(day=Weekday.WED),
            require_publication=True,
            **deps,
        )

    deps["get_object_state"].assert_not_called()
    deps["upload_schedule"].assert_not_called()
    deps["quarantine_schedule"].assert_not_called()


def test_wrong_extract_type_fails_before_storage_lookup() -> None:
    deps = dependencies(
        fetched=publication_info(extract_type=ExtractType.FULL),
        existing=None,
    )

    with pytest.raises(PublicationMismatchError):
        fetch_and_upload_schedule(
            schedule_request=UpdateRequest(day=Weekday.WED),
            require_publication=True,
            **deps,
        )

    deps["get_object_state"].assert_not_called()
    deps["upload_schedule"].assert_not_called()
    deps["quarantine_schedule"].assert_not_called()
