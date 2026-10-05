from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

from schedule.core import (
    ExtractType,
    FreshPublicationNotAvailableError,
    FullSnapshotRequest,
    PublicationMismatchError,
    ScheduleHeader,
    SchedulePublicationInfo,
    SourceMetadata,
    SourceMutationError,
    StoredObjectState,
    UpdateRequest,
    Weekday,
    fetch_and_upload_schedule,
    object_name,
    publication_is_fresh,
    quarantine_object_name,
    validate_header_matches_schedule_request,
)


LONDON = ZoneInfo("Europe/London")

NOW = datetime(
    2026,
    10,
    3,
    10,
    3,
    42,
    381927,
    tzinfo=timezone.utc,
)

# NOW is Saturday 3 October 2026.
# For a Wednesday request, the most recent Wednesday is 30 September.
# The accepted freshness floor is therefore:
# Tuesday 29 September 00:00 Europe/London.
FRESHNESS_FLOOR = datetime(
    2026,
    9,
    29,
    0,
    0,
    0,
    tzinfo=LONDON,
)

FRESH_UPDATE_TIME = datetime(
    2026,
    10,
    1,
    0,
    30,
    0,
    tzinfo=LONDON,
)

STALE_UPDATE_TIME = datetime(
    2026,
    9,
    23,
    0,
    30,
    0,
    tzinfo=LONDON,
)

SOURCE_METADATA = SourceMetadata(
    etag='"abc123"',
    last_modified="Sat, 03 Oct 2026 00:32:35 GMT",
    content_length=123_456,
)


def publication_info(
    *,
    extract_type: ExtractType = ExtractType.UPDATE,
    sequence: int = 5246,
    timestamp: int | None = None,
    source_metadata: SourceMetadata = SOURCE_METADATA,
) -> SchedulePublicationInfo:
    if timestamp is None:
        timestamp = int(FRESH_UPDATE_TIME.timestamp())

    return SchedulePublicationInfo(
        header=ScheduleHeader(
            extract_type=extract_type,
            sequence=sequence,
            timestamp=timestamp,
        ),
        source_metadata=source_metadata,
    )


def dependencies(
    *,
    fetched: SchedulePublicationInfo,
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
        object_name(
            FullSnapshotRequest(),
            5245,
        )
        == "schedules/full/sequence=5245/schedules.json.gz"
    )

    assert (
        object_name(
            UpdateRequest(day=Weekday.WED),
            5246,
        )
        == "schedules/update/sequence=5246/schedules.json.gz"
    )


def test_quarantine_object_name_uses_type_sequence_and_microsecond_timestamp(
) -> None:
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
            timestamp=int(FRESH_UPDATE_TIME.timestamp()),
        ),
    )


def test_validate_header_rejects_wrong_extract_type() -> None:
    with pytest.raises(PublicationMismatchError):
        validate_header_matches_schedule_request(
            schedule_request=UpdateRequest(day=Weekday.WED),
            header=ScheduleHeader(
                extract_type=ExtractType.FULL,
                sequence=5246,
                timestamp=int(FRESH_UPDATE_TIME.timestamp()),
            ),
        )


def test_full_snapshot_is_fresh_regardless_of_timestamp() -> None:
    very_old_timestamp = int(
        datetime(
            2020,
            1,
            1,
            tzinfo=timezone.utc,
        ).timestamp()
    )

    assert publication_is_fresh(
        schedule_request=FullSnapshotRequest(),
        header=ScheduleHeader(
            extract_type=ExtractType.FULL,
            sequence=100,
            timestamp=very_old_timestamp,
        ),
        now=NOW,
    )


def test_update_at_freshness_floor_is_fresh() -> None:
    assert publication_is_fresh(
        schedule_request=UpdateRequest(day=Weekday.WED),
        header=ScheduleHeader(
            extract_type=ExtractType.UPDATE,
            sequence=5246,
            timestamp=int(FRESHNESS_FLOOR.timestamp()),
        ),
        now=NOW,
    )


def test_update_one_second_before_freshness_floor_is_stale() -> None:
    timestamp = int(
        (
            FRESHNESS_FLOOR
            - timedelta(seconds=1)
        ).timestamp()
    )

    assert not publication_is_fresh(
        schedule_request=UpdateRequest(day=Weekday.WED),
        header=ScheduleHeader(
            extract_type=ExtractType.UPDATE,
            sequence=5246,
            timestamp=timestamp,
        ),
        now=NOW,
    )


def test_recent_update_is_fresh() -> None:
    assert publication_is_fresh(
        schedule_request=UpdateRequest(day=Weekday.WED),
        header=ScheduleHeader(
            extract_type=ExtractType.UPDATE,
            sequence=5246,
            timestamp=int(FRESH_UPDATE_TIME.timestamp()),
        ),
        now=NOW,
    )


def test_previous_weeks_update_is_stale() -> None:
    assert not publication_is_fresh(
        schedule_request=UpdateRequest(day=Weekday.WED),
        header=ScheduleHeader(
            extract_type=ExtractType.UPDATE,
            sequence=5240,
            timestamp=int(STALE_UPDATE_TIME.timestamp()),
        ),
        now=NOW,
    )


def test_new_publication_is_uploaded() -> None:
    schedule_request = UpdateRequest(day=Weekday.WED)
    fetched = publication_info()

    deps = dependencies(
        fetched=fetched,
        existing=None,
    )

    fetch_and_upload_schedule(
        schedule_request=schedule_request,
        require_fresh_publication=False,
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
        require_fresh_publication=True,
        **deps,
    )

    deps["upload_schedule"].assert_not_called()
    deps["quarantine_schedule"].assert_not_called()


def test_existing_publication_with_different_metadata_is_quarantined_and_fails(
) -> None:
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
            require_fresh_publication=False,
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


def test_stale_update_is_tolerated_when_fresh_publication_not_required(
) -> None:
    fetched = publication_info(
        sequence=5240,
        timestamp=int(STALE_UPDATE_TIME.timestamp()),
    )

    deps = dependencies(
        fetched=fetched,
        existing=None,
    )

    fetch_and_upload_schedule(
        schedule_request=UpdateRequest(day=Weekday.WED),
        require_fresh_publication=False,
        **deps,
    )

    deps["get_object_state"].assert_not_called()
    deps["upload_schedule"].assert_not_called()
    deps["quarantine_schedule"].assert_not_called()


def test_stale_update_fails_when_fresh_publication_required() -> None:
    fetched = publication_info(
        sequence=5240,
        timestamp=int(STALE_UPDATE_TIME.timestamp()),
    )

    deps = dependencies(
        fetched=fetched,
        existing=None,
    )

    with pytest.raises(FreshPublicationNotAvailableError):
        fetch_and_upload_schedule(
            schedule_request=UpdateRequest(day=Weekday.WED),
            require_fresh_publication=True,
            **deps,
        )

    deps["get_object_state"].assert_not_called()
    deps["upload_schedule"].assert_not_called()
    deps["quarantine_schedule"].assert_not_called()


def test_old_full_snapshot_is_still_accepted() -> None:
    fetched = publication_info(
        extract_type=ExtractType.FULL,
        sequence=5200,
        timestamp=int(
            datetime(
                2020,
                1,
                1,
                tzinfo=timezone.utc,
            ).timestamp()
        ),
    )

    deps = dependencies(
        fetched=fetched,
        existing=None,
    )

    schedule_request = FullSnapshotRequest()

    fetch_and_upload_schedule(
        schedule_request=schedule_request,
        require_fresh_publication=True,
        **deps,
    )

    deps["upload_schedule"].assert_called_once_with(
        schedule_request,
        "schedules/full/sequence=5200/schedules.json.gz",
        fetched,
    )


def test_wrong_extract_type_fails_before_storage_lookup() -> None:
    deps = dependencies(
        fetched=publication_info(
            extract_type=ExtractType.FULL,
        ),
        existing=None,
    )

    with pytest.raises(PublicationMismatchError):
        fetch_and_upload_schedule(
            schedule_request=UpdateRequest(day=Weekday.WED),
            require_fresh_publication=True,
            **deps,
        )

    deps["get_object_state"].assert_not_called()
    deps["upload_schedule"].assert_not_called()
    deps["quarantine_schedule"].assert_not_called()


def test_utc_now_is_resolved_once_per_ingest() -> None:
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
            require_fresh_publication=True,
            **deps,
        )

    deps["utc_now"].assert_called_once_with()
