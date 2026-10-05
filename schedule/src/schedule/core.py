from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, time
from zoneinfo import ZoneInfo
from enum import StrEnum
from typing import assert_never


logger = logging.getLogger(__name__)


class Weekday(StrEnum):
    MON = "mon"
    TUE = "tue"
    WED = "wed"
    THU = "thu"
    FRI = "fri"
    SAT = "sat"
    SUN = "sun"


WEEKDAY_TO_INT = {
    "mon": 0,
    "tue": 1,
    "wed": 2,
    "thu": 3,
    "fri": 4,
    "sat": 5,
    "sun": 6,
}

LONDON = ZoneInfo("Europe/London")


class ExtractType(StrEnum):
    FULL = "full"
    UPDATE = "update"


@dataclass(frozen=True)
class FullSnapshotRequest:
    """Request the current full SCHEDULE snapshot."""


@dataclass(frozen=True)
class UpdateRequest:
    """Request one weekday-addressed SCHEDULE update."""

    day: Weekday


type ScheduleRequest = FullSnapshotRequest | UpdateRequest


@dataclass(frozen=True)
class ScheduleHeader:
    """Metadata from header row of SCHEDULE extract file."""

    extract_type: ExtractType
    sequence: int
    timestamp: int


@dataclass(frozen=True)
class SourceMetadata:
    """Metadata describing the upstream S3 object."""

    etag: str
    last_modified: str
    content_length: int


@dataclass(frozen=True)
class SchedulePublicationInfo:
    """Immutable facts obtained from the short probe request."""

    header: ScheduleHeader
    source_metadata: SourceMetadata


@dataclass(frozen=True)
class StoredObjectState:
    """Relevant state of an already-landed GCS object."""

    source_metadata: SourceMetadata
    generation: int


class FreshPublicationNotAvailableError(RuntimeError):
    pass


class PublicationMismatchError(RuntimeError):
    pass


class SourceMutationError(RuntimeError):
    pass


type FetchHeaderRow = Callable[
    [ScheduleRequest],
    SchedulePublicationInfo,
]

type GetObjectState = Callable[[str], StoredObjectState | None]

type UploadSchedule = Callable[
    [ScheduleRequest, str, SchedulePublicationInfo],
    None,
]

type QuarantineSchedule = Callable[
    [ScheduleRequest, str, SchedulePublicationInfo],
    None,
]


def extract_type_for_schedule_request(
    schedule_request: ScheduleRequest,
) -> ExtractType:
    match schedule_request:
        case FullSnapshotRequest():
            return ExtractType.FULL
        case UpdateRequest():
            return ExtractType.UPDATE
        case _:
            assert_never(schedule_request)


def validate_header_matches_schedule_request(
    *,
    schedule_request: ScheduleRequest,
    header: ScheduleHeader,
) -> None:
    expected_extract_type = extract_type_for_schedule_request(
        schedule_request
    )

    if header.extract_type != expected_extract_type:
        raise PublicationMismatchError(
            "SCHEDULE publication type does not match the requested extract: "
            f"expected={expected_extract_type.value}, "
            f"actual={header.extract_type.value}"
        )


def publication_is_fresh(
    *,
    schedule_request: ScheduleRequest,
    header: ScheduleHeader,
    now: datetime,
) -> bool:
    """
    Return whether the publication is fresh enough for this request.

    Full snapshots are accepted regardless of timestamp.

    Update publications are fresh when their header timestamp falls within
    the accepted publication window for the most recent occurrence of the
    requested weekday.
    """
    if isinstance(schedule_request, FullSnapshotRequest):
        return True

    requested_day = schedule_request.day
    freshness_floor = _get_freshness_floor(requested_day, now)

    publication_time = datetime.fromtimestamp(
        header.timestamp,
        tz=timezone.utc,
    )

    return publication_time >= freshness_floor


def _get_freshness_floor(day: Weekday, now: datetime) -> datetime:
    """
    Given a weekday, returns midnight of one day before of the latest date
    of that weekday before now.

    This is the freshness floor for daily updates.
    """
    local_now = now.astimezone(LONDON)
    current_date = local_now.date()
    current_weekday = current_date.weekday()

    target_weekday = WEEKDAY_TO_INT[day]

    days_back = (current_weekday - target_weekday) % 7
    latest_target_date = current_date - timedelta(days=days_back)

    return datetime.combine(
        latest_target_date - timedelta(days=1),
        time.min,
        tzinfo=LONDON,
    )


def object_name(
    schedule_request: ScheduleRequest,
    sequence: int,
) -> str:
    extract_type = extract_type_for_schedule_request(schedule_request)
    return (
        f"schedules/{extract_type.value}/"
        f"sequence={sequence}/schedules.json.gz"
    )


def quarantine_object_name(
    schedule_request: ScheduleRequest,
    sequence: int,
    observed_at: datetime,
) -> str:
    extract_type = extract_type_for_schedule_request(schedule_request)

    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")

    timestamp = (
        observed_at.astimezone(timezone.utc)
        .strftime("%Y-%m-%dT%H-%M-%S.%fZ")
    )

    return (
        f"schedules/quarantine/{extract_type.value}/"
        f"sequence={sequence}/{timestamp}.json.gz"
    )


def fetch_and_upload_schedule(
    *,
    schedule_request: ScheduleRequest,
    require_fresh_publication: bool,
    fetch_header_row: FetchHeaderRow,
    get_object_state: GetObjectState,
    upload_schedule: UploadSchedule,
    quarantine_schedule: QuarantineSchedule,
    utc_now: Callable[[], datetime],
) -> None:
    """
    Probe one SCHEDULE publication and decide whether to upload, no-op,
    quarantine, or fail.

    This is deliberately orchestration only. HTTP probing, partial gzip
    decompression, full HTTP streaming, and GCS streaming belong in adapters.
    """
    now = utc_now()

    pub_info = fetch_header_row(schedule_request)

    validate_header_matches_schedule_request(
        schedule_request=schedule_request,
        header=pub_info.header,
    )

    if not publication_is_fresh(
        schedule_request=schedule_request,
        header=pub_info.header,
        now=now,
    ):
        if require_fresh_publication:
            raise FreshPublicationNotAvailableError

        extra = {
            "event": "schedule_publication_stale",
            "extract_type": extract_type_for_schedule_request(
                schedule_request
            ).value,
            "sequence": pub_info.header.sequence,
            "timestamp": pub_info.header.timestamp,
        }

        match schedule_request:
            case UpdateRequest(day=day):
                extra["update_day"] = day.value
            case FullSnapshotRequest():
                pass

        logger.warning(
            "Fresh SCHEDULE publication not yet available",
            extra=extra,
        )
        return

    name = object_name(
        schedule_request=schedule_request,
        sequence=pub_info.header.sequence,
    )
    object_state = get_object_state(name)

    if object_state is None:
        upload_schedule(
            schedule_request,
            name,
            pub_info,
        )
        return

    if object_state.source_metadata == pub_info.source_metadata:
        logger.info(
            "Published SCHEDULE metadata matches existing object. Exiting..."
        )
        return

    logger.info(
        "Published SCHEDULE metadata does not match existing object. Quarantining published file"
    )

    quarantine_schedule(
        schedule_request,
        quarantine_object_name(
            schedule_request,
            pub_info.header.sequence,
            now,
        ),
        pub_info,
    )
    raise SourceMutationError
