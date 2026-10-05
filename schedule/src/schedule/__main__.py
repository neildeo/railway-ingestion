from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from functools import partial

from schedule.core import (
    FullSnapshotRequest,
    ScheduleRequest,
    UpdateRequest,
    Weekday,
    fetch_and_upload_schedule,
)

from schedule import adapters

from schedule.logging_config import configure_logging

logger = logging.getLogger(__name__)

PROJECT_ID = "railway-analytics-508615"

SECRET_ID = "network-rail-credentials"

_LONDON = ZoneInfo("Europe/London")

_WEEKDAY_BY_PYTHON_WEEKDAY = (
    Weekday.MON,
    Weekday.TUE,
    Weekday.WED,
    Weekday.THU,
    Weekday.FRI,
    Weekday.SAT,
    Weekday.SUN,
)


def parse_bool_env(name: str) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        raise ValueError(f"{name} must be set")

    match raw.strip().lower():
        case "true":
            return True
        case "false":
            return False
        case _:
            raise ValueError(f"{name} must be 'true' or 'false'")


def resolve_schedule_request(
    *,
    full_snapshot: bool,
    update_day: str | None,
    now: datetime,
) -> ScheduleRequest:
    """
    Resolve runtime configuration into a valid ScheduleRequest.

    The core ingest never receives an update request without a concrete day.
    If UPDATE_DAY is absent, main resolves it to yesterday's weekday in the
    Network Rail operational timezone.
    """
    if full_snapshot:
        if update_day is not None:
            raise ValueError(
                "UPDATE_DAY must not be set when FULL_SNAPSHOT=true"
            )
        return FullSnapshotRequest()

    if update_day is not None:
        try:
            return UpdateRequest(
                day=Weekday(update_day.strip().lower())
            )
        except ValueError as exc:
            raise ValueError(
                "UPDATE_DAY must be one of: "
                "mon, tue, wed, thu, fri, sat, sun"
            ) from exc

    yesterday = (
        now.astimezone(_LONDON).date()
        - timedelta(days=1)
    )

    return UpdateRequest(
        day=_WEEKDAY_BY_PYTHON_WEEKDAY[yesterday.weekday()]
    )


def main() -> None:
    """Runtime/configuration boundary."""
    configure_logging()

    full_snapshot = parse_bool_env("FULL_SNAPSHOT")
    require_fresh_publication = parse_bool_env(
        "REQUIRE_FRESH_PUBLICATION"
    )
    update_day = os.environ.get("UPDATE_DAY")

    now = datetime.now(timezone.utc)

    schedule_request = resolve_schedule_request(
        full_snapshot=full_snapshot,
        update_day=update_day,
        now=now,
    )

    logger.info(
        "Starting SCHEDULE ingestion",
        extra={
            "event": "schedule_ingest_started",
            "schedule_request": repr(schedule_request),
            "require_fresh_publication": require_fresh_publication,
        },
    )

    credentials = (
        adapters.get_network_rail_credentials_from_secret_manager(
            project_id=PROJECT_ID,
            secret_id=SECRET_ID,
        )
    )

    session = adapters.create_requests_session(
        credentials
    )

    fetch_and_upload_schedule(
        schedule_request=schedule_request,
        require_fresh_publication=require_fresh_publication,
        fetch_header_row=partial(
            adapters.fetch_header_row,
            session=session,
        ),
        get_object_state=adapters.get_object_state,
        upload_schedule=partial(
            adapters.upload_schedule,
            session=session,
        ),
        quarantine_schedule=partial(
            adapters.quarantine_schedule,
            session=session,
        ),
        utc_now=lambda: now,
    )

    logger.info(
        "SCHEDULE ingestion completed successfully",
        extra={
            "event": "schedule_ingest_completed",
            "schedule_request": repr(schedule_request),
        },
    )


if __name__ == "__main__":
    main()
