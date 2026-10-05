from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from schedule.core import (
    FullSnapshotRequest,
    ScheduleRequest,
    UpdateRequest,
    Weekday,
)


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
    full_snapshot = parse_bool_env("FULL_SNAPSHOT")
    require_fresh_publication = parse_bool_env(
        "REQUIRE_FRESH_PUBLICATION"
    )
    update_day = os.environ.get("UPDATE_DAY")

    schedule_request = resolve_schedule_request(
        full_snapshot=full_snapshot,
        update_day=update_day,
        now=datetime.now(timezone.utc),
    )

    # Later:
    #
    # fetch_and_upload_schedule(
    #     schedule_request=schedule_request,
    #     require_fresh_publication=require_fresh_publication,
    #     fetch_header_row=real_fetch_header_row,
    #     get_object_state=real_get_object_state,
    #     upload_schedule=real_upload_schedule,
    #     quarantine_schedule=real_quarantine_schedule,
    #     utc_now=lambda: datetime.now(timezone.utc),
    # )

    _ = schedule_request, require_fresh_publication


if __name__ == "__main__":
    main()
