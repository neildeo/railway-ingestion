from datetime import datetime, timezone

import pytest
from unittest.mock import Mock, patch

from schedule.__main__ import (
    resolve_schedule_request,
    PROJECT_ID,
    SECRET_ID,
    main,
)
from schedule.core import (
    FullSnapshotRequest,
    UpdateRequest,
    Weekday,
)


NOW = datetime(
    2026,
    10,
    3,
    7,
    0,
    tzinfo=timezone.utc,
)  # Saturday in both UTC and Europe/London


def test_full_snapshot_resolves_without_update_day() -> None:
    assert resolve_schedule_request(
        full_snapshot=True,
        update_day=None,
        now=NOW,
    ) == FullSnapshotRequest()


def test_full_snapshot_rejects_update_day() -> None:
    with pytest.raises(ValueError):
        resolve_schedule_request(
            full_snapshot=True,
            update_day="fri",
            now=NOW,
        )


def test_update_uses_explicit_update_day() -> None:
    assert resolve_schedule_request(
        full_snapshot=False,
        update_day="tue",
        now=NOW,
    ) == UpdateRequest(day=Weekday.TUE)


def test_update_defaults_to_yesterdays_weekday() -> None:
    assert resolve_schedule_request(
        full_snapshot=False,
        update_day=None,
        now=NOW,
    ) == UpdateRequest(day=Weekday.FRI)


def test_update_defaults_using_europe_london_date() -> None:
    # 23:30 UTC on Monday is 00:30 Tuesday during BST.
    # Network Rail's operational "yesterday" is therefore Monday.
    now = datetime(
        2026,
        7,
        6,
        23,
        30,
        tzinfo=timezone.utc,
    )

    assert resolve_schedule_request(
        full_snapshot=False,
        update_day=None,
        now=now,
    ) == UpdateRequest(day=Weekday.MON)


def test_update_rejects_invalid_update_day() -> None:
    with pytest.raises(ValueError):
        resolve_schedule_request(
            full_snapshot=False,
            update_day="funday",
            now=NOW,
        )


def test_main_fetches_credentials_and_creates_session_once(
    monkeypatch,
) -> None:
    monkeypatch.setenv("FULL_SNAPSHOT", "false")
    monkeypatch.setenv(
        "REQUIRE_FRESH_PUBLICATION",
        "false",
    )
    monkeypatch.delenv("UPDATE_DAY", raising=False)

    credentials = Mock()
    session = Mock()

    with (
        patch(
            "schedule.__main__.adapters."
            "get_network_rail_credentials_from_secret_manager",
            return_value=credentials,
        ) as get_credentials,
        patch(
            "schedule.__main__.adapters.create_requests_session",
            return_value=session,
        ) as create_session,
        patch(
            "schedule.__main__.fetch_and_upload_schedule",
        ) as run_ingest,
    ):
        main()

    get_credentials.assert_called_once_with(
        project_id=PROJECT_ID,
        secret_id=SECRET_ID,
    )

    create_session.assert_called_once_with(
        credentials
    )

    run_ingest.assert_called_once()
