from datetime import datetime, timezone

import pytest

from schedule.__main__ import resolve_schedule_request
from schedule.core import FullSnapshotRequest, UpdateRequest, Weekday


NOW = datetime(2026, 10, 3, 7, 0, tzinfo=timezone.utc)  # Saturday


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


def test_update_rejects_invalid_update_day() -> None:
    with pytest.raises(ValueError):
        resolve_schedule_request(
            full_snapshot=False,
            update_day="funday",
            now=NOW,
        )
