from __future__ import annotations

import pytest

from schedule_load_backfill.config import (
    BackfillConfig,
    load_config_from_env,
)


def test_load_config_from_env_parses_expected_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "BQ_PROJECT",
        "railway-analytics-508615",
    )
    monkeypatch.setenv(
        "SOURCE_BUCKET",
        "railway-analytics-508615-network-rail-open-data-raw",
    )
    monkeypatch.setenv(
        "BQ_DATASET",
        "raw",
    )
    monkeypatch.setenv(
        "WORKFLOW_PARENT",
        (
            "projects/railway-analytics-508615/"
            "locations/europe-west1/"
            "workflows/railway-load-schedule-object"
        ),
    )
    monkeypatch.setenv(
        "MAX_IN_FLIGHT",
        "10",
    )
    monkeypatch.setenv(
        "POLL_INTERVAL_SECONDS",
        "10.0",
    )
    monkeypatch.setenv(
        "DRY_RUN",
        "true",
    )

    result = load_config_from_env()

    assert result == BackfillConfig(
        project="railway-analytics-508615",
        source_bucket=(
            "railway-analytics-508615-network-rail-open-data-raw"
        ),
        audit_dataset="raw",
        workflow_parent=(
            "projects/railway-analytics-508615/"
            "locations/europe-west1/"
            "workflows/railway-load-schedule-object"
        ),
        max_in_flight=10,
        poll_interval_seconds=10.0,
        dry_run=True,
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("true", True),
        ("TRUE", True),
        ("True", True),
        ("false", False),
        ("FALSE", False),
        ("False", False),
    ],
)
def test_load_config_from_env_parses_dry_run_case_insensitively(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    expected: bool,
) -> None:
    monkeypatch.setenv(
        "BQ_PROJECT",
        "railway-analytics-508615",
    )
    monkeypatch.setenv(
        "SOURCE_BUCKET",
        "bucket",
    )
    monkeypatch.setenv(
        "BQ_DATASET",
        "raw",
    )
    monkeypatch.setenv(
        "WORKFLOW_PARENT",
        "workflow-parent",
    )
    monkeypatch.setenv(
        "MAX_IN_FLIGHT",
        "10",
    )
    monkeypatch.setenv(
        "POLL_INTERVAL_SECONDS",
        "10",
    )
    monkeypatch.setenv(
        "DRY_RUN",
        value,
    )

    result = load_config_from_env()

    assert result.dry_run is expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        "yes",
        "no",
        "1",
        "0",
        "banana",
    ],
)
def test_load_config_from_env_rejects_invalid_dry_run_value(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv(
        "BQ_PROJECT",
        "railway-analytics-508615",
    )
    monkeypatch.setenv(
        "SOURCE_BUCKET",
        "bucket",
    )
    monkeypatch.setenv(
        "BQ_DATASET",
        "raw",
    )
    monkeypatch.setenv(
        "WORKFLOW_PARENT",
        "workflow-parent",
    )
    monkeypatch.setenv(
        "MAX_IN_FLIGHT",
        "10",
    )
    monkeypatch.setenv(
        "POLL_INTERVAL_SECONDS",
        "10",
    )
    monkeypatch.setenv(
        "DRY_RUN",
        value,
    )

    with pytest.raises(ValueError):
        load_config_from_env()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("MAX_IN_FLIGHT", "not-an-int"),
        ("POLL_INTERVAL_SECONDS", "not-a-float"),
    ],
)
def test_load_config_from_env_rejects_invalid_numeric_values(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    monkeypatch.setenv(
        "BQ_PROJECT",
        "railway-analytics-508615",
    )
    monkeypatch.setenv(
        "SOURCE_BUCKET",
        "bucket",
    )
    monkeypatch.setenv(
        "BQ_DATASET",
        "raw",
    )
    monkeypatch.setenv(
        "WORKFLOW_PARENT",
        "workflow-parent",
    )
    monkeypatch.setenv(
        "MAX_IN_FLIGHT",
        "10",
    )
    monkeypatch.setenv(
        "POLL_INTERVAL_SECONDS",
        "10",
    )
    monkeypatch.setenv(
        "DRY_RUN",
        "false",
    )

    monkeypatch.setenv(
        name,
        value,
    )

    with pytest.raises(ValueError):
        load_config_from_env()


def test_load_config_from_env_rejects_missing_required_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "SOURCE_BUCKET",
        "bucket",
    )
    monkeypatch.setenv(
        "BQ_DATASET",
        "raw",
    )
    monkeypatch.setenv(
        "WORKFLOW_PARENT",
        "workflow-parent",
    )
    monkeypatch.setenv(
        "MAX_IN_FLIGHT",
        "10",
    )
    monkeypatch.setenv(
        "POLL_INTERVAL_SECONDS",
        "10",
    )
    monkeypatch.setenv(
        "DRY_RUN",
        "false",
    )

    monkeypatch.delenv(
        "BQ_PROJECT",
        raising=False,
    )

    with pytest.raises(KeyError):
        load_config_from_env()
