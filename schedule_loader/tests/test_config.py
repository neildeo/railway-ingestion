from __future__ import annotations

from pathlib import Path

import pytest

import schedule_loader.config as config_module
from schedule_loader.config import (
    PublicationLoadConfig,
    publication_config_from_env,
)


def _set_required_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "SOURCE_BUCKET",
        "railway-raw",
    )
    monkeypatch.setenv(
        "SOURCE_OBJECT",
        (
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
    )
    monkeypatch.setenv(
        "SOURCE_GENERATION",
        "123456789",
    )
    monkeypatch.setenv(
        "BQ_PROJECT",
        "test-project",
    )
    monkeypatch.setenv(
        "BQ_DATASET",
        "integration_test",
    )
    monkeypatch.setenv(
        "BQ_TABLE",
        "schedule_publications",
    )


def test_publication_config_from_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_required_env(monkeypatch)

    schema_root = tmp_path / "schedule_schemas"

    monkeypatch.setattr(
        config_module,
        "SCHEMA_ROOT",
        schema_root,
    )

    result = publication_config_from_env()

    assert result == PublicationLoadConfig(
        source_bucket="railway-raw",
        source_object=(
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
        source_generation=123456789,
        project="test-project",
        dataset="integration_test",
        table_name="schedule_publications",
        schema_path=(
            schema_root
            / "schedule_publications.json"
        ),
    )


def test_publication_config_parses_generation_as_integer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_required_env(monkeypatch)

    monkeypatch.setenv(
        "SOURCE_GENERATION",
        "987654321",
    )

    monkeypatch.setattr(
        config_module,
        "SCHEMA_ROOT",
        tmp_path,
    )

    result = publication_config_from_env()

    assert result.source_generation == 987654321
    assert isinstance(
        result.source_generation,
        int,
    )


def test_publication_config_rejects_invalid_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_required_env(monkeypatch)

    monkeypatch.setenv(
        "SOURCE_GENERATION",
        "not-an-integer",
    )

    monkeypatch.setattr(
        config_module,
        "SCHEMA_ROOT",
        tmp_path,
    )

    with pytest.raises(ValueError):
        publication_config_from_env()


@pytest.mark.parametrize(
    "missing_variable",
    [
        "SOURCE_BUCKET",
        "SOURCE_OBJECT",
        "SOURCE_GENERATION",
        "BQ_PROJECT",
        "BQ_DATASET",
        "BQ_TABLE",
    ],
)
def test_publication_config_requires_environment_variables(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    missing_variable: str,
) -> None:
    _set_required_env(monkeypatch)

    monkeypatch.delenv(
        missing_variable,
        raising=False,
    )

    monkeypatch.setattr(
        config_module,
        "SCHEMA_ROOT",
        tmp_path,
    )

    with pytest.raises(
        KeyError,
        match=missing_variable,
    ):
        publication_config_from_env()
