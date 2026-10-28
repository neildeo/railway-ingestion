from __future__ import annotations

from pathlib import Path

import pytest

import schedule_loader.config as config_module
from schedule_loader.config import (
    LoadConfig,
    load_config_from_env,
)
from schedule_loader.source import RecordType


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
        "RECORD_TYPE",
        "publication",
    )


def test_load_config_from_env(
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

    result = load_config_from_env()

    assert result == LoadConfig(
        source_bucket="railway-raw",
        source_object=(
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
        source_generation=123456789,
        project="test-project",
        dataset="integration_test",
        record_type=RecordType.PUBLICATION,
        table_name="schedule_publications",
        schema_path=(
            schema_root
            / "schedule_publications.json"
        ),
    )


def test_load_config_parses_generation_as_integer(
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

    result = load_config_from_env()

    assert result.source_generation == 987654321
    assert isinstance(
        result.source_generation,
        int,
    )


def test_load_config_derives_publication_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_required_env(monkeypatch)

    monkeypatch.setattr(
        config_module,
        "SCHEMA_ROOT",
        tmp_path,
    )

    result = load_config_from_env()

    assert result.record_type is RecordType.PUBLICATION
    assert result.table_name == "schedule_publications"
    assert result.schema_path == (
        tmp_path / "schedule_publications.json"
    )


def test_load_config_rejects_invalid_generation(
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
        load_config_from_env()


def test_load_config_rejects_unknown_record_type(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_required_env(monkeypatch)

    monkeypatch.setenv(
        "RECORD_TYPE",
        "definitely-not-a-record-type",
    )

    monkeypatch.setattr(
        config_module,
        "SCHEMA_ROOT",
        tmp_path,
    )

    with pytest.raises(ValueError):
        load_config_from_env()


@pytest.mark.parametrize(
    "missing_variable",
    [
        "SOURCE_BUCKET",
        "SOURCE_OBJECT",
        "SOURCE_GENERATION",
        "BQ_PROJECT",
        "BQ_DATASET",
        "RECORD_TYPE",
    ],
)
def test_load_config_requires_environment_variables(
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
        load_config_from_env()
