from __future__ import annotations

import json

import pytest
from google.cloud import bigquery

from schedule_loader.schema import (
    SchemaMismatchError,
    assert_schemas_equal,
    load_bigquery_schema,
)


def test_load_bigquery_schema(tmp_path) -> None:
    path = tmp_path / "schema.json"

    path.write_text(
        json.dumps(
            [
                {
                    "name": "load_id",
                    "type": "STRING",
                    "mode": "REQUIRED",
                    "description": "Some description",
                },
                {
                    "name": "publication_sequence",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                },
            ]
        ),
        encoding="utf-8",
    )

    result = load_bigquery_schema(path)

    assert result == (
        bigquery.SchemaField(
            "load_id",
            "STRING",
            mode="REQUIRED",
            description="Some description",
        ),
        bigquery.SchemaField(
            "publication_sequence",
            "INTEGER",
            mode="REQUIRED",
        ),
    )


def test_schema_comparison_accepts_matching_schema() -> None:
    schema = (
        bigquery.SchemaField(
            "load_id",
            "STRING",
            mode="REQUIRED",
        ),
    )

    assert_schemas_equal(
        expected=schema,
        actual=schema,
    )


def test_schema_comparison_ignores_descriptions() -> None:
    expected = (
        bigquery.SchemaField(
            "load_id",
            "STRING",
            mode="REQUIRED",
            description="local",
        ),
    )

    actual = (
        bigquery.SchemaField(
            "load_id",
            "STRING",
            mode="REQUIRED",
            description="remote",
        ),
    )

    assert_schemas_equal(
        expected=expected,
        actual=actual,
    )


def test_schema_mismatch_reports_expected_and_actual_fields() -> None:
    expected = (
        bigquery.SchemaField(
            "publication_sequence",
            "INTEGER",
            mode="REQUIRED",
        ),
    )

    actual = (
        bigquery.SchemaField(
            "publication_sequence",
            "STRING",
            mode="NULLABLE",
        ),
    )

    with pytest.raises(
        SchemaMismatchError,
    ) as exc_info:
        assert_schemas_equal(
            expected=expected,
            actual=actual,
        )

    message = str(exc_info.value)

    assert "publication_sequence" in message
    assert "INTEGER" in message
    assert "STRING" in message
    assert "REQUIRED" in message
    assert "NULLABLE" in message


def test_schema_mismatch_reports_nested_field_path() -> None:
    expected = (
        bigquery.SchemaField(
            "schedule_segment",
            "RECORD",
            fields=(
                bigquery.SchemaField(
                    "schedule_location",
                    "RECORD",
                    mode="REPEATED",
                    fields=(
                        bigquery.SchemaField(
                            "tiploc_code",
                            "STRING",
                            mode="REQUIRED",
                        ),
                    ),
                ),
            ),
        ),
    )

    actual = (
        bigquery.SchemaField(
            "schedule_segment",
            "RECORD",
            fields=(
                bigquery.SchemaField(
                    "schedule_location",
                    "RECORD",
                    mode="REPEATED",
                    fields=(
                        bigquery.SchemaField(
                            "tiploc_code",
                            "INTEGER",
                            mode="REQUIRED",
                        ),
                    ),
                ),
            ),
        ),
    )

    with pytest.raises(
        SchemaMismatchError,
        match=(
            "schedule_segment"
            r"\.schedule_location"
            r"\.tiploc_code"
        ),
    ):
        assert_schemas_equal(
            expected=expected,
            actual=actual,
        )


def test_schema_comparison_rejects_different_field_count() -> None:
    expected = (
        bigquery.SchemaField("first", "STRING"),
        bigquery.SchemaField("second", "STRING"),
    )

    actual = (
        bigquery.SchemaField("first", "STRING"),
    )

    with pytest.raises(
        SchemaMismatchError,
        match="expected 2 fields, got 1",
    ):
        assert_schemas_equal(
            expected=expected,
            actual=actual,
        )


def test_schema_comparison_rejects_different_field_order() -> None:
    expected = (
        bigquery.SchemaField("first", "STRING"),
        bigquery.SchemaField("second", "STRING"),
    )

    actual = (
        bigquery.SchemaField("second", "STRING"),
        bigquery.SchemaField("first", "STRING"),
    )

    with pytest.raises(
        SchemaMismatchError,
        match="field index 0",
    ):
        assert_schemas_equal(
            expected=expected,
            actual=actual,
        )
