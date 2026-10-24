from __future__ import annotations

import json

import pyarrow as pa
import pytest
from google.cloud import bigquery

from schedule_loader.schema import (
    SchemaMismatchError,
    assert_schemas_equal,
    bigquery_to_arrow_schema,
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


@pytest.mark.parametrize(
    ("bq_type", "arrow_type"),
    [
        ("STRING", pa.string()),
        ("INTEGER", pa.int64()),
        ("BOOLEAN", pa.bool_()),
        ("DATE", pa.date32()),
        (
            "TIMESTAMP",
            pa.timestamp("us", tz="UTC"),
        ),
    ],
)
def test_scalar_bigquery_types_map_to_arrow(
    bq_type,
    arrow_type,
) -> None:
    schema = (
        bigquery.SchemaField(
            "value",
            bq_type,
        ),
    )

    result = bigquery_to_arrow_schema(schema)

    assert result.field("value").type == arrow_type


def test_record_maps_recursively_to_struct() -> None:
    schema = (
        bigquery.SchemaField(
            "segment",
            "RECORD",
            fields=(
                bigquery.SchemaField(
                    "code",
                    "STRING",
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "speed",
                    "INTEGER",
                ),
            ),
        ),
    )

    result = bigquery_to_arrow_schema(schema)

    assert result.field("segment").type == pa.struct(
        [
            pa.field("code", pa.string()),
            pa.field("speed", pa.int64()),
        ]
    )


def test_repeated_record_maps_to_list_of_structs() -> None:
    schema = (
        bigquery.SchemaField(
            "locations",
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
    )

    result = bigquery_to_arrow_schema(schema)

    assert result.field("locations").type == pa.list_(
        pa.struct(
            [
                pa.field(
                    "tiploc_code",
                    pa.string(),
                )
            ]
        )
    )


def test_arrow_nullability_does_not_encode_bigquery_mode() -> None:
    schema = (
        bigquery.SchemaField(
            "required_value",
            "STRING",
            mode="REQUIRED",
        ),
    )

    result = bigquery_to_arrow_schema(schema)

    assert result.field(
        "required_value"
    ).nullable


def test_unsupported_bigquery_type_raises_not_implemented() -> None:
    schema = (
        bigquery.SchemaField(
            "amount",
            "NUMERIC",
        ),
    )

    with pytest.raises(
        NotImplementedError,
        match="NUMERIC",
    ):
        bigquery_to_arrow_schema(schema)
