from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
from google.cloud import bigquery


class SchemaMismatchError(ValueError):
    """Local BigQuery schema does not match the deployed table schema."""


def load_bigquery_schema(
    path: str | Path,
) -> tuple[bigquery.SchemaField, ...]:
    raise NotImplementedError


def assert_schemas_equal(
    *,
    expected: Sequence[bigquery.SchemaField],
    actual: Sequence[bigquery.SchemaField],
) -> None:
    _compare_field_sequences(
        expected=expected,
        actual=actual,
        path="<root>",
    )


def bigquery_to_arrow_schema(
    schema: Sequence[bigquery.SchemaField],
) -> pa.Schema:
    return pa.schema(
        [
            _bigquery_to_arrow_field(field)
            for field in schema
        ]
    )


def _compare_field_sequences(
    *,
    expected: Sequence[bigquery.SchemaField],
    actual: Sequence[bigquery.SchemaField],
    path: str,
) -> None:
    if len(expected) != len(actual):
        raise SchemaMismatchError(
            f"Schema mismatch at {path}: "
            f"expected {len(expected)} fields, "
            f"got {len(actual)}"
        )

    for index, (expected_field, actual_field) in enumerate(
        zip(expected, actual, strict=True)
    ):
        field_path = (
            expected_field.name
            if path == "<root>"
            else f"{path}.{expected_field.name}"
        )

        _compare_field(
            expected=expected_field,
            actual=actual_field,
            path=field_path,
            index=index,
        )


def _compare_field(
    *,
    expected: bigquery.SchemaField,
    actual: bigquery.SchemaField,
    path: str,
    index: int,
) -> None:
    expected_contract = (
        expected.name,
        expected.field_type,
        expected.mode,
    )
    actual_contract = (
        actual.name,
        actual.field_type,
        actual.mode,
    )

    if expected_contract != actual_contract:
        raise SchemaMismatchError(
            f"Schema mismatch at {path} "
            f"(field index {index}): "
            f"expected={expected_contract!r}, "
            f"actual={actual_contract!r}"
        )

    _compare_field_sequences(
        expected=expected.fields,
        actual=actual.fields,
        path=path,
    )


def _bigquery_to_arrow_field(
    field: bigquery.SchemaField,
) -> pa.Field:
    raise NotImplementedError(
        "BigQuery to Arrow field conversion "
        "has not been implemented yet"
    )
