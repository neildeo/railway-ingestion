from __future__ import annotations

import pyarrow as pa
import pytest
from google.cloud import bigquery

from schedule_loader.arrow import (
    bigquery_to_arrow_schema,
    iter_rows_as_arrow_batches,
)


TEST_SCHEMA = (
    bigquery.SchemaField(
        "id",
        "INTEGER",
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        "name",
        "STRING",
        mode="REQUIRED",
    ),
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


def test_iter_rows_as_arrow_batches_splits_rows_by_batch_size() -> None:
    rows = (
        {
            "id": i,
            "name": f"row-{i}",
        }
        for i in range(5)
    )

    batches = list(
        iter_rows_as_arrow_batches(
            rows,
            bq_schema=TEST_SCHEMA,
            batch_size=2,
        )
    )

    assert [
        batch.num_rows
        for batch in batches
    ] == [2, 2, 1]


def test_iter_rows_as_arrow_batches_preserves_row_order() -> None:
    rows = (
        {
            "id": i,
            "name": f"row-{i}",
        }
        for i in range(5)
    )

    batches = list(
        iter_rows_as_arrow_batches(
            rows,
            bq_schema=TEST_SCHEMA,
            batch_size=2,
        )
    )

    result = [
        row
        for batch in batches
        for row in batch.to_pylist()
    ]

    assert result == [
        {
            "id": 0,
            "name": "row-0",
        },
        {
            "id": 1,
            "name": "row-1",
        },
        {
            "id": 2,
            "name": "row-2",
        },
        {
            "id": 3,
            "name": "row-3",
        },
        {
            "id": 4,
            "name": "row-4",
        },
    ]


def test_iter_rows_as_arrow_batches_uses_expected_schema() -> None:
    rows = (
        {
            "id": i,
            "name": f"row-{i}",
        }
        for i in range(3)
    )

    batches = list(
        iter_rows_as_arrow_batches(
            rows,
            bq_schema=TEST_SCHEMA,
            batch_size=2,
        )
    )

    expected_schema = bigquery_to_arrow_schema(
        TEST_SCHEMA
    )

    assert all(
        batch.schema == expected_schema
        for batch in batches
    )


def test_iter_rows_as_arrow_batches_returns_empty_for_empty_input() -> None:
    batches = list(
        iter_rows_as_arrow_batches(
            iter([]),
            bq_schema=TEST_SCHEMA,
            batch_size=2,
        )
    )

    assert batches == []


def test_iter_rows_as_arrow_batches_none_uses_single_batch() -> None:
    rows = (
        {
            "id": i,
            "name": f"row-{i}",
        }
        for i in range(5)
    )

    batches = list(
        iter_rows_as_arrow_batches(
            rows,
            bq_schema=TEST_SCHEMA,
            batch_size=None,
        )
    )

    assert len(batches) == 1

    assert batches[0].to_pylist() == [
        {
            "id": 0,
            "name": "row-0",
        },
        {
            "id": 1,
            "name": "row-1",
        },
        {
            "id": 2,
            "name": "row-2",
        },
        {
            "id": 3,
            "name": "row-3",
        },
        {
            "id": 4,
            "name": "row-4",
        },
    ]


def test_iter_rows_as_arrow_batches_none_returns_empty_for_empty_input() -> None:
    batches = list(
        iter_rows_as_arrow_batches(
            iter([]),
            bq_schema=TEST_SCHEMA,
            batch_size=None,
        )
    )

    assert batches == []
