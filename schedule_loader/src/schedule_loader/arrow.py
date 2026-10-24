import pyarrow as pa
from typing import Sequence, Any
from google.cloud import bigquery


def bigquery_to_arrow_schema(
    schema: Sequence[bigquery.SchemaField],
) -> pa.Schema:
    return pa.schema(
        [
            _bigquery_to_arrow_field(field)
            for field in schema
        ]
    )


def batch_rows_as_arrow(rows: list[dict[str, Any]], bq_schema: Sequence[bigquery.SchemaField]) -> pa.RecordBatch:
    arrow_schema = bigquery_to_arrow_schema(bq_schema)

    return pa.RecordBatch.from_pylist(
        rows,
        schema=arrow_schema,
    )


def _bigquery_to_arrow_field(
    field: bigquery.SchemaField,
) -> pa.Field:
    return pa.field(
        field.name,
        type=_bigquery_to_arrow_type(field),
        nullable=True,
    )


def _bigquery_to_arrow_type(
    bq_field: bigquery.SchemaField,
) -> pa.DataType:
    match bq_field.field_type:
        case "STRING":
            arrow_type = pa.string()

        case "INTEGER" | "INT64":
            arrow_type = pa.int64()

        case "BOOLEAN" | "BOOL":
            arrow_type = pa.bool_()

        case "DATE":
            arrow_type = pa.date32()

        case "TIMESTAMP":
            arrow_type = pa.timestamp(
                "us",
                tz="UTC",
            )

        case "RECORD" | "STRUCT":
            arrow_type = pa.struct(
                [
                    _bigquery_to_arrow_field(field)
                    for field in bq_field.fields
                ]
            )

        case _:
            raise NotImplementedError(
                "PyArrow conversion not supported "
                f"for BigQuery type: {bq_field.field_type}"
            )

    if bq_field.mode == "REPEATED":
        return pa.list_(arrow_type)

    return arrow_type
