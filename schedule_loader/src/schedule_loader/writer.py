from __future__ import annotations

import pyarrow as pa
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1.writer import AppendRowsStream
from google.protobuf import wrappers_pb2


def write_pending_batch(
    *,
    client: bigquery_storage_v1.BigQueryWriteClient,
    table: str,
    batch: pa.RecordBatch,
) -> None:
    """
    Atomically write one Arrow RecordBatch to a BigQuery table.

    `table` must be the fully-qualified Storage Write API table resource:

        projects/{project}/datasets/{dataset}/tables/{table}

    Data is written to a PENDING stream and becomes visible only after
    the stream is finalized and committed.
    """
    write_stream = client.create_write_stream(
        parent=table,
        write_stream=types.WriteStream(
            type_=types.WriteStream.Type.PENDING,
        ),
    )

    request_template = _append_request_template(
        write_stream=write_stream.name,
        schema=batch.schema,
    )

    append_stream = AppendRowsStream(
        client,
        request_template,
    )

    try:
        request = _append_request(batch)

        append_future = append_stream.send(request)

        # AppendRows is asynchronous. Do not finalize the stream until
        # BigQuery has acknowledged this append.
        append_future.result()

    finally:
        append_stream.close()

    client.finalize_write_stream(
        name=write_stream.name,
    )

    commit_response = client.batch_commit_write_streams(
        request=types.BatchCommitWriteStreamsRequest(
            parent=table,
            write_streams=[write_stream.name],
        )
    )

    if commit_response.stream_errors:
        raise RuntimeError(
            "Failed to commit BigQuery write stream "
            f"{write_stream.name!r}: "
            f"{list(commit_response.stream_errors)!r}"
        )


def _append_request_template(
    *,
    write_stream: str,
    schema: pa.Schema,
) -> types.AppendRowsRequest:
    """
    Build the first-request template for an Arrow AppendRows stream.

    The stream identity and Arrow schema are sent once when the
    AppendRowsStream is created.
    """
    request = types.AppendRowsRequest(
        write_stream=write_stream,
    )

    request.arrow_rows.writer_schema.serialized_schema = (
        schema.serialize().to_pybytes()
    )

    return request


def _append_request(
    batch: pa.RecordBatch,
) -> types.AppendRowsRequest:
    """
    Build one AppendRows request containing a serialized Arrow batch.
    """
    request = types.AppendRowsRequest()

    request.arrow_rows.rows.serialized_record_batch = (
        batch.serialize().to_pybytes()
    )

    # This is the first and only append to this stream. Using an explicit
    # offset gives the append deterministic position semantics and sets us
    # up naturally for multiple batches later.
    request.offset = wrappers_pb2.Int64Value(value=0)

    return request
