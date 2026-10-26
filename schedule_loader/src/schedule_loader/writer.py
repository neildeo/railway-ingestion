from __future__ import annotations
from typing import Iterator
from itertools import chain

import pyarrow as pa
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1.writer import AppendRowsStream
from google.protobuf import wrappers_pb2


def write_pending_batches(
    *,
    client: bigquery_storage_v1.BigQueryWriteClient,
    table: str,
    batches: Iterator[pa.RecordBatch]
):
    """
    Atomically write a sequence of Arrow `RecordBatch`es to a BigQuery table.

    `table` must be the fully-qualified Storage Write API table resource:

        projects/{project}/datasets/{dataset}/tables/{table}

    Data is written to a PENDING stream and becomes visible only after
    the stream is finalized and committed.

    This function does not control the batching strategy - this 
    responsibility falls to the caller.
    """
    # We need to peek the first batch to:
    # (a) get the schema without advancing the original iterator;
    # (b) check if batches is empty
    batches = iter(batches)

    try:
        first_batch = next(batches)
    except StopIteration:
        return

    pa_schema = first_batch.schema

    batches = chain(
        (first_batch,),
        batches,
    )

    write_stream = client.create_write_stream(
        parent=table,
        write_stream=types.WriteStream(
            type_=types.WriteStream.Type.PENDING,
        ),
    )

    request_template = _append_request_template(
        write_stream=write_stream.name,
        schema=pa_schema,
    )
    append_stream = AppendRowsStream(
        client,
        request_template,
    )

    try:
        # offset = wrappers_pb2.Int64Value(value=0)
        offset = 0
        for batch in batches:
            request = _append_request(batch)
            request.offset = wrappers_pb2.Int64Value(value=offset)

            append_future = append_stream.send(request)

            # AppendRows is asynchronous. Do not finalize the stream until
            # BigQuery has acknowledged this append.
            append_future.result()
            offset += batch.num_rows
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

    Caller is responsible for setting the offset.
    """
    request = types.AppendRowsRequest()

    request.arrow_rows.rows.serialized_record_batch = (
        batch.serialize().to_pybytes()
    )

    return request
