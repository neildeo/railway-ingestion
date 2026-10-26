from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa
import pytest

import schedule_loader.writer as writer_module
from schedule_loader.writer import write_pending_batches


TABLE = (
    "projects/test-project/"
    "datasets/raw/"
    "tables/schedule_tiplocs"
)


def _batch(*values: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(
        [
            {"value": value}
            for value in values
        ],
        schema=pa.schema(
            [
                pa.field(
                    "value",
                    pa.int64(),
                )
            ]
        ),
    )


class FakeAppendFuture:
    def __init__(self) -> None:
        self.result_called = False

    def result(self) -> None:
        self.result_called = True


class FakeAppendRowsStream:
    instances: list["FakeAppendRowsStream"] = []

    def __init__(
        self,
        client,
        request_template,
    ) -> None:
        self.client = client
        self.request_template = request_template
        self.requests = []
        self.futures: list[FakeAppendFuture] = []
        self.closed = False

        self.__class__.instances.append(self)

    def send(self, request):
        future = FakeAppendFuture()

        self.requests.append(request)
        self.futures.append(future)

        return future

    def close(self) -> None:
        self.closed = True


class FakeWriteClient:
    def __init__(
        self,
        *,
        stream_errors=None,
    ) -> None:
        self.stream_errors = (
            []
            if stream_errors is None
            else stream_errors
        )

        self.create_calls = []
        self.finalize_calls = []
        self.commit_calls = []

    def create_write_stream(
        self,
        *,
        parent,
        write_stream,
    ):
        self.create_calls.append(
            {
                "parent": parent,
                "write_stream": write_stream,
            }
        )

        return SimpleNamespace(
            name=(
                f"{TABLE}/streams/test-stream"
            )
        )

    def finalize_write_stream(
        self,
        *,
        name,
    ):
        self.finalize_calls.append(name)

    def batch_commit_write_streams(
        self,
        *,
        request,
    ):
        self.commit_calls.append(request)

        return SimpleNamespace(
            stream_errors=self.stream_errors,
        )


@pytest.fixture(autouse=True)
def fake_append_rows_stream(
    monkeypatch: pytest.MonkeyPatch,
):
    FakeAppendRowsStream.instances = []

    monkeypatch.setattr(
        writer_module,
        "AppendRowsStream",
        FakeAppendRowsStream,
    )


def test_write_pending_batches_appends_all_batches_to_one_stream() -> None:
    client = FakeWriteClient()

    batches = iter(
        [
            _batch(1, 2),
            _batch(3, 4),
            _batch(5),
        ]
    )

    write_pending_batches(
        client=client,  # pyright: ignore[reportArgumentType]
        table=TABLE,
        batches=batches,
    )

    assert len(client.create_calls) == 1
    assert len(FakeAppendRowsStream.instances) == 1

    append_stream = FakeAppendRowsStream.instances[0]

    assert len(append_stream.requests) == 3


def test_write_pending_batches_uses_cumulative_row_offsets() -> None:
    client = FakeWriteClient()

    batches = iter(
        [
            _batch(1, 2),
            _batch(3, 4),
            _batch(5),
        ]
    )

    write_pending_batches(
        client=client,  # pyright: ignore[reportArgumentType]
        table=TABLE,
        batches=batches,
    )

    append_stream = FakeAppendRowsStream.instances[0]

    assert [
        request.offset
        for request in append_stream.requests
    ] == [0, 2, 4]


def test_write_pending_batches_sends_each_record_batch() -> None:
    client = FakeWriteClient()

    batches = [
        _batch(1, 2),
        _batch(3),
    ]

    write_pending_batches(
        client=client,  # pyright: ignore[reportArgumentType]
        table=TABLE,
        batches=iter(batches),
    )

    append_stream = FakeAppendRowsStream.instances[0]

    assert [
        request.arrow_rows.rows.serialized_record_batch
        for request in append_stream.requests
    ] == [
        batch.serialize().to_pybytes()
        for batch in batches
    ]


def test_write_pending_batches_waits_for_every_append() -> None:
    client = FakeWriteClient()

    write_pending_batches(
        client=client,  # pyright: ignore[reportArgumentType]
        table=TABLE,
        batches=iter(
            [
                _batch(1, 2),
                _batch(3, 4),
                _batch(5),
            ]
        ),
    )

    append_stream = FakeAppendRowsStream.instances[0]

    assert all(
        future.result_called
        for future in append_stream.futures
    )


def test_write_pending_batches_finalizes_and_commits_once() -> None:
    client = FakeWriteClient()

    write_pending_batches(
        client=client,  # pyright: ignore[reportArgumentType]
        table=TABLE,
        batches=iter(
            [
                _batch(1, 2),
                _batch(3, 4),
            ]
        ),
    )

    stream_name = (
        f"{TABLE}/streams/test-stream"
    )

    assert client.finalize_calls == [
        stream_name,
    ]

    assert len(client.commit_calls) == 1

    commit_request = client.commit_calls[0]

    assert commit_request.parent == TABLE
    assert list(
        commit_request.write_streams
    ) == [stream_name]


def test_write_pending_batches_closes_append_stream() -> None:
    client = FakeWriteClient()

    write_pending_batches(
        client=client,  # pyright: ignore[reportArgumentType]
        table=TABLE,
        batches=iter(
            [
                _batch(1),
            ]
        ),
    )

    append_stream = FakeAppendRowsStream.instances[0]

    assert append_stream.closed


def test_write_pending_batches_uses_first_batch_schema() -> None:
    client = FakeWriteClient()

    batch = _batch(1, 2)

    write_pending_batches(
        client=client,  # pyright: ignore[reportArgumentType]
        table=TABLE,
        batches=iter([batch]),
    )

    append_stream = FakeAppendRowsStream.instances[0]

    assert (
        append_stream
        .request_template
        .arrow_rows
        .writer_schema
        .serialized_schema
        == batch.schema.serialize().to_pybytes()
    )


def test_write_pending_batches_returns_without_writing_for_empty_input() -> None:
    client = FakeWriteClient()

    write_pending_batches(
        client=client,  # pyright: ignore[reportArgumentType]
        table=TABLE,
        batches=iter([]),
    )

    assert client.create_calls == []
    assert FakeAppendRowsStream.instances == []
    assert client.finalize_calls == []
    assert client.commit_calls == []


def test_write_pending_batches_raises_on_commit_stream_errors() -> None:
    client = FakeWriteClient(
        stream_errors=[
            "commit failed",
        ]
    )

    with pytest.raises(
        RuntimeError,
        match="Failed to commit BigQuery write stream",
    ):
        write_pending_batches(
            client=client,  # pyright: ignore[reportArgumentType]
            table=TABLE,
            batches=iter(
                [
                    _batch(1),
                ]
            ),
        )
