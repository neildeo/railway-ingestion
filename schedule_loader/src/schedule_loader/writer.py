from __future__ import annotations

import pyarrow as pa


def write_pending_batch(
    *,
    table: str,
    batch: pa.RecordBatch,
) -> None:
    """
    Write one Arrow batch through a BigQuery Storage Write API
    PENDING stream, then finalize and commit it atomically.
    """
    raise NotImplementedError
