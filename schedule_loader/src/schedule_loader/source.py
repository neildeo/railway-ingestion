from __future__ import annotations

from typing import Any

from google.cloud import storage


def read_schedule_header(
    *,
    client: storage.Client,
    bucket_name: str,
    object_name: str,
    generation: int,
) -> dict[str, Any]:
    """
    Open one exact immutable GCS object generation and return its
    JsonTimetableV1 header record.
    """
    raise NotImplementedError
