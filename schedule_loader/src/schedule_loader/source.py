from __future__ import annotations

from typing import Any


def read_schedule_header(
    *,
    bucket_name: str,
    object_name: str,
    generation: int,
) -> dict[str, Any]:
    """
    Open one exact immutable GCS object generation and return its
    JsonTimetableV1 header record.
    """
    raise NotImplementedError
