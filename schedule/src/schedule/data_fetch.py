from __future__ import annotations

from schedule.core import (
    SchedulePublicationInfo,
    ScheduleRequest,
)


def fetch_header_row(
    schedule_request: ScheduleRequest,
) -> SchedulePublicationInfo | None:
    """
    Make the short probe request.

    Intended responsibilities:
    - build the Network Rail URL for `schedule_request`
    - authenticate and follow the redirect to S3
    - capture S3 response metadata
    - consume only enough compressed bytes to obtain the first NDJSON row
    - partially decompress and parse JsonTimetableV1
    - close the HTTP response before returning

    Return None only for the normal "publication not available yet" outcome.
    Transport/auth/parsing failures should raise.
    """
    raise NotImplementedError
