from __future__ import annotations

from schedule.core import (
    SchedulePublicationInfo,
    ScheduleRequest,
    StoredObjectState,
    SourceMetadata,
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


def get_object_state(object_name: str) -> StoredObjectState | None:
    """
    Return the existing GCS object's stored source provenance and generation,
    or None when the object does not exist.
    """
    raise NotImplementedError


def upload_schedule(
    schedule_request: ScheduleRequest,
    object_name: str,
    expected_source_metadata: SourceMetadata,
) -> None:
    """
    Perform a fresh full source request and stream the exact compressed bytes
    into the normal GCS object.

    Before transferring the body, verify the new S3 response metadata still
    matches `expected_source_metadata`.

    The final GCS write should be create-only (`if_generation_match=0`).
    """
    raise NotImplementedError


def quarantine_schedule(
    schedule_request: ScheduleRequest,
    object_name: str,
    expected_source_metadata: SourceMetadata,
) -> None:
    """
    Perform a fresh full source request and preserve the conflicting artefact
    under the supplied quarantine object name.

    The new source response metadata should still be checked against the probe
    metadata before the transfer begins.
    """
    raise NotImplementedError
