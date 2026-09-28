from dataclasses import dataclass
from datetime import date
from typing import Callable
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ObjectState:
    crc32c: str
    generation: int


def object_name(acquisition_date: date) -> str:
    return f"corpus/acquired_date={acquisition_date.isoformat()}/corpus.json"


def sync_corpus(
    *,
    acquisition_date: date,
    project_id: str,
    secret_id: str,
    fetch_latest_corpus_file: Callable[[str, str], bytes],
    crc32c_checksum: Callable[[bytes], str],
    get_object_state: Callable[[str], ObjectState | None],
    upload_object: Callable[[str, bytes, int], None],
) -> None:
    name = object_name(acquisition_date)

    content = fetch_latest_corpus_file(project_id, secret_id)
    local_crc32c = crc32c_checksum(content)
    existing = get_object_state(name)

    if existing is not None and existing.crc32c == local_crc32c:
        logger.info(
            "CORPUS object already matches current source; no upload required"
        )
        return

    expected_generation = (
        existing.generation
        if existing is not None
        else 0
    )

    logger.info(
        "Uploading CORPUS object %s with generation precondition %s",
        name,
        expected_generation,
    )

    upload_object(name, content, expected_generation)

    logger.info("Successfully uploaded CORPUS object %s", name)
