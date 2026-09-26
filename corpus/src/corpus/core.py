from dataclasses import dataclass
from datetime import date
from typing import Callable


@dataclass(frozen=True)
class ObjectState:
    crc32c: str
    generation: int


def object_name(acquisition_date: date) -> str:
    return f"corpus/acquired_date={acquisition_date.isoformat()}/corpus.json"


def sync_corpus(
    *,
    acquisition_date: date,
    fetch_latest_corpus_file: Callable[[], bytes],
    crc32c_checksum: Callable[[bytes], str],
    get_object_state: Callable[[str], ObjectState | None],
    upload_object: Callable[[str, bytes, int], None],
) -> None:
    name = object_name(acquisition_date)

    content = fetch_latest_corpus_file()
    local_crc32c = crc32c_checksum(content)
    existing = get_object_state(name)

    if existing is not None and existing.crc32c == local_crc32c:
        return

    expected_generation = (
        existing.generation
        if existing is not None
        else 0
    )

    upload_object(name, content, expected_generation)
