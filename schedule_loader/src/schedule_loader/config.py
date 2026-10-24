from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PublicationLoadConfig:
    source_bucket: str
    source_object: str
    source_generation: int
    project: str
    dataset: str
    table_name: str
    schema_path: Path


def publication_config_from_env() -> PublicationLoadConfig:
    raise NotImplementedError
