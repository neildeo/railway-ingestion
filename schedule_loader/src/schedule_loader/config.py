from dataclasses import dataclass
from pathlib import Path
import os

from schedule_loader.source import RecordType


SCHEMA_ROOT = Path("schedule_schemas")


@dataclass(frozen=True)
class LoadConfig:
    source_bucket: str
    source_object: str
    source_generation: int
    record_type: RecordType
    project: str
    dataset: str
    table_name: str
    schema_path: Path


def load_config_from_env() -> LoadConfig:
    raise NotImplementedError

    source_bucket = os.environ["SOURCE_BUCKET"]
    source_object = os.environ["SOURCE_OBJECT"]
    source_generation = os.environ["SOURCE_GENERATION"]
    project = os.environ["BQ_PROJECT"]
    dataset = os.environ["BQ_DATASET"]
    table_name = os.environ["BQ_TABLE"]

    source_generation = int(source_generation)

    return LoadConfig(
        source_bucket=source_bucket,
        source_object=source_object,
        source_generation=source_generation,
        project=project,
        dataset=dataset,
        table_name=table_name,
        schema_path=SCHEMA_ROOT / "schedule_publications.json",
    )
