from dataclasses import dataclass
from pathlib import Path
import os

from schedule_loader.source import RecordType


SCHEMA_ROOT = Path("schemas")

# Mapping from record type to local BigQuery schema
SCHEMA_FILES: dict[RecordType, str] = {
    RecordType.ASSOCIATION: "schedule_associations.json",
    RecordType.PUBLICATION: "schedule_publications.json",
    RecordType.SCHEDULE: "schedule_schedules.json",
    RecordType.TIPLOC: "schedule_tiplocs.json",
}

# Mapping from record type to landing table name
# Dataset is configured separately
TABLES: dict[RecordType, str] = {
    RecordType.ASSOCIATION: "schedule_associations",
    RecordType.PUBLICATION: "schedule_publications",
    RecordType.SCHEDULE: "schedule_schedules",
    RecordType.TIPLOC: "schedule_tiplocs",
}

# Mapping from convenient record type names to names used in extracts
RECORD_TYPE_NAMES: dict[str, str] = {
    "association": "JsonAssociationV1",
    "publication": "JsonTimetableV1",
    "schedule": "JsonScheduleV1",
    "tiploc": "TiplocV1",
}


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

    source_bucket = os.environ["SOURCE_BUCKET"]
    source_object = os.environ["SOURCE_OBJECT"]
    source_generation = os.environ["SOURCE_GENERATION"]
    project = os.environ["BQ_PROJECT"]
    dataset = os.environ["BQ_DATASET"]

    record_type_name = os.environ["RECORD_TYPE"]
    try:
        record_type = RecordType(RECORD_TYPE_NAMES[record_type_name])
    except KeyError as e:
        raise ValueError(
            f"Unrecognised record type : {record_type_name}") from e

    source_generation = int(source_generation)

    return LoadConfig(
        source_bucket=source_bucket,
        source_object=source_object,
        source_generation=source_generation,
        record_type=record_type,
        project=project,
        dataset=dataset,
        table_name=TABLES[record_type],
        schema_path=SCHEMA_ROOT / SCHEMA_FILES[record_type],
    )
