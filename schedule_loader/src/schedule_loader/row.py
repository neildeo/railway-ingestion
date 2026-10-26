from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any


@dataclass(frozen=True)
class LoadContext:
    load_id: str
    source_object: str
    source_generation: int
    publication_sequence: int
    extract_type: str


def attach_load_context(
    row: dict[str, Any],
    *,
    context: LoadContext,
) -> dict[str, Any]:
    """
    Attach fields common to every raw SCHEDULE record family.
    """
    return {
        "load_id": context.load_id,
        "source_object": context.source_object,
        "source_generation": context.source_generation,
        "publication_sequence": context.publication_sequence,
        "extract_type": context.extract_type,
        **row,
    }


def association_row(
    record: dict[str, Any],
) -> dict[str, Any]:
    """
    Convert AssociationV1 source data into fields specific
    to raw.schedule_associations.

    Common load/provenance fields are attached separately with
    attach_load_context().
    """
    return {
        "transaction_type": record["transaction_type"],
        "CIF_stp_indicator": record["CIF_stp_indicator"],
        "assoc_start_date": date.fromisoformat(
            record["assoc_start_date"][:10]
        ),
        "assoc_train_uid": record["assoc_train_uid"],
        "base_location_suffix": record.get(
            "base_location_suffix"
        ),
        "diagram_type": record["diagram_type"],
        "location": record["location"],
        "main_train_uid": record["main_train_uid"],
        "assoc_days": record.get("assoc_days"),
        "assoc_end_date": (
            date.fromisoformat(
                record["assoc_end_date"][:10]
            )
            if record.get("assoc_end_date")
            else None
        ),
        "assoc_location_suffix": record.get(
            "assoc_location_suffix"
        ),
        "category": record.get("category"),
        "date_indicator": record.get("date_indicator"),
    }


def publication_row(
    header: dict[str, Any],
) -> dict[str, Any]:
    """
    Convert JsonTimetableV1 header data into the fields specific
    to raw.schedule_publications.

    Common load/provenance fields are attached separately with
    attach_load_context().
    """
    sender = header["Sender"]

    return {
        "timestamp": datetime.fromtimestamp(
            header["timestamp"],
            tz=timezone.utc,
        ),
        "sender_application": sender["application"],
        "sender_component": sender["component"],
        "sender_organisation": sender["organisation"],
        "classification": header["classification"],
        "owner": header["owner"],
    }


def tiploc_row(
    record: dict[str, Any],
) -> dict[str, Any]:
    """
    Convert TiplocV1 source data into fields specific
    to raw.schedule_tiplocs.

    Common load/provenance fields are attached separately with
    attach_load_context().
    """
    return {
        "transaction_type": record["transaction_type"],
        "tiploc_code": record["tiploc_code"],
        "tps_description": record["tps_description"],
        "description": record.get("description"),
        "crs_code": record.get("crs_code"),
        "stanox": record.get("stanox"),
        "nalco": record["nalco"],
    }
