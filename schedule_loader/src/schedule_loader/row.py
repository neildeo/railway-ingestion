from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
