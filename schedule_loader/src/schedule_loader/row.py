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


def schedule_row(
    record: dict[str, Any],
) -> dict[str, Any]:
    """
    Convert JsonScheduleV1 source data into fields specific
    to raw.schedule_schedules.

    Common load/provenance fields are attached separately with
    attach_load_context().
    """
    new_schedule_segment = record.get(
        "new_schedule_segment"
    )
    schedule_segment = record.get(
        "schedule_segment"
    )

    return {
        "transaction_type": record["transaction_type"],
        "CIF_stp_indicator": record["CIF_stp_indicator"],
        "CIF_train_uid": record["CIF_train_uid"],
        "schedule_start_date": date.fromisoformat(
            record["schedule_start_date"]
        ),
        "schedule_end_date": (
            date.fromisoformat(
                record["schedule_end_date"]
            )
            if record.get("schedule_end_date")
            else None
        ),
        "CIF_bank_holiday_running": record.get(
            "CIF_bank_holiday_running"
        ),
        "schedule_days_runs": record.get(
            "schedule_days_runs"
        ),
        "train_status": record.get("train_status"),
        "atoc_code": record.get("atoc_code"),
        "applicable_timetable": record.get(
            "applicable_timetable"
        ),
        "new_schedule_segment": (
            _new_schedule_segment_row(
                new_schedule_segment
            )
            if new_schedule_segment is not None
            else None
        ),
        "schedule_segment": (
            _schedule_segment_row(schedule_segment)
            if schedule_segment is not None
            else _empty_schedule_segment_row()
        ),
    }


def _new_schedule_segment_row(
    segment: dict[str, Any],
) -> dict[str, Any]:
    return {
        "traction_class": segment.get(
            "traction_class"
        ),
        "uic_code": segment.get("uic_code"),
    }


def _schedule_segment_row(
    segment: dict[str, Any],
) -> dict[str, Any]:
    locations = segment.get(
        "schedule_location",
        [],
    )

    return {
        "signalling_id": segment["signalling_id"],
        "CIF_business_sector": (
            segment["CIF_business_sector"]
        ),
        "CIF_course_indicator": (
            segment["CIF_course_indicator"]
        ),
        "CIF_headcode": segment["CIF_headcode"],
        "CIF_train_category": (
            segment["CIF_train_category"]
        ),
        "CIF_service_branding": (
            segment["CIF_service_branding"]
        ),
        "CIF_train_service_code": (
            segment["CIF_train_service_code"]
        ),
        "CIF_connection_indicator": segment.get(
            "CIF_connection_indicator"
        ),
        "CIF_sleepers": segment.get(
            "CIF_sleepers"
        ),
        "CIF_catering_code": segment.get(
            "CIF_catering_code"
        ),
        "CIF_reservations": segment.get(
            "CIF_reservations"
        ),
        "CIF_operating_characteristics": (
            segment.get(
                "CIF_operating_characteristics"
            )
        ),
        "CIF_timing_load": segment.get(
            "CIF_timing_load"
        ),
        "CIF_train_class": segment.get(
            "CIF_train_class"
        ),
        "CIF_power_type": segment.get(
            "CIF_power_type"
        ),
        "CIF_speed": segment.get("CIF_speed"),
        "schedule_location": [
            _schedule_location_row(location)
            for location in locations
        ],
    }


def _empty_schedule_segment_row() -> dict[str, Any]:
    return {
        "signalling_id": None,
        "CIF_business_sector": None,
        "CIF_course_indicator": None,
        "CIF_headcode": None,
        "CIF_train_category": None,
        "CIF_service_branding": None,
        "CIF_train_service_code": None,
        "CIF_connection_indicator": None,
        "CIF_sleepers": None,
        "CIF_catering_code": None,
        "CIF_reservations": None,
        "CIF_operating_characteristics": None,
        "CIF_timing_load": None,
        "CIF_train_class": None,
        "CIF_power_type": None,
        "CIF_speed": None,
        "schedule_location": [],
    }


def _schedule_location_row(
    location: dict[str, Any],
) -> dict[str, Any]:
    return {
        "location_type": location["location_type"],
        "record_identity": location["record_identity"],
        "tiploc_code": location["tiploc_code"],
        "tiploc_instance": location.get(
            "tiploc_instance"
        ),
        "platform": location.get("platform"),
        "arrival": location.get("arrival"),
        "departure": location.get("departure"),
        "pass": location.get("pass"),
        "public_arrival": location.get(
            "public_arrival"
        ),
        "public_departure": location.get(
            "public_departure"
        ),
        "path": location.get("path"),
        "line": location.get("line"),
        "engineering_allowance": location.get(
            "engineering_allowance"
        ),
        "pathing_allowance": location.get(
            "pathing_allowance"
        ),
        "performance_allowance": location.get(
            "performance_allowance"
        ),
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
