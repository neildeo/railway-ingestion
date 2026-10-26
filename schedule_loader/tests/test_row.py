from __future__ import annotations

from datetime import date, datetime, timezone

from schedule_loader.row import (
    LoadContext,
    association_row,
    attach_load_context,
    publication_row,
    schedule_row,
    tiploc_row,
)


def test_publication_row_contains_only_record_specific_fields() -> None:
    header = {
        "Metadata": {
            "sequence": 5251,
            "type": "full",
        },
        "Sender": {
            "application": "NTROD",
            "component": "SCHEDULE",
            "organisation": "Rockshore",
        },
        "classification": "public",
        "owner": "Network Rail",
        "timestamp": 1791246721,
    }

    row = publication_row(header)

    assert row == {
        "timestamp": datetime.fromtimestamp(
            1791246721,
            tz=timezone.utc,
        ),
        "sender_application": "NTROD",
        "sender_component": "SCHEDULE",
        "sender_organisation": "Rockshore",
        "classification": "public",
        "owner": "Network Rail",
    }


def test_tiploc_row_maps_required_fields() -> None:
    record = {
        "transaction_type": "Create",
        "tiploc_code": "PADTON",
        "tps_description": "LONDON PADDINGTON",
        "nalco": "308700",
    }

    result = tiploc_row(record)

    assert result == {
        "transaction_type": "Create",
        "tiploc_code": "PADTON",
        "tps_description": "LONDON PADDINGTON",
        "description": None,
        "crs_code": None,
        "stanox": None,
        "nalco": "308700",
    }


def test_tiploc_row_preserves_optional_fields() -> None:
    record = {
        "transaction_type": "Create",
        "tiploc_code": "PADTON",
        "tps_description": "LONDON PADDINGTON",
        "description": "PADDINGTON",
        "crs_code": "PAD",
        "stanox": "33057",
        "nalco": "308700",
    }

    result = tiploc_row(record)

    assert result == {
        "transaction_type": "Create",
        "tiploc_code": "PADTON",
        "tps_description": "LONDON PADDINGTON",
        "description": "PADDINGTON",
        "crs_code": "PAD",
        "stanox": "33057",
        "nalco": "308700",
    }


def test_attach_load_context_adds_common_raw_fields() -> None:
    context = LoadContext(
        load_id="20261019T084900.000000Z-a7f3c9",
        source_object=(
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
        source_generation=123456789,
        publication_sequence=5251,
        extract_type="full",
    )

    record_row = {
        "timestamp": datetime(
            2026,
            10,
            6,
            tzinfo=timezone.utc,
        ),
        "owner": "Network Rail",
    }

    row = attach_load_context(
        record_row,
        context=context,
    )

    assert row == {
        "load_id": "20261019T084900.000000Z-a7f3c9",
        "source_object": (
            "schedules/full/sequence=5251/"
            "schedules.json.gz"
        ),
        "source_generation": 123456789,
        "publication_sequence": 5251,
        "extract_type": "full",
        "timestamp": datetime(
            2026,
            10,
            6,
            tzinfo=timezone.utc,
        ),
        "owner": "Network Rail",
    }


def test_attach_load_context_does_not_mutate_input_row() -> None:
    context = LoadContext(
        load_id="load-1",
        source_object="source.json.gz",
        source_generation=123,
        publication_sequence=5251,
        extract_type="full",
    )

    original = {
        "owner": "Network Rail",
    }

    attach_load_context(
        original,
        context=context,
    )

    assert original == {
        "owner": "Network Rail",
    }


def test_association_row_maps_create_record() -> None:
    record = {
        "transaction_type": "Create",
        "main_train_uid": "C30123",
        "assoc_train_uid": "C42949",
        "assoc_start_date": "2024-06-03T00:00:00Z",
        "assoc_end_date": "2024-06-21T00:00:00Z",
        "assoc_days": "1111100",
        "category": "NP",
        "date_indicator": "S",
        "location": "YORK",
        "base_location_suffix": None,
        "assoc_location_suffix": None,
        "diagram_type": "T",
        "CIF_stp_indicator": "P",
    }

    result = association_row(record)

    assert result == {
        "transaction_type": "Create",
        "CIF_stp_indicator": "P",
        "assoc_start_date": date(2024, 6, 3),
        "assoc_train_uid": "C42949",
        "base_location_suffix": None,
        "diagram_type": "T",
        "location": "YORK",
        "main_train_uid": "C30123",
        "assoc_days": "1111100",
        "assoc_end_date": date(2024, 6, 21),
        "assoc_location_suffix": None,
        "category": "NP",
        "date_indicator": "S",
    }


def test_association_row_maps_optional_fields_when_absent() -> None:
    record = {
        "transaction_type": "Delete",
        "main_train_uid": "C30123",
        "assoc_train_uid": "C42949",
        "assoc_start_date": "2024-06-03T00:00:00Z",
        "base_location_suffix": None,
        "diagram_type": "T",
        "location": "YORK",
        "CIF_stp_indicator": "P",
    }

    result = association_row(record)

    assert result == {
        "transaction_type": "Delete",
        "CIF_stp_indicator": "P",
        "assoc_start_date": date(2024, 6, 3),
        "assoc_train_uid": "C42949",
        "base_location_suffix": None,
        "diagram_type": "T",
        "location": "YORK",
        "main_train_uid": "C30123",
        "assoc_days": None,
        "assoc_end_date": None,
        "assoc_location_suffix": None,
        "category": None,
        "date_indicator": None,
    }


def test_schedule_row_maps_full_schedule() -> None:
    record = {
        "transaction_type": "Create",
        "CIF_stp_indicator": "P",
        "CIF_train_uid": "C00038",
        "schedule_start_date": "2026-05-17",
        "schedule_end_date": "2026-11-08",
        "CIF_bank_holiday_running": None,
        "schedule_days_runs": "0000001",
        "train_status": "P",
        "atoc_code": "NT",
        "applicable_timetable": "Y",
        "new_schedule_segment": {
            "traction_class": "",
            "uic_code": "",
        },
        "schedule_segment": {
            "signalling_id": "5H63",
            "CIF_train_category": "EE",
            "CIF_headcode": "",
            "CIF_course_indicator": 1,
            "CIF_train_service_code": "22974000",
            "CIF_business_sector": "??",
            "CIF_power_type": "DMU",
            "CIF_timing_load": "S",
            "CIF_speed": "075",
            "CIF_operating_characteristics": None,
            "CIF_train_class": None,
            "CIF_sleepers": None,
            "CIF_reservations": None,
            "CIF_connection_indicator": None,
            "CIF_catering_code": None,
            "CIF_service_branding": "",
            "schedule_location": [
                {
                    "location_type": "LO",
                    "record_identity": "LO",
                    "tiploc_code": "CARLILE",
                    "tiploc_instance": None,
                    "departure": "0905",
                    "public_departure": None,
                    "platform": "CLS",
                    "line": None,
                    "engineering_allowance": None,
                    "pathing_allowance": None,
                    "performance_allowance": None,
                },
                {
                    "location_type": "LI",
                    "record_identity": "LI",
                    "tiploc_code": "CARLNEC",
                    "tiploc_instance": None,
                    "departure": "0911",
                    "public_departure": None,
                    "platform": None,
                    "line": None,
                    "engineering_allowance": None,
                    "pathing_allowance": None,
                    "performance_allowance": None,
                    "arrival": "0908",
                    "public_arrival": None,
                    "pass": None,
                    "path": None,
                },
                {
                    "location_type": "LT",
                    "record_identity": "LT",
                    "tiploc_code": "CARLILE",
                    "tiploc_instance": "2",
                    "platform": "6",
                    "arrival": "0914",
                    "public_arrival": None,
                    "path": None,
                },
            ],
        },
    }

    result = schedule_row(record)

    assert result == {
        "transaction_type": "Create",
        "CIF_stp_indicator": "P",
        "CIF_train_uid": "C00038",
        "schedule_start_date": date(2026, 5, 17),
        "schedule_end_date": date(2026, 11, 8),
        "CIF_bank_holiday_running": None,
        "schedule_days_runs": "0000001",
        "train_status": "P",
        "atoc_code": "NT",
        "applicable_timetable": "Y",
        "new_schedule_segment": {
            "traction_class": "",
            "uic_code": "",
        },
        "schedule_segment": {
            "signalling_id": "5H63",
            "CIF_business_sector": "??",
            "CIF_course_indicator": 1,
            "CIF_headcode": "",
            "CIF_train_category": "EE",
            "CIF_service_branding": "",
            "CIF_train_service_code": "22974000",
            "CIF_connection_indicator": None,
            "CIF_sleepers": None,
            "CIF_catering_code": None,
            "CIF_reservations": None,
            "CIF_operating_characteristics": None,
            "CIF_timing_load": "S",
            "CIF_train_class": None,
            "CIF_power_type": "DMU",
            "CIF_speed": "075",
            "schedule_location": [
                {
                    "location_type": "LO",
                    "record_identity": "LO",
                    "tiploc_code": "CARLILE",
                    "tiploc_instance": None,
                    "platform": "CLS",
                    "arrival": None,
                    "departure": "0905",
                    "pass": None,
                    "public_arrival": None,
                    "public_departure": None,
                    "path": None,
                    "line": None,
                    "engineering_allowance": None,
                    "pathing_allowance": None,
                    "performance_allowance": None,
                },
                {
                    "location_type": "LI",
                    "record_identity": "LI",
                    "tiploc_code": "CARLNEC",
                    "tiploc_instance": None,
                    "platform": None,
                    "arrival": "0908",
                    "departure": "0911",
                    "pass": None,
                    "public_arrival": None,
                    "public_departure": None,
                    "path": None,
                    "line": None,
                    "engineering_allowance": None,
                    "pathing_allowance": None,
                    "performance_allowance": None,
                },
                {
                    "location_type": "LT",
                    "record_identity": "LT",
                    "tiploc_code": "CARLILE",
                    "tiploc_instance": "2",
                    "platform": "6",
                    "arrival": "0914",
                    "departure": None,
                    "pass": None,
                    "public_arrival": None,
                    "public_departure": None,
                    "path": None,
                    "line": None,
                    "engineering_allowance": None,
                    "pathing_allowance": None,
                    "performance_allowance": None,
                },
            ],
        },
    }


def test_schedule_row_maps_missing_schedule_locations_to_empty_list() -> None:
    record = {
        "transaction_type": "Create",
        "CIF_stp_indicator": "C",
        "CIF_train_uid": "C18018",
        "schedule_start_date": "2026-05-17",
        "schedule_end_date": "2026-12-06",
        "CIF_bank_holiday_running": None,
        "schedule_days_runs": "0000001",
        "train_status": " ",
        "schedule_segment": {
            "signalling_id": "    ",
            "CIF_train_category": "",
            "CIF_headcode": "",
            "CIF_course_indicator": 1,
            "CIF_train_service_code": "        ",
            "CIF_business_sector": "??",
            "CIF_power_type": None,
            "CIF_timing_load": None,
            "CIF_speed": None,
            "CIF_operating_characteristics": None,
            "CIF_train_class": None,
            "CIF_sleepers": None,
            "CIF_reservations": None,
            "CIF_connection_indicator": None,
            "CIF_catering_code": None,
            "CIF_service_branding": "",
        },
    }

    result = schedule_row(record)

    assert result["atoc_code"] is None
    assert result["applicable_timetable"] is None
    assert result["new_schedule_segment"] is None
    assert result["schedule_segment"] is not None
    assert (
        result["schedule_segment"]["schedule_location"]
        == []
    )


def test_schedule_row_preserves_explicit_empty_schedule_locations() -> None:
    record = {
        "transaction_type": "Create",
        "CIF_stp_indicator": "C",
        "CIF_train_uid": "C18018",
        "schedule_start_date": "2026-05-17",
        "schedule_end_date": "2026-12-06",
        "CIF_bank_holiday_running": None,
        "schedule_days_runs": "0000001",
        "train_status": " ",
        "schedule_segment": {
            "signalling_id": "    ",
            "CIF_train_category": "",
            "CIF_headcode": "",
            "CIF_course_indicator": 1,
            "CIF_train_service_code": "        ",
            "CIF_business_sector": "??",
            "CIF_power_type": None,
            "CIF_timing_load": None,
            "CIF_speed": None,
            "CIF_operating_characteristics": None,
            "CIF_train_class": None,
            "CIF_sleepers": None,
            "CIF_reservations": None,
            "CIF_connection_indicator": None,
            "CIF_catering_code": None,
            "CIF_service_branding": "",
            "schedule_location": [],
        },
    }

    result = schedule_row(record)

    assert result["schedule_segment"] is not None
    assert (
        result["schedule_segment"]["schedule_location"]
        == []
    )


def test_schedule_row_maps_missing_schedule_segment_to_none() -> None:
    record = {
        "transaction_type": "Delete",
        "CIF_stp_indicator": "P",
        "CIF_train_uid": "C00038",
        "schedule_start_date": "2026-05-17",
    }

    result = schedule_row(record)

    assert result["schedule_segment"] is None
    assert result["new_schedule_segment"] is None
    assert result["schedule_end_date"] is None
    assert result["schedule_days_runs"] is None
    assert result["train_status"] is None
    assert result["atoc_code"] is None
    assert result["applicable_timetable"] is None
