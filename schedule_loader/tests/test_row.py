from __future__ import annotations

from datetime import datetime, timezone

from schedule_loader.row import (
    LoadContext,
    attach_load_context,
    publication_row,
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
