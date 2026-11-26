"""Analyse previously captured Network Rail TRUST messages."""

import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


def describe(values: list[int] | list[float]) -> str:
    """Format basic descriptive statistics."""
    if not values:
        return "No observations"

    return (
        f"min={min(values):,.1f}, "
        f"mean={statistics.mean(values):,.1f}, "
        f"median={statistics.median(values):,.1f}, "
        f"max={max(values):,.1f}"
    )


def print_distribution(counts: Counter) -> None:
    """Print a sorted frequency distribution."""
    for value, count in sorted(counts.items()):
        print(f"  {str(value):>8}: {count:>6}")


def analyse(
    input_path: Path,
    examples_output: Path | None = None,
) -> None:
    """Analyse a capture containing one STOMP message per NDJSON line."""

    message_count = 0
    event_count = 0
    malformed_bodies = 0
    malformed_events = 0

    batch_sizes: list[int] = []
    body_sizes: list[int] = []
    arrival_gaps: list[float] = []

    event_types: Counter[str] = Counter()
    header_fields: dict[str, set[str]] = defaultdict(set)
    body_fields: dict[str, set[str]] = defaultdict(set)

    examples: dict[str, dict[str, Any]] = {}
    previous_timestamp: datetime | None = None

    with input_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                received_at = datetime.fromisoformat(
                    record["received_at"]
                )
                body = record["body"]
            except (ValueError, TypeError, KeyError) as exc:
                raise ValueError(
                    f"Invalid capture record on line {line_number}"
                ) from exc

            if not isinstance(body, str):
                raise ValueError(
                    f"Expected string body on line {line_number}"
                )

            message_count += 1
            body_sizes.append(len(body.encode("utf-8")))

            if previous_timestamp is not None:
                gap = (received_at - previous_timestamp).total_seconds()
                arrival_gaps.append(gap)

            previous_timestamp = received_at

            try:
                events = json.loads(body)
            except json.JSONDecodeError:
                malformed_bodies += 1
                continue

            if not isinstance(events, list):
                malformed_bodies += 1
                continue

            batch_sizes.append(len(events))

            for event in events:
                if not isinstance(event, dict):
                    malformed_events += 1
                    continue

                header = event.get("header")
                event_body = event.get("body")

                if not isinstance(header, dict) or not isinstance(
                    event_body, dict
                ):
                    malformed_events += 1
                    continue

                event_count += 1

                msg_type = str(header.get("msg_type", "<missing>"))
                event_types[msg_type] += 1

                header_fields[msg_type].update(header.keys())
                body_fields[msg_type].update(event_body.keys())

                if msg_type not in examples:
                    examples[msg_type] = event

    print(f"\nTRUST capture analysis: {input_path}")
    print("=" * 65)

    print("\nVolume")
    print(f"  STOMP messages:       {message_count:,}")
    print(f"  Valid TRUST events:   {event_count:,}")
    print(f"  Malformed bodies:     {malformed_bodies:,}")
    print(f"  Malformed events:     {malformed_events:,}")

    print("\nEvents per STOMP message")
    print(f"  {describe(batch_sizes)}")
    print("  Distribution:")
    print_distribution(Counter(batch_sizes))

    print("\nSTOMP message body sizes (bytes)")
    print(f"  {describe(body_sizes)}")
    print(f"  Total: {sum(body_sizes):,} bytes")

    print("\nTime between received messages (seconds)")
    print(f"  {describe(arrival_gaps)}")

    print("\nTRUST event types")
    for msg_type, count in sorted(event_types.items()):
        percentage = 100 * count / event_count
        print(
            f"  {msg_type:>8}: {count:>6,} "
            f"({percentage:5.1f}%)"
        )

    print("\nFields observed by event type")
    for msg_type in sorted(event_types):
        print(f"\n  msg_type={msg_type}")
        print(f"    Header: {', '.join(sorted(header_fields[msg_type]))}")
        print(f"    Body:   {', '.join(sorted(body_fields[msg_type]))}")

    if examples_output is not None:
        examples_output.parent.mkdir(parents=True, exist_ok=True)

        with examples_output.open("w", encoding="utf-8") as file:
            json.dump(
                examples,
                file,
                indent=2,
                ensure_ascii=False,
            )
            file.write("\n")

        print(
            f"\nWrote {len(examples)} representative events "
            f"to {examples_output}"
        )
