from __future__ import annotations

from dataclasses import dataclass

from schedule_load_backfill.discovery import (
    discover_workflow_executions,
)
from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
)


BUCKET_NAME = (
    "railway-analytics-508615-network-rail-open-data-raw"
)
PROJECT = "railway-analytics-508615"
AUDIT_DATASET = "raw"


@dataclass(frozen=True)
class FakeBlob:
    name: str
    generation: int


@dataclass(frozen=True)
class FakeAuditRow:
    source_object: str
    source_generation: int
    record_family: str
    status: str


class FakeQueryJob:
    def __init__(
        self,
        rows: list[FakeAuditRow],
    ) -> None:
        self.rows = rows

    def result(self) -> list[FakeAuditRow]:
        return self.rows


class FakeStorageClient:
    def __init__(
        self,
        blobs: list[FakeBlob],
    ) -> None:
        self.blobs = blobs
        self.list_calls: list[dict[str, str]] = []

    def list_blobs(
        self,
        bucket_name: str,
        *,
        prefix: str,
    ) -> list[FakeBlob]:
        self.list_calls.append(
            {
                "bucket_name": bucket_name,
                "prefix": prefix,
            }
        )

        return self.blobs


class FakeBigQueryClient:
    def __init__(
        self,
        rows: list[FakeAuditRow],
    ) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def query(
        self,
        query: str,
    ) -> FakeQueryJob:
        self.queries.append(query)
        return FakeQueryJob(self.rows)


def audit_row(
    *,
    object: str,
    generation: int,
    record_family: str,
    status: str = "SUCCEEDED",
) -> FakeAuditRow:
    return FakeAuditRow(
        source_object=object,
        source_generation=generation,
        record_family=record_family,
        status=status,
    )


def successful_audit_rows(
    *,
    object: str,
    generation: int,
) -> list[FakeAuditRow]:
    return [
        audit_row(
            object=object,
            generation=generation,
            record_family="publication",
        ),
        audit_row(
            object=object,
            generation=generation,
            record_family="tiploc",
        ),
        audit_row(
            object=object,
            generation=generation,
            record_family="association",
        ),
        audit_row(
            object=object,
            generation=generation,
            record_family="schedule",
        ),
    ]


def test_discovery_returns_all_objects_when_none_are_loaded() -> None:
    storage_client = FakeStorageClient(
        [
            FakeBlob(
                name=(
                    "schedules/update/sequence=5253/"
                    "schedules.json.gz"
                ),
                generation=333,
            ),
            FakeBlob(
                name=(
                    "schedules/update/sequence=5251/"
                    "schedules.json.gz"
                ),
                generation=111,
            ),
            FakeBlob(
                name=(
                    "schedules/update/sequence=5252/"
                    "schedules.json.gz"
                ),
                generation=222,
            ),
        ]
    )
    bigquery_client = FakeBigQueryClient([])

    result = discover_workflow_executions(
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        bucket_name=BUCKET_NAME,
        project=PROJECT,
        audit_dataset=AUDIT_DATASET,
    )

    assert result == [
        WorkflowExecutionArgs(
            object=(
                "schedules/update/sequence=5251/"
                "schedules.json.gz"
            ),
            generation=111,
        ),
        WorkflowExecutionArgs(
            object=(
                "schedules/update/sequence=5252/"
                "schedules.json.gz"
            ),
            generation=222,
        ),
        WorkflowExecutionArgs(
            object=(
                "schedules/update/sequence=5253/"
                "schedules.json.gz"
            ),
            generation=333,
        ),
    ]

    assert storage_client.list_calls == [
        {
            "bucket_name": BUCKET_NAME,
            "prefix": "schedules/",
        }
    ]

    assert len(bigquery_client.queries) == 1


def test_discovery_removes_completely_loaded_object_generation() -> None:
    object_a = (
        "schedules/update/sequence=5251/"
        "schedules.json.gz"
    )
    object_b = (
        "schedules/update/sequence=5252/"
        "schedules.json.gz"
    )

    storage_client = FakeStorageClient(
        [
            FakeBlob(
                name=object_a,
                generation=111,
            ),
            FakeBlob(
                name=object_b,
                generation=222,
            ),
        ]
    )

    bigquery_client = FakeBigQueryClient(
        successful_audit_rows(
            object=object_a,
            generation=111,
        )
    )

    result = discover_workflow_executions(
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        bucket_name=BUCKET_NAME,
        project=PROJECT,
        audit_dataset=AUDIT_DATASET,
    )

    assert result == [
        WorkflowExecutionArgs(
            object=object_b,
            generation=222,
        ),
    ]


def test_discovery_retains_object_when_generation_has_changed() -> None:
    object_name = (
        "schedules/update/sequence=5251/"
        "schedules.json.gz"
    )

    storage_client = FakeStorageClient(
        [
            FakeBlob(
                name=object_name,
                generation=222,
            ),
        ]
    )

    bigquery_client = FakeBigQueryClient(
        successful_audit_rows(
            object=object_name,
            generation=111,
        )
    )

    result = discover_workflow_executions(
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        bucket_name=BUCKET_NAME,
        project=PROJECT,
        audit_dataset=AUDIT_DATASET,
    )

    assert result == [
        WorkflowExecutionArgs(
            object=object_name,
            generation=222,
        ),
    ]


def test_discovery_retains_partially_loaded_object() -> None:
    object_name = (
        "schedules/update/sequence=5251/"
        "schedules.json.gz"
    )

    storage_client = FakeStorageClient(
        [
            FakeBlob(
                name=object_name,
                generation=111,
            ),
        ]
    )

    bigquery_client = FakeBigQueryClient(
        [
            audit_row(
                object=object_name,
                generation=111,
                record_family="publication",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="tiploc",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="association",
            ),
        ]
    )

    result = discover_workflow_executions(
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        bucket_name=BUCKET_NAME,
        project=PROJECT,
        audit_dataset=AUDIT_DATASET,
    )

    assert result == [
        WorkflowExecutionArgs(
            object=object_name,
            generation=111,
        ),
    ]


def test_discovery_does_not_count_failed_record_family_as_complete() -> None:
    object_name = (
        "schedules/update/sequence=5251/"
        "schedules.json.gz"
    )

    storage_client = FakeStorageClient(
        [
            FakeBlob(
                name=object_name,
                generation=111,
            ),
        ]
    )

    bigquery_client = FakeBigQueryClient(
        [
            audit_row(
                object=object_name,
                generation=111,
                record_family="publication",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="tiploc",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="association",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="schedule",
                status="FAILED",
            ),
        ]
    )

    result = discover_workflow_executions(
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        bucket_name=BUCKET_NAME,
        project=PROJECT,
        audit_dataset=AUDIT_DATASET,
    )

    assert result == [
        WorkflowExecutionArgs(
            object=object_name,
            generation=111,
        ),
    ]


def test_discovery_duplicate_successes_do_not_fake_completeness() -> None:
    object_name = (
        "schedules/update/sequence=5251/"
        "schedules.json.gz"
    )

    storage_client = FakeStorageClient(
        [
            FakeBlob(
                name=object_name,
                generation=111,
            ),
        ]
    )

    bigquery_client = FakeBigQueryClient(
        [
            audit_row(
                object=object_name,
                generation=111,
                record_family="publication",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="publication",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="tiploc",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="association",
            ),
        ]
    )

    result = discover_workflow_executions(
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        bucket_name=BUCKET_NAME,
        project=PROJECT,
        audit_dataset=AUDIT_DATASET,
    )

    assert result == [
        WorkflowExecutionArgs(
            object=object_name,
            generation=111,
        ),
    ]


def test_discovery_requires_expected_record_families() -> None:
    object_name = (
        "schedules/update/sequence=5251/"
        "schedules.json.gz"
    )

    storage_client = FakeStorageClient(
        [
            FakeBlob(
                name=object_name,
                generation=111,
            ),
        ]
    )

    bigquery_client = FakeBigQueryClient(
        [
            audit_row(
                object=object_name,
                generation=111,
                record_family="publication",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="tiploc",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="association",
            ),
            audit_row(
                object=object_name,
                generation=111,
                record_family="unexpected",
            ),
        ]
    )

    result = discover_workflow_executions(
        storage_client=storage_client,  # pyright: ignore[reportArgumentType]
        bigquery_client=bigquery_client,  # pyright: ignore[reportArgumentType]
        bucket_name=BUCKET_NAME,
        project=PROJECT,
        audit_dataset=AUDIT_DATASET,
    )

    assert result == [
        WorkflowExecutionArgs(
            object=object_name,
            generation=111,
        ),
    ]
