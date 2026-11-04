from __future__ import annotations

import logging

import pytest

from schedule_load_backfill.__main__ import (
    WorkflowBatchFailed,
    main,
    run_backfill,
)
from schedule_load_backfill.config import BackfillConfig
from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
    WorkflowExecutionResult,
    WorkflowTerminalState,
)


PROJECT = "railway-analytics-508615"
BUCKET_NAME = (
    "railway-analytics-508615-network-rail-open-data-raw"
)
AUDIT_DATASET = "raw"
PARENT = (
    "projects/railway-analytics-508615/"
    "locations/europe-west1/"
    "workflows/railway-load-schedule-object"
)

ARGUMENTS_A = WorkflowExecutionArgs(
    object=(
        "schedules/update/sequence=5251/"
        "schedules.json.gz"
    ),
    generation=111,
)

ARGUMENTS_B = WorkflowExecutionArgs(
    object=(
        "schedules/update/sequence=5252/"
        "schedules.json.gz"
    ),
    generation=222,
)

ARGUMENTS_C = WorkflowExecutionArgs(
    object=(
        "schedules/update/sequence=5253/"
        "schedules.json.gz"
    ),
    generation=333,
)


def config(
    *,
    dry_run: bool = False,
) -> BackfillConfig:
    return BackfillConfig(
        project=PROJECT,
        source_bucket=BUCKET_NAME,
        audit_dataset=AUDIT_DATASET,
        workflow_parent=PARENT,
        max_in_flight=10,
        poll_interval_seconds=10.0,
        dry_run=dry_run,
    )


def result(
    *,
    arguments: WorkflowExecutionArgs,
    state: WorkflowTerminalState,
) -> WorkflowExecutionResult:
    return WorkflowExecutionResult(
        execution_name=(
            f"{PARENT}/executions/"
            f"{arguments.generation}"
        ),
        arguments=arguments,
        state=state,
    )


def log_extra(
    record: logging.LogRecord,
    key: str,
):
    return getattr(record, key, None)


@pytest.mark.asyncio
async def test_run_backfill_dry_run_discovers_but_does_not_execute(
    caplog: pytest.LogCaptureFixture,
) -> None:
    discovered = [
        ARGUMENTS_A,
        ARGUMENTS_B,
    ]

    discovery_calls: list[dict] = []
    execution_calls: list[dict] = []

    def discover(**kwargs):
        discovery_calls.append(kwargs)
        return discovered

    async def execute(**kwargs):
        execution_calls.append(kwargs)
        raise AssertionError(
            "execute_workflows must not be called during dry run"
        )

    with caplog.at_level(logging.INFO):
        await run_backfill(
            config=config(dry_run=True),
            storage_client=object(),
            bigquery_client=object(),
            executions_client=object(),
            discover=discover,
            execute=execute,
        )

    assert len(discovery_calls) == 1
    assert execution_calls == []

    assert {
        log_extra(record, "event")
        for record in caplog.records
        if hasattr(record, "event")
    } >= {
        "schedule_backfill_started",
        "schedule_backfill_candidate",
        "schedule_backfill_dry_run_completed",
    }

    candidate_records = [
        record
        for record in caplog.records
        if getattr(
            record,
            "event",
            None,
        ) == "schedule_backfill_candidate"
    ]

    assert {
        (
            log_extra(record, "source_object"),
            log_extra(record, "source_generation"),
        )
        for record in candidate_records
    } == {
        (
            ARGUMENTS_A.object,
            ARGUMENTS_A.generation,
        ),
        (
            ARGUMENTS_B.object,
            ARGUMENTS_B.generation,
        ),
    }


@pytest.mark.asyncio
async def test_run_backfill_exits_successfully_when_nothing_is_discovered(
    caplog: pytest.LogCaptureFixture,
) -> None:
    execution_calls: list[dict] = []

    def discover(**kwargs):
        return []

    async def execute(**kwargs):
        execution_calls.append(kwargs)
        raise AssertionError(
            "execute_workflows must not be called with no work"
        )

    with caplog.at_level(logging.INFO):
        await run_backfill(
            config=config(),
            storage_client=object(),
            bigquery_client=object(),
            executions_client=object(),
            discover=discover,
            execute=execute,
        )

    assert execution_calls == []

    assert any(
        getattr(
            record,
            "event",
            None,
        ) == "schedule_backfill_completed"
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_run_backfill_executes_discovered_workflows() -> None:
    execution_calls: list[dict] = []

    def discover(**kwargs):
        return [
            ARGUMENTS_A,
            ARGUMENTS_B,
        ]

    async def execute(**kwargs):
        execution_calls.append(kwargs)

        return [
            result(
                arguments=ARGUMENTS_A,
                state=WorkflowTerminalState.SUCCEEDED,
            ),
            result(
                arguments=ARGUMENTS_B,
                state=WorkflowTerminalState.SUCCEEDED,
            ),
        ]

    executions_client = object()

    await run_backfill(
        config=config(),
        storage_client=object(),
        bigquery_client=object(),
        executions_client=executions_client,
        discover=discover,
        execute=execute,
    )

    assert len(execution_calls) == 1

    call = execution_calls[0]

    assert call["client"] is executions_client
    assert call["parent"] == PARENT
    assert call["execution_configs"] == [
        ARGUMENTS_A,
        ARGUMENTS_B,
    ]
    assert call["max_in_flight"] == 10
    assert call["poll_interval_seconds"] == 10.0


@pytest.mark.asyncio
async def test_run_backfill_completes_successfully_when_all_workflows_succeed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def discover(**kwargs):
        return [
            ARGUMENTS_A,
            ARGUMENTS_B,
        ]

    async def execute(**kwargs):
        return [
            result(
                arguments=ARGUMENTS_A,
                state=WorkflowTerminalState.SUCCEEDED,
            ),
            result(
                arguments=ARGUMENTS_B,
                state=WorkflowTerminalState.SUCCEEDED,
            ),
        ]

    with caplog.at_level(logging.INFO):
        await run_backfill(
            config=config(),
            storage_client=object(),
            bigquery_client=object(),
            executions_client=object(),
            discover=discover,
            execute=execute,
        )

    completed = next(
        record
        for record in caplog.records
        if getattr(
            record,
            "event",
            None,
        ) == "schedule_backfill_completed"
    )

    assert log_extra(completed, "discovered_count") == 2
    assert log_extra(completed, "started_count") == 2
    assert log_extra(completed, "succeeded_count") == 2
    assert log_extra(completed, "unsuccessful_count") == 0
    assert log_extra(completed, "not_started_count") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_state",
    [
        WorkflowTerminalState.FAILED,
        WorkflowTerminalState.CANCELLED,
    ],
)
async def test_run_backfill_logs_unsuccessful_workflow_and_raises_batch_failure(
    terminal_state: WorkflowTerminalState,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def discover(**kwargs):
        return [
            ARGUMENTS_A,
            ARGUMENTS_B,
            ARGUMENTS_C,
        ]

    unsuccessful = result(
        arguments=ARGUMENTS_B,
        state=terminal_state,
    )

    async def execute(**kwargs):
        return [
            result(
                arguments=ARGUMENTS_A,
                state=WorkflowTerminalState.SUCCEEDED,
            ),
            unsuccessful,
        ]

    with caplog.at_level(logging.INFO):
        with pytest.raises(WorkflowBatchFailed):
            await run_backfill(
                config=config(),
                storage_client=object(),
                bigquery_client=object(),
                executions_client=object(),
                discover=discover,
                execute=execute,
            )

    failure_records = [
        record
        for record in caplog.records
        if getattr(
            record,
            "event",
            None,
        ) == "schedule_backfill_workflow_failed"
    ]

    assert len(failure_records) == 1

    failure_record = failure_records[0]

    assert (
        log_extra(failure_record, "execution_name")
        == unsuccessful.execution_name
    )
    assert (
        log_extra(failure_record, "source_object")
        == ARGUMENTS_B.object
    )
    assert (
        log_extra(failure_record, "source_generation")
        == ARGUMENTS_B.generation
    )
    assert (
        log_extra(failure_record, "workflow_state")
        == terminal_state.value
    )


@pytest.mark.asyncio
async def test_run_backfill_failure_summary_includes_unstarted_work() -> None:
    def discover(**kwargs):
        return [
            ARGUMENTS_A,
            ARGUMENTS_B,
            ARGUMENTS_C,
        ]

    async def execute(**kwargs):
        return [
            result(
                arguments=ARGUMENTS_A,
                state=WorkflowTerminalState.SUCCEEDED,
            ),
            result(
                arguments=ARGUMENTS_B,
                state=WorkflowTerminalState.FAILED,
            ),
        ]

    with pytest.raises(WorkflowBatchFailed) as exc_info:
        await run_backfill(
            config=config(),
            storage_client=object(),
            bigquery_client=object(),
            executions_client=object(),
            discover=discover,
            execute=execute,
        )

    assert exc_info.value.discovered_count == 3
    assert exc_info.value.started_count == 2
    assert exc_info.value.succeeded_count == 1
    assert exc_info.value.unsuccessful_count == 1
    assert exc_info.value.not_started_count == 1


@pytest.mark.asyncio
async def test_run_backfill_logs_and_propagates_orchestration_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def discover(**kwargs):
        return [
            ARGUMENTS_A,
            ARGUMENTS_B,
        ]

    failure = RuntimeError(
        "workflow monitoring failed"
    )

    async def execute(**kwargs):
        raise failure

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError) as exc_info:
            await run_backfill(
                config=config(),
                storage_client=object(),
                bigquery_client=object(),
                executions_client=object(),
                discover=discover,
                execute=execute,
            )

    assert exc_info.value is failure

    assert any(
        getattr(
            record,
            "event",
            None,
        ) == "schedule_backfill_orchestration_failed"
        for record in caplog.records
    )


# Regression test: Google Async Clients run in an event loop at the scope in
# which they are created. This tests that those clients run in the same loop
# as our main coroutine.
def test_main_creates_async_workflows_client_inside_running_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeExecutionsAsyncClient:
        def __init__(self) -> None:
            import asyncio

            asyncio.get_running_loop()

    async def fake_run_backfill(**kwargs) -> None:
        return None

    monkeypatch.setattr(
        "schedule_load_backfill.__main__.load_config_from_env",
        lambda: config(),
    )

    monkeypatch.setattr(
        "schedule_load_backfill.__main__.storage.Client",
        lambda **kwargs: object(),
    )

    monkeypatch.setattr(
        "schedule_load_backfill.__main__.bigquery.Client",
        lambda **kwargs: object(),
    )

    monkeypatch.setattr(
        "schedule_load_backfill.__main__.ExecutionsAsyncClient",
        FakeExecutionsAsyncClient,
    )

    monkeypatch.setattr(
        "schedule_load_backfill.__main__.run_backfill",
        fake_run_backfill,
    )

    main()
