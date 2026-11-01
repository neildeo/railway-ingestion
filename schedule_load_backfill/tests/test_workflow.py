from __future__ import annotations

import json

import pytest

from google.cloud.workflows.executions_v1.types import Execution

from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
    WorkflowExecutionResult,
    WorkflowTerminalState,
    run_workflow_to_completion,
)


PARENT = (
    "projects/test-project/"
    "locations/europe-west1/"
    "workflows/railway-load-schedule-object"
)

EXECUTION_NAME = (
    f"{PARENT}/executions/execution-123"
)

ARGUMENTS = WorkflowExecutionArgs(
    object=(
        "schedules/update/sequence=5251/schedules.json.gz"
    ),
    generation=1791271962017410,
    dataset="integration_test",
)


class FakeExecutionsClient:
    def __init__(
        self,
        *,
        created_execution: Execution,
        polled_executions: list[Execution] | None = None,
    ) -> None:
        self.created_execution = created_execution
        self.polled_executions = list(
            polled_executions or []
        )

        self.create_requests: list[dict] = []
        self.get_requests: list[dict] = []

    async def create_execution(
        self,
        *,
        request: dict,
    ) -> Execution:
        self.create_requests.append(request)
        return self.created_execution

    async def get_execution(
        self,
        *,
        request: dict,
    ) -> Execution:
        self.get_requests.append(request)

        if not self.polled_executions:
            raise AssertionError(
                "Unexpected get_execution call"
            )

        return self.polled_executions.pop(0)


@pytest.mark.asyncio
async def test_run_workflow_to_completion_returns_succeeded_result() -> None:
    client = FakeExecutionsClient(
        created_execution=Execution(
            name=EXECUTION_NAME,
            state=Execution.State.QUEUED,
        ),
        polled_executions=[
            Execution(
                name=EXECUTION_NAME,
                state=Execution.State.ACTIVE,
            ),
            Execution(
                name=EXECUTION_NAME,
                state=Execution.State.SUCCEEDED,
            ),
        ],
    )

    result = await run_workflow_to_completion(
        client=client,  # pyright: ignore[reportArgumentType]
        parent=PARENT,
        arguments=ARGUMENTS,
        poll_interval_seconds=0,
    )

    assert result == WorkflowExecutionResult(
        execution_name=EXECUTION_NAME,
        arguments=ARGUMENTS,
        state=WorkflowTerminalState.SUCCEEDED,
    )


@pytest.mark.asyncio
async def test_run_workflow_to_completion_creates_execution_with_arguments() -> None:
    client = FakeExecutionsClient(
        created_execution=Execution(
            name=EXECUTION_NAME,
            state=Execution.State.SUCCEEDED,
        ),
    )

    await run_workflow_to_completion(
        client=client,  # pyright: ignore[reportArgumentType]
        parent=PARENT,
        arguments=ARGUMENTS,
        poll_interval_seconds=0,
    )

    assert len(client.create_requests) == 1

    request = client.create_requests[0]

    assert request["parent"] == PARENT

    execution = request["execution"]

    assert json.loads(execution.argument) == {
        "object": (
            "schedules/update/sequence=5251/"
            "schedules.json.gz"
        ),
        "generation": 1791271962017410,
        "dataset": "integration_test",
    }


@pytest.mark.asyncio
async def test_run_workflow_to_completion_omits_dataset_when_not_supplied() -> None:
    arguments = WorkflowExecutionArgs(
        object=(
            "schedules/update/sequence=5251/"
            "schedules.json.gz"
        ),
        generation=1791271962017410,
    )

    client = FakeExecutionsClient(
        created_execution=Execution(
            name=EXECUTION_NAME,
            state=Execution.State.SUCCEEDED,
        ),
    )

    await run_workflow_to_completion(
        client=client,  # pyright: ignore[reportArgumentType]
        parent=PARENT,
        arguments=arguments,
        poll_interval_seconds=0,
    )

    execution = client.create_requests[0]["execution"]

    assert json.loads(execution.argument) == {
        "object": (
            "schedules/update/sequence=5251/"
            "schedules.json.gz"
        ),
        "generation": 1791271962017410,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("google_state", "expected_state"),
    [
        (
            Execution.State.FAILED,
            WorkflowTerminalState.FAILED,
        ),
        (
            Execution.State.CANCELLED,
            WorkflowTerminalState.CANCELLED,
        ),
    ],
)
async def test_run_workflow_to_completion_returns_terminal_failure(
    google_state: Execution.State,
    expected_state: WorkflowTerminalState,
) -> None:
    client = FakeExecutionsClient(
        created_execution=Execution(
            name=EXECUTION_NAME,
            state=Execution.State.ACTIVE,
        ),
        polled_executions=[
            Execution(
                name=EXECUTION_NAME,
                state=google_state,
            ),
        ],
    )

    result = await run_workflow_to_completion(
        client=client,  # pyright: ignore[reportArgumentType]
        parent=PARENT,
        arguments=ARGUMENTS,
        poll_interval_seconds=0,
    )

    assert result == WorkflowExecutionResult(
        execution_name=EXECUTION_NAME,
        arguments=ARGUMENTS,
        state=expected_state,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_state",
    [
        Execution.State.SUCCEEDED,
        Execution.State.FAILED,
        Execution.State.CANCELLED,
    ],
)
async def test_run_workflow_to_completion_does_not_poll_terminal_execution(
    terminal_state: Execution.State,
) -> None:
    client = FakeExecutionsClient(
        created_execution=Execution(
            name=EXECUTION_NAME,
            state=terminal_state,
        ),
    )

    await run_workflow_to_completion(
        client=client,  # pyright: ignore[reportArgumentType]
        parent=PARENT,
        arguments=ARGUMENTS,
        poll_interval_seconds=0,
    )

    assert client.get_requests == []


@pytest.mark.asyncio
async def test_run_workflow_to_completion_continues_polling_unavailable_state() -> None:
    client = FakeExecutionsClient(
        created_execution=Execution(
            name=EXECUTION_NAME,
            state=Execution.State.ACTIVE,
        ),
        polled_executions=[
            Execution(
                name=EXECUTION_NAME,
                state=Execution.State.UNAVAILABLE,
            ),
            Execution(
                name=EXECUTION_NAME,
                state=Execution.State.ACTIVE,
            ),
            Execution(
                name=EXECUTION_NAME,
                state=Execution.State.SUCCEEDED,
            ),
        ],
    )

    result = await run_workflow_to_completion(
        client=client,  # pyright: ignore[reportArgumentType]
        parent=PARENT,
        arguments=ARGUMENTS,
        poll_interval_seconds=0,
    )

    assert result.state is WorkflowTerminalState.SUCCEEDED
    assert len(client.get_requests) == 3


@pytest.mark.asyncio
async def test_run_workflow_to_completion_polls_exact_execution() -> None:
    client = FakeExecutionsClient(
        created_execution=Execution(
            name=EXECUTION_NAME,
            state=Execution.State.ACTIVE,
        ),
        polled_executions=[
            Execution(
                name=EXECUTION_NAME,
                state=Execution.State.SUCCEEDED,
            ),
        ],
    )

    await run_workflow_to_completion(
        client=client,  # pyright: ignore[reportArgumentType]
        parent=PARENT,
        arguments=ARGUMENTS,
        poll_interval_seconds=0,
    )

    assert client.get_requests == [
        {
            "name": EXECUTION_NAME,
        }
    ]


@pytest.mark.asyncio
async def test_run_workflow_to_completion_rejects_unspecified_state() -> None:
    client = FakeExecutionsClient(
        created_execution=Execution(
            name=EXECUTION_NAME,
            state=Execution.State.STATE_UNSPECIFIED,
        ),
    )

    with pytest.raises(
        ValueError,
        match="STATE_UNSPECIFIED",
    ):
        await run_workflow_to_completion(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            arguments=ARGUMENTS,
            poll_interval_seconds=0,
        )


@pytest.mark.asyncio
async def test_run_workflow_to_completion_propagates_create_error() -> None:
    failure = RuntimeError("create failed")

    class FailingCreateClient:
        async def create_execution(
            self,
            *,
            request: dict,
        ) -> Execution:
            raise failure

    with pytest.raises(RuntimeError) as exc_info:
        await run_workflow_to_completion(
            client=FailingCreateClient(),  # type: ignore
            parent=PARENT,
            arguments=ARGUMENTS,
            poll_interval_seconds=0,
        )

    assert exc_info.value is failure


@pytest.mark.asyncio
async def test_run_workflow_to_completion_propagates_poll_error() -> None:
    failure = RuntimeError("poll failed")

    class FailingPollClient:
        async def create_execution(
            self,
            *,
            request: dict,
        ) -> Execution:
            return Execution(
                name=EXECUTION_NAME,
                state=Execution.State.ACTIVE,
            )

        async def get_execution(
            self,
            *,
            request: dict,
        ) -> Execution:
            raise failure

    with pytest.raises(RuntimeError) as exc_info:
        await run_workflow_to_completion(
            client=FailingPollClient(),  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            arguments=ARGUMENTS,
            poll_interval_seconds=0,
        )

    assert exc_info.value is failure
