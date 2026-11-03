from __future__ import annotations

import asyncio
import json

from google.cloud.workflows.executions_v1.types import Execution
import pytest

from schedule_load_backfill.orchestrator import (
    execute_workflows,
)
from schedule_load_backfill.workflow import (
    WorkflowExecutionArgs,
    WorkflowExecutionResult,
    WorkflowTerminalState,
)


PARENT = (
    "projects/test-project/"
    "locations/europe-west1/"
    "workflows/railway-load-schedule-object"
)

ARGUMENTS_A = WorkflowExecutionArgs(
    object="schedules/update/sequence=5251/schedules.json.gz",
    generation=111,
)

ARGUMENTS_B = WorkflowExecutionArgs(
    object="schedules/update/sequence=5252/schedules.json.gz",
    generation=222,
)

ARGUMENTS_C = WorkflowExecutionArgs(
    object="schedules/update/sequence=5253/schedules.json.gz",
    generation=333,
)

ARGUMENTS_D = WorkflowExecutionArgs(
    object="schedules/update/sequence=5254/schedules.json.gz",
    generation=444,
)

ARGUMENTS_E = WorkflowExecutionArgs(
    object="schedules/update/sequence=5255/schedules.json.gz",
    generation=555,
)

EXECUTION_A = f"{PARENT}/executions/execution-a"
EXECUTION_B = f"{PARENT}/executions/execution-b"
EXECUTION_C = f"{PARENT}/executions/execution-c"
EXECUTION_D = f"{PARENT}/executions/execution-d"
EXECUTION_E = f"{PARENT}/executions/execution-e"

EXECUTION_NAMES = {
    ARGUMENTS_A.object: EXECUTION_A,
    ARGUMENTS_B.object: EXECUTION_B,
    ARGUMENTS_C.object: EXECUTION_C,
    ARGUMENTS_D.object: EXECUTION_D,
    ARGUMENTS_E.object: EXECUTION_E,
}


class BarrierExecutionsClient:
    def __init__(self) -> None:
        self.poll_barrier = asyncio.Barrier(2)

        self.create_requests: list[dict] = []
        self.get_requests: list[dict] = []

    async def create_execution(
        self,
        *,
        request: dict,
    ) -> Execution:
        self.create_requests.append(request)

        arguments = json.loads(
            request["execution"].argument
        )

        return Execution(
            name=EXECUTION_NAMES[
                arguments["object"]
            ],
            state=Execution.State.ACTIVE,
        )

    async def get_execution(
        self,
        *,
        request: dict,
    ) -> Execution:
        self.get_requests.append(request)

        await self.poll_barrier.wait()

        return Execution(
            name=request["name"],
            state=Execution.State.SUCCEEDED,
        )


class ControlledExecutionsClient:
    def __init__(self) -> None:
        self.create_requests: list[dict] = []

        self.release_events = {
            execution_name: asyncio.Event()
            for execution_name in EXECUTION_NAMES.values()
        }

        self.terminal_states = {
            execution_name: Execution.State.SUCCEEDED
            for execution_name in EXECUTION_NAMES.values()
        }

    async def create_execution(
        self,
        *,
        request: dict,
    ) -> Execution:
        self.create_requests.append(request)

        arguments = WorkflowExecutionArgs(
            **json.loads(
                request["execution"].argument
            )
        )

        execution_name = EXECUTION_NAMES[
            arguments.object
        ]

        return Execution(
            name=execution_name,
            state=Execution.State.ACTIVE,
        )

    async def get_execution(
        self,
        *,
        request: dict,
    ) -> Execution:
        execution_name = request["name"]

        await self.release_events[
            execution_name
        ].wait()

        return Execution(
            name=execution_name,
            state=self.terminal_states[
                execution_name
            ],
        )

    def release(
        self,
        execution_name: str,
        *,
        state: Execution.State = (
            Execution.State.SUCCEEDED
        ),
    ) -> None:
        self.terminal_states[
            execution_name
        ] = state

        self.release_events[
            execution_name
        ].set()

    @property
    def started_arguments(
        self,
    ) -> set[WorkflowExecutionArgs]:
        return {
            WorkflowExecutionArgs(
                **json.loads(
                    request["execution"].argument
                )
            )
            for request in self.create_requests
        }


class FailingExecutionsClient(ControlledExecutionsClient):
    def __init__(self) -> None:
        super().__init__()

        self.poll_errors: dict[str, Exception] = {}

    async def get_execution(
        self,
        *,
        request: dict,
    ) -> Execution:
        execution_name = request["name"]

        await self.release_events[
            execution_name
        ].wait()

        if execution_name in self.poll_errors:
            raise self.poll_errors[
                execution_name
            ]

        return Execution(
            name=execution_name,
            state=self.terminal_states[
                execution_name
            ],
        )

    def fail(
        self,
        execution_name: str,
        *,
        error: Exception,
    ) -> None:
        self.poll_errors[
            execution_name
        ] = error

        self.release_events[
            execution_name
        ].set()


async def wait_until(
    predicate,
) -> None:
    for _ in range(100):
        if predicate():
            return

        await asyncio.sleep(0)

    raise AssertionError(
        "Condition was not reached"
    )


@pytest.mark.asyncio
async def test_execute_workflows_runs_executions_concurrently() -> None:
    client = BarrierExecutionsClient()

    results = await asyncio.wait_for(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
            ],
            max_in_flight=2,
            poll_interval_seconds=0,
        ),
        timeout=1,
    )

    assert set(results) == {
        WorkflowExecutionResult(
            execution_name=EXECUTION_A,
            arguments=ARGUMENTS_A,
            state=WorkflowTerminalState.SUCCEEDED,
        ),
        WorkflowExecutionResult(
            execution_name=EXECUTION_B,
            arguments=ARGUMENTS_B,
            state=WorkflowTerminalState.SUCCEEDED,
        ),
    }


@pytest.mark.asyncio
async def test_execute_workflows_starts_all_executions() -> None:
    client = BarrierExecutionsClient()

    await asyncio.wait_for(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
            ],
            max_in_flight=2,
            poll_interval_seconds=0,
        ),
        timeout=1,
    )

    created_arguments = {
        WorkflowExecutionArgs(
            **json.loads(
                request["execution"].argument
            )
        )
        for request in client.create_requests
    }

    assert created_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
    }


@pytest.mark.asyncio
async def test_execute_workflows_respects_max_in_flight() -> None:
    client = ControlledExecutionsClient()

    task = asyncio.create_task(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
                ARGUMENTS_C,
                ARGUMENTS_D,
                ARGUMENTS_E,
            ],
            max_in_flight=2,
            poll_interval_seconds=0,
        )
    )

    await wait_until(
        lambda: len(client.create_requests) == 2
    )

    assert client.started_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
    }

    client.release(EXECUTION_A)

    await wait_until(
        lambda: len(client.create_requests) == 3
    )

    assert client.started_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
        ARGUMENTS_C,
    }

    client.release(EXECUTION_B)

    await wait_until(
        lambda: len(client.create_requests) == 4
    )

    assert client.started_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
        ARGUMENTS_C,
        ARGUMENTS_D,
    }

    client.release(EXECUTION_C)
    client.release(EXECUTION_D)

    await wait_until(
        lambda: len(client.create_requests) == 5
    )

    client.release(EXECUTION_E)

    results = await asyncio.wait_for(
        task,
        timeout=1,
    )

    assert {
        result.arguments
        for result in results
    } == {
        ARGUMENTS_A,
        ARGUMENTS_B,
        ARGUMENTS_C,
        ARGUMENTS_D,
        ARGUMENTS_E,
    }


@pytest.mark.asyncio
async def test_execute_workflows_refills_slot_when_any_execution_completes() -> None:
    client = ControlledExecutionsClient()

    task = asyncio.create_task(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
                ARGUMENTS_C,
            ],
            max_in_flight=2,
            poll_interval_seconds=0,
        )
    )

    await wait_until(
        lambda: len(client.create_requests) == 2
    )

    assert client.started_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
    }

    client.release(EXECUTION_B)

    await wait_until(
        lambda: len(client.create_requests) == 3
    )

    assert client.started_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
        ARGUMENTS_C,
    }

    client.release(EXECUTION_A)
    client.release(EXECUTION_C)

    results = await asyncio.wait_for(
        task,
        timeout=1,
    )

    assert {
        result.arguments
        for result in results
    } == {
        ARGUMENTS_A,
        ARGUMENTS_B,
        ARGUMENTS_C,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_state",
    [
        Execution.State.FAILED,
        Execution.State.CANCELLED,
    ],
)
async def test_execute_workflows_stops_admission_after_unsuccessful_execution(
    terminal_state: Execution.State,
) -> None:
    client = ControlledExecutionsClient()

    task = asyncio.create_task(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
                ARGUMENTS_C,
                ARGUMENTS_D,
            ],
            max_in_flight=2,
            poll_interval_seconds=0,
        )
    )

    await wait_until(
        lambda: len(client.create_requests) == 2
    )

    client.release(
        EXECUTION_B,
        state=terminal_state,
    )

    await asyncio.sleep(0)

    assert client.started_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
    }

    client.release(EXECUTION_A)

    results = await asyncio.wait_for(
        task,
        timeout=1,
    )

    assert {
        result.arguments
        for result in results
    } == {
        ARGUMENTS_A,
        ARGUMENTS_B,
    }


@pytest.mark.asyncio
async def test_execute_workflows_drains_in_flight_executions_after_failure() -> None:
    client = ControlledExecutionsClient()

    task = asyncio.create_task(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
                ARGUMENTS_C,
            ],
            max_in_flight=2,
            poll_interval_seconds=0,
        )
    )

    await wait_until(
        lambda: len(client.create_requests) == 2
    )

    client.release(
        EXECUTION_B,
        state=Execution.State.FAILED,
    )

    await asyncio.sleep(0)

    assert not task.done()

    client.release(EXECUTION_A)

    results = await asyncio.wait_for(
        task,
        timeout=1,
    )

    assert set(results) == {
        WorkflowExecutionResult(
            execution_name=EXECUTION_A,
            arguments=ARGUMENTS_A,
            state=WorkflowTerminalState.SUCCEEDED,
        ),
        WorkflowExecutionResult(
            execution_name=EXECUTION_B,
            arguments=ARGUMENTS_B,
            state=WorkflowTerminalState.FAILED,
        ),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "max_in_flight",
    [
        0,
        -1,
    ],
)
async def test_execute_workflows_rejects_invalid_max_in_flight(
    max_in_flight: int,
) -> None:
    client = ControlledExecutionsClient()

    with pytest.raises(
        ValueError,
        match="max_in_flight",
    ):
        await asyncio.wait_for(
            execute_workflows(
                client=client,  # pyright: ignore[reportArgumentType]
                parent=PARENT,
                execution_configs=[
                    ARGUMENTS_A,
                ],
                max_in_flight=max_in_flight,
                poll_interval_seconds=0,
            ),
            timeout=1,
        )

    assert client.create_requests == []


@pytest.mark.asyncio
async def test_execute_workflows_handles_fewer_configs_than_max_in_flight() -> None:
    client = ControlledExecutionsClient()

    task = asyncio.create_task(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
            ],
            max_in_flight=5,
            poll_interval_seconds=0,
        )
    )

    await wait_until(
        lambda: len(client.create_requests) == 2
    )

    assert client.started_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
    }

    client.release(EXECUTION_A)
    client.release(EXECUTION_B)

    results = await asyncio.wait_for(
        task,
        timeout=1,
    )

    assert set(results) == {
        WorkflowExecutionResult(
            execution_name=EXECUTION_A,
            arguments=ARGUMENTS_A,
            state=WorkflowTerminalState.SUCCEEDED,
        ),
        WorkflowExecutionResult(
            execution_name=EXECUTION_B,
            arguments=ARGUMENTS_B,
            state=WorkflowTerminalState.SUCCEEDED,
        ),
    }


@pytest.mark.asyncio
async def test_execute_workflows_stops_admission_drains_and_reraises_orchestration_error() -> None:
    client = FailingExecutionsClient()

    task = asyncio.create_task(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
                ARGUMENTS_C,
            ],
            max_in_flight=2,
            poll_interval_seconds=0,
        )
    )

    await wait_until(
        lambda: len(client.create_requests) == 2
    )

    failure = RuntimeError("poll failed")

    client.fail(
        EXECUTION_B,
        error=failure,
    )

    # B has failed, but A is still deliberately blocked.
    #
    # A correct orchestrator must therefore remain alive so that it
    # can drain A before propagating B's exception.
    #
    # shield() prevents the timeout from cancelling the orchestrator.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            asyncio.shield(task),
            timeout=0.05,
        )

    # No further work may have been admitted while draining.
    assert client.started_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
    }

    # Allow the remaining admitted workflow to finish.
    client.release(EXECUTION_A)

    # Only now may the orchestration error escape.
    with pytest.raises(RuntimeError) as exc_info:
        await asyncio.wait_for(
            task,
            timeout=1,
        )

    assert exc_info.value is failure

    # C must never have been admitted at any point.
    assert client.started_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
    }


@pytest.mark.asyncio
async def test_execute_workflows_raises_exception_group_for_multiple_orchestration_errors() -> None:
    client = FailingExecutionsClient()

    task = asyncio.create_task(
        execute_workflows(
            client=client,  # pyright: ignore[reportArgumentType]
            parent=PARENT,
            execution_configs=[
                ARGUMENTS_A,
                ARGUMENTS_B,
                ARGUMENTS_C,
                ARGUMENTS_D,
            ],
            max_in_flight=3,
            poll_interval_seconds=0,
        )
    )

    await wait_until(
        lambda: len(client.create_requests) == 3
    )

    failure_a = ValueError("A poll failed")
    failure_b = RuntimeError("B poll failed")

    client.fail(
        EXECUTION_A,
        error=failure_a,
    )

    client.fail(
        EXECUTION_B,
        error=failure_b,
    )

    await asyncio.sleep(0)

    assert client.started_arguments == {
        ARGUMENTS_A,
        ARGUMENTS_B,
        ARGUMENTS_C,
    }

    assert not task.done()

    client.release(EXECUTION_C)

    with pytest.raises(ExceptionGroup) as exc_info:
        await asyncio.wait_for(
            task,
            timeout=1,
        )

    assert set(exc_info.value.exceptions) == {
        failure_a,
        failure_b,
    }
