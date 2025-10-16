# TODO: This file is AI generated and needs review

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock

import pytest
import rio

from rio_jobs import JobScheduler, Run, ScheduledJob


# Fixtures
@pytest.fixture
def mock_rio_app() -> Mock:
    """Create a mock Rio app for testing."""
    app = Mock(spec=rio.App)
    app.name = "test_app"
    return app


@pytest.fixture
def scheduler() -> JobScheduler:
    """Create a fresh JobScheduler instance for each test."""
    return JobScheduler()


@pytest.fixture
def now_utc() -> datetime:
    """Return current time in UTC timezone."""
    return datetime.now(timezone.utc)


def test_is_running_property(mock_rio_app: Mock, now_utc: datetime) -> None:
    """Test that _is_running correctly reports running state."""
    run = Run(
        started_at=now_utc,
        app=mock_rio_app,
        _finished_at=None,
        status_message="",
        progress=None,
        _log_messages=[],
        _result=None,
    )

    assert run._is_running is True

    run._finished_at = now_utc + timedelta(seconds=10)
    assert run._is_running is False


def test_has_succeeded_property(mock_rio_app: Mock, now_utc: datetime) -> None:
    """Test that _has_succeeded correctly identifies successful runs."""
    run = Run(
        started_at=now_utc,
        app=mock_rio_app,
        _finished_at=None,
        status_message="",
        progress=None,
        _log_messages=[],
        _result=None,
    )

    # Running job hasn't succeeded yet
    assert run._has_succeeded is False

    # Finished job with no exception
    run._finished_at = now_utc + timedelta(seconds=10)
    run._result = None
    assert run._has_succeeded is True

    # Finished job with exception
    run._result = ValueError("test error")
    assert run._has_succeeded is False


def test_has_failed_property(mock_rio_app: Mock, now_utc: datetime) -> None:
    """Test that _has_failed correctly identifies failed runs."""
    run = Run(
        started_at=now_utc,
        app=mock_rio_app,
        _finished_at=None,
        status_message="",
        progress=None,
        _log_messages=[],
        _result=None,
    )

    # Running job hasn't failed
    assert run._has_failed is False

    # Finished job without exception
    run._finished_at = now_utc + timedelta(seconds=10)
    run._result = None
    assert run._has_failed is False

    # Finished job with exception
    run._result = ValueError("test error")
    assert run._has_failed is True


def test_log_methods(mock_rio_app: Mock, now_utc: datetime) -> None:
    """Test that logging methods correctly store messages."""
    run = Run(
        started_at=now_utc,
        app=mock_rio_app,
        _finished_at=None,
        status_message="",
        progress=None,
        _log_messages=[],
        _result=None,
    )

    run._debug("debug message")
    run._info("info message")
    run._warning("warning message")
    run._error("error message")
    run._critical("critical message")

    assert len(run._log_messages) == 5

    assert run._log_messages[0][1] == "debug"
    assert run._log_messages[0][2] == "debug message"

    assert run._log_messages[1][1] == "info"
    assert run._log_messages[1][2] == "info message"

    assert run._log_messages[2][1] == "warning"
    assert run._log_messages[2][2] == "warning message"

    assert run._log_messages[3][1] == "error"
    assert run._log_messages[3][2] == "error message"

    assert run._log_messages[4][1] == "critical"
    assert run._log_messages[4][2] == "critical message"


def test_add_job(scheduler: JobScheduler) -> None:
    """Test that add_job registers a job and handles parameters correctly."""

    def sample_job(run: Run):
        pass

    result = scheduler.add_job(
        sample_job,
        timedelta(seconds=60),
        name="Custom Job Name",
        wait_for_initial_interval=False,
        soft_start=False,
    )

    assert result is scheduler  # Should return self for chaining
    assert len(scheduler._job_objects) == 1
    assert scheduler._job_objects[0].callback == sample_job
    assert scheduler._job_objects[0].interval == timedelta(seconds=60)
    assert scheduler._job_objects[0].name == "Custom Job Name"
    assert scheduler._job_objects[0].wait_for_initial_interval is False
    assert scheduler._job_objects[0].soft_start is False


def test_add_job_auto_generates_name(scheduler: JobScheduler) -> None:
    """Test that add_job auto-generates names from function names."""

    def my_test_job(run: Run):
        pass

    scheduler.add_job(my_test_job, timedelta(seconds=60))

    # Function name is converted to title case with underscores as spaces
    # The name will include the full qualified path for nested functions
    assert "My Test Job" in scheduler._job_objects[0].name


def test_add_job_validation_not_callable(scheduler: JobScheduler) -> None:
    """Test that add_job rejects non-callable objects."""
    with pytest.raises(ValueError, match="should be a callable"):
        # Intentionally pass an invalid type to test validation error handling.
        scheduler.add_job("not a function", timedelta(seconds=60))  # type: ignore[arg-type]


def test_add_job_validation_negative_interval(scheduler: JobScheduler) -> None:
    """Test that add_job rejects negative intervals."""

    def sample_job(run: Run):
        pass

    with pytest.raises(ValueError, match="should be greater than zero"):
        scheduler.add_job(sample_job, timedelta(seconds=-10))


def test_add_job_validation_zero_interval(scheduler: JobScheduler) -> None:
    """Test that add_job rejects zero intervals."""

    def sample_job(run: Run):
        pass

    with pytest.raises(ValueError, match="should be greater than zero"):
        scheduler.add_job(sample_job, timedelta(seconds=0))


def test_add_job_validation_invalid_name(scheduler: JobScheduler) -> None:
    """Test that add_job rejects invalid name types."""

    def sample_job(run: Run):
        pass

    with pytest.raises(ValueError, match="should be a string or `None`"):
        # Intentionally pass an invalid type to test validation error handling.
        scheduler.add_job(sample_job, timedelta(seconds=60), name=123)  # type: ignore[arg-type]


def test_job_decorator(scheduler: JobScheduler) -> None:
    """Test that @scheduler.job decorator works and handles parameters correctly."""

    @scheduler.job(
        timedelta(seconds=30),
        name="Custom Decorator Job",
        wait_for_initial_interval=False,
        soft_start=False,
    )
    def decorated_job(run: Run):
        pass

    assert len(scheduler._job_objects) == 1
    assert scheduler._job_objects[0].callback == decorated_job
    assert scheduler._job_objects[0].interval == timedelta(seconds=30)
    assert scheduler._job_objects[0].name == "Custom Decorator Job"
    assert scheduler._job_objects[0].wait_for_initial_interval is False
    assert scheduler._job_objects[0].soft_start is False


def test_job_decorator_returns_function(scheduler: JobScheduler) -> None:
    """Test that decorator returns the original function."""

    def original_func(run: Run):
        pass

    decorated = scheduler.job(timedelta(seconds=30))(original_func)

    assert decorated is original_func


def test_multiple_jobs(scheduler: JobScheduler) -> None:
    """Test that multiple jobs can be registered."""

    def job1(run: Run):
        pass

    def job2(run: Run):
        pass

    def job3(run: Run):
        pass

    scheduler.add_job(job1, timedelta(seconds=10))
    scheduler.add_job(job2, timedelta(seconds=20))
    scheduler.add_job(job3, timedelta(seconds=30))

    assert len(scheduler._job_objects) == 3


def test_get_next_run_time_with_none(scheduler: JobScheduler) -> None:
    """Test that returning None uses the default interval."""

    def sample_job(run: Run):
        pass

    scheduler.add_job(sample_job, timedelta(seconds=60))
    job = scheduler._job_objects[0]

    before = datetime.now(timezone.utc)
    next_run = scheduler._get_next_run_time(job, None)
    after = datetime.now(timezone.utc)

    # Should be approximately now + 60 seconds
    expected_min = before + timedelta(seconds=60)
    expected_max = after + timedelta(seconds=60)

    # Verify it returns a datetime (not "never") when given None.
    assert isinstance(next_run, datetime)
    assert expected_min <= next_run <= expected_max


def test_get_next_run_time_with_datetime(
    scheduler: JobScheduler, now_utc: datetime
) -> None:
    """Test that returning a datetime uses that specific time."""

    def sample_job(run: Run):
        pass

    scheduler.add_job(sample_job, timedelta(seconds=60))
    job = scheduler._job_objects[0]

    specific_time = now_utc + timedelta(hours=2)
    next_run = scheduler._get_next_run_time(job, specific_time)

    assert next_run == specific_time.astimezone(timezone.utc)


def test_get_next_run_time_with_timedelta(scheduler: JobScheduler) -> None:
    """Test that returning a timedelta uses that as the next interval."""

    def sample_job(run: Run):
        pass

    scheduler.add_job(sample_job, timedelta(seconds=60))
    job = scheduler._job_objects[0]

    before = datetime.now(timezone.utc)
    next_run = scheduler._get_next_run_time(job, timedelta(seconds=120))
    after = datetime.now(timezone.utc)

    # Should be approximately now + 120 seconds
    expected_min = before + timedelta(seconds=120)
    expected_max = after + timedelta(seconds=120)

    # Verify it returns a datetime (not "never") when given a timedelta.
    assert isinstance(next_run, datetime)
    assert expected_min <= next_run <= expected_max


def test_get_next_run_time_with_never(scheduler: JobScheduler) -> None:
    """Test that returning 'never' unschedules the job."""

    def sample_job(run: Run):
        pass

    scheduler.add_job(sample_job, timedelta(seconds=60))
    job = scheduler._job_objects[0]

    next_run = scheduler._get_next_run_time(job, "never")

    assert next_run == "never"


def test_get_next_run_time_with_invalid_result(scheduler: JobScheduler) -> None:
    """Test that invalid results fall back to default interval."""

    def sample_job(run: Run):
        pass

    scheduler.add_job(sample_job, timedelta(seconds=60))
    job = scheduler._job_objects[0]

    before = datetime.now(timezone.utc)
    next_run = scheduler._get_next_run_time(job, "invalid")
    after = datetime.now(timezone.utc)

    # Should fall back to default interval (60 seconds)
    expected_min = before + timedelta(seconds=60)
    expected_max = after + timedelta(seconds=60)

    # Verify it returns a datetime (not "never") when given invalid input.
    assert isinstance(next_run, datetime)
    assert expected_min <= next_run <= expected_max


def test_limit_past_runs_by_count(
    scheduler: JobScheduler, mock_rio_app: Mock, now_utc: datetime
) -> None:
    """Test that old runs are removed when count exceeds limit and they're outside time window."""
    scheduler = JobScheduler(
        keep_past_runs_for=timedelta(hours=1),
        keep_past_n_runs=3,
    )

    # Create 5 runs: 3 recent (within 1 hour) + 2 old (beyond 1 hour)
    runs = []
    # 2 old runs (beyond time threshold)
    for i in range(2):
        run = Run(
            started_at=now_utc - timedelta(hours=5 - i),
            app=mock_rio_app,
            _finished_at=now_utc - timedelta(hours=5 - i) + timedelta(seconds=1),
            status_message="",
            progress=None,
            _log_messages=[],
            _result=None,
        )
        runs.append(run)

    # 3 recent runs (within time threshold)
    for i in range(3):
        run = Run(
            started_at=now_utc - timedelta(minutes=30 - i * 5),
            app=mock_rio_app,
            _finished_at=now_utc - timedelta(minutes=30 - i * 5) + timedelta(seconds=1),
            status_message="",
            progress=None,
            _log_messages=[],
            _result=None,
        )
        runs.append(run)

    scheduler._limit_past_runs_inplace(runs)

    # Should keep the 3 most recent runs (all within time threshold)
    # Old runs beyond both time and count thresholds are removed
    assert len(runs) == 3


def test_limit_past_runs_by_time(
    scheduler: JobScheduler, mock_rio_app: Mock, now_utc: datetime
) -> None:
    """Test that old runs are removed when they exceed time limit and count."""
    scheduler = JobScheduler(
        keep_past_runs_for=timedelta(hours=1),
        keep_past_n_runs=2,  # Keep only 2 most recent
    )

    # Create 4 runs: 2 old + 2 recent
    runs = [
        Run(
            started_at=now_utc - timedelta(hours=3),
            app=mock_rio_app,
            _finished_at=now_utc - timedelta(hours=3) + timedelta(seconds=1),
            status_message="",
            progress=None,
            _log_messages=[],
            _result=None,
        ),
        Run(
            started_at=now_utc - timedelta(hours=2),
            app=mock_rio_app,
            _finished_at=now_utc - timedelta(hours=2) + timedelta(seconds=1),
            status_message="",
            progress=None,
            _log_messages=[],
            _result=None,
        ),
        Run(
            started_at=now_utc - timedelta(minutes=30),
            app=mock_rio_app,
            _finished_at=now_utc - timedelta(minutes=30) + timedelta(seconds=1),
            status_message="",
            progress=None,
            _log_messages=[],
            _result=None,
        ),
        Run(
            started_at=now_utc - timedelta(minutes=10),
            app=mock_rio_app,
            _finished_at=now_utc - timedelta(minutes=10) + timedelta(seconds=1),
            status_message="",
            progress=None,
            _log_messages=[],
            _result=None,
        ),
    ]

    scheduler._limit_past_runs_inplace(runs)

    # Should keep the 2 most recent runs (both within 1 hour)
    # Old runs beyond both thresholds are removed
    assert len(runs) == 2
    assert runs[0].started_at == now_utc - timedelta(minutes=30)
    assert runs[1].started_at == now_utc - timedelta(minutes=10)


def test_limit_past_runs_keeps_recent_n_runs(
    scheduler: JobScheduler, mock_rio_app: Mock, now_utc: datetime
) -> None:
    """Test that the most recent N runs are always kept, even if old."""
    scheduler = JobScheduler(
        keep_past_runs_for=timedelta(hours=1),
        keep_past_n_runs=2,
    )

    # Create 3 old runs (all beyond 1 hour)
    runs = [
        Run(
            started_at=now_utc - timedelta(hours=5),
            app=mock_rio_app,
            _finished_at=now_utc - timedelta(hours=5) + timedelta(seconds=1),
            status_message="",
            progress=None,
            _log_messages=[],
            _result=None,
        ),
        Run(
            started_at=now_utc - timedelta(hours=4),
            app=mock_rio_app,
            _finished_at=now_utc - timedelta(hours=4) + timedelta(seconds=1),
            status_message="",
            progress=None,
            _log_messages=[],
            _result=None,
        ),
        Run(
            started_at=now_utc - timedelta(hours=3),
            app=mock_rio_app,
            _finished_at=now_utc - timedelta(hours=3) + timedelta(seconds=1),
            status_message="",
            progress=None,
            _log_messages=[],
            _result=None,
        ),
    ]

    scheduler._limit_past_runs_inplace(runs)

    # Should keep the 2 most recent, even though all are old
    assert len(runs) == 2
    assert runs[0].started_at == now_utc - timedelta(hours=4)
    assert runs[1].started_at == now_utc - timedelta(hours=3)





@pytest.mark.asyncio
async def test_sync_job_execution(scheduler: JobScheduler, mock_rio_app: Mock) -> None:
    """Test that synchronous jobs execute correctly."""
    executed = []

    def sync_job(run: Run):
        executed.append(True)
        return None

    scheduler._app = mock_rio_app
    scheduler.add_job(sync_job, timedelta(seconds=60), wait_for_initial_interval=False)
    job = scheduler._job_objects[0]

    # Manually set next run to now so it runs immediately
    job._next_run_at = datetime.now(timezone.utc)

    # Create and run the worker task briefly
    task = asyncio.create_task(scheduler._job_worker(job))

    # Give it time to execute once
    await asyncio.sleep(0.1)

    # Cancel the task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Job should have executed
    assert len(executed) == 1
    assert len(job.past_runs) == 1
    assert job.past_runs[0]._has_succeeded


@pytest.mark.asyncio
async def test_async_job_execution(scheduler: JobScheduler, mock_rio_app: Mock) -> None:
    """Test that asynchronous jobs execute correctly."""
    executed = []

    async def async_job(run: Run):
        executed.append(True)
        return None

    scheduler._app = mock_rio_app
    scheduler.add_job(async_job, timedelta(seconds=60), wait_for_initial_interval=False)
    job = scheduler._job_objects[0]

    # Manually set next run to now so it runs immediately
    job._next_run_at = datetime.now(timezone.utc)

    # Create and run the worker task briefly
    task = asyncio.create_task(scheduler._job_worker(job))

    # Give it time to execute once
    await asyncio.sleep(0.1)

    # Cancel the task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Job should have executed
    assert len(executed) == 1
    assert len(job.past_runs) == 1
    assert job.past_runs[0]._has_succeeded


@pytest.mark.asyncio
async def test_job_exception_handling(
    scheduler: JobScheduler, mock_rio_app: Mock
) -> None:
    """Test that exceptions in jobs are caught and logged."""

    def failing_job(run: Run):
        raise ValueError("Test error")

    scheduler._app = mock_rio_app
    scheduler.add_job(
        failing_job, timedelta(seconds=60), wait_for_initial_interval=False
    )
    job = scheduler._job_objects[0]

    # Manually set next run to now so it runs immediately
    job._next_run_at = datetime.now(timezone.utc)

    # Create and run the worker task briefly
    task = asyncio.create_task(scheduler._job_worker(job))

    # Give it time to execute once
    await asyncio.sleep(0.1)

    # Cancel the task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Job should have executed and failed
    assert len(job.past_runs) == 1
    assert job.past_runs[0]._has_failed
    assert isinstance(job.past_runs[0]._result, ValueError)
    assert str(job.past_runs[0]._result) == "Test error"


@pytest.mark.asyncio
async def test_job_returns_never(scheduler: JobScheduler, mock_rio_app: Mock) -> None:
    """Test that jobs returning 'never' are unscheduled."""
    execution_count = []

    def job_that_stops(run: Run):
        execution_count.append(True)
        return "never"

    scheduler._app = mock_rio_app
    scheduler.add_job(
        job_that_stops, timedelta(seconds=60), wait_for_initial_interval=False
    )
    job = scheduler._job_objects[0]

    # Manually set next run to now so it runs immediately
    job._next_run_at = datetime.now(timezone.utc)

    # Run the worker task
    await scheduler._job_worker(job)

    # Job should have executed once and then stopped
    assert len(execution_count) == 1
    assert job._next_run_at == "never"


@pytest.mark.asyncio
async def test_job_progress_tracking(
    scheduler: JobScheduler, mock_rio_app: Mock
) -> None:
    """Test that job progress can be tracked."""

    def job_with_progress(run: Run):
        run.status_message = "Processing"
        run.progress = 0.5
        return None

    scheduler._app = mock_rio_app
    scheduler.add_job(
        job_with_progress, timedelta(seconds=60), wait_for_initial_interval=False
    )
    job = scheduler._job_objects[0]

    # Manually set next run to now so it runs immediately
    job._next_run_at = datetime.now(timezone.utc)

    # Create and run the worker task briefly
    task = asyncio.create_task(scheduler._job_worker(job))

    # Give it time to execute once
    await asyncio.sleep(0.1)

    # Cancel the task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Check that progress was tracked
    assert len(job.past_runs) == 1
    assert job.past_runs[0].status_message == "Processing"
    assert job.past_runs[0].progress == 0.5

