"""
A job scheduler / task scheduler extension for Rio. Python functions can be
scheduled to run periodically, with the scheduler taking care of catching
exceptions and rescheduling the job for the future.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import logging
import time
import revel
import typing as t
from datetime import datetime, timedelta, timezone

import rio

__all__ = [
    "JobScheduler",
]


T = t.TypeVar("T")
P = t.ParamSpec("P")


JobFunction: t.TypeAlias = t.Callable[
    [],
    datetime
    | None
    | timedelta
    | t.Literal["never"]
    | t.Awaitable[None | datetime | timedelta | t.Literal["never"],],
]

J = t.TypeVar("J", bound=JobFunction)


LogLevel = t.Literal[
    "debug",
    "info",
    "warning",
    "error",
    "critical",
]

_logger = logging.getLogger(__name__)


def _get_function_name(
    func: t.Callable,
) -> str:
    """
    Given a function, return a nice, recognizable name for it.
    """

    # Is this a method?
    try:
        self = func.__self__  # type: ignore

    # Nope, just a function
    except AttributeError:
        try:
            return func.__qualname__
        except AttributeError:
            return repr(func)

    # Yes, include the class name
    return f"{type(self).__name__}.{func.__name__}"


async def _call_sync_or_async_function(
    func: t.Callable[P, T | t.Awaitable[T]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """
    Calls a function, which can be either synchronous or asynchronous and
    returns its result. All exceptions are propagated.
    """
    # Call the function
    result = func(*args, **kwargs)

    # If the result needs awaiting, do that
    if inspect.isawaitable(result):
        result = await result

    # Done
    return result  # type: ignore


@dataclasses.dataclass
class Run:
    """
    A past or current run of a job.
    """

    # When the job was started. Always has a timezone set.
    started_at: datetime

    # When the job was finished. Always has a timezone set. This is `None` if
    # the job is still running.
    finished_at: datetime | None

    # All messages logged to this run
    log_messages: list[tuple[datetime, LogLevel, str]]

    # The result of the job, or, if the job failed, the raised exception. This
    # is `None` if the job is still running.
    result: datetime | timedelta | t.Literal["never"] | None | BaseException

    @property
    def is_running(self) -> bool:
        """
        Whether the job is still running.

        Returns `True` if the job is still in progress and `False` otherwise.
        """
        return self.finished_at is None

    @property
    def has_succeeded(self) -> bool:
        """
        Whether the job completed successfully.

        Returns `True` if the job has finished and did not raise an exception
        and `False` otherwise.
        """
        return self.finished_at is not None and not isinstance(
            self.result, BaseException
        )

    @property
    def has_failed(self) -> bool:
        """
        Whether the job has failed.

        Returns `True` if the job has finished and raised an exception and
        `False` otherwise.
        """
        return self.finished_at is not None and isinstance(self.result, BaseException)

    def log(self, level: LogLevel, message: str) -> None:
        """
        Logs a message to the job's run.

        This is similar to `logging.log`, but the message is stored with the
        job's run. This allows you to inspect the log messages of a job after
        it has run.
        """
        self.log_messages.append(
            (
                datetime.now(timezone.utc),
                level,
                message,
            )
        )

    def debug(self, message: str) -> None:
        """
        Logs a debug message to the job's run.

        This is similar to `logging.debug`, but the message is stored with the
        job's run. This allows you to inspect the log messages of a job after
        it has run.
        """
        self.log("debug", message)

    def info(self, message: str) -> None:
        """
        Logs an info message to the job's run.

        This is similar to `logging.info`, but the message is stored with the
        job's run. This allows you to inspect the log messages of a job after
        it has run.
        """
        self.log("info", message)

    def warning(self, message: str) -> None:
        """
        Logs a warning message to the job's run.

        This is similar to `logging.warning`, but the message is stored with the
        job's run. This allows you to inspect the log messages of a job after
        it has run.
        """
        self.log("warning", message)

    def error(self, message: str) -> None:
        """
        Logs an error message to the job's run.

        This is similar to `logging.error`, but the message is stored with the
        job's run. This allows you to inspect the log messages of a job after
        it has run.
        """
        self.log("error", message)

    def critical(self, message: str) -> None:
        """
        Logs a critical message to the job's run.

        This is similar to `logging.critical`, but the message is stored with
        the job's run. This allows you to inspect the log messages of a job
        after it has run.
        """
        self.log("critical", message)


@dataclasses.dataclass
class ScheduledJob:
    """
    A job that has been scheduled.
    """

    # The callback to run. Can be synchronous or asynchronous.
    callback: JobFunction

    # The interval at which this function is configured to run.
    interval: timedelta

    # A name for the job
    name: str

    # Whether the job should wait for the interval duration before running for
    # the first time
    wait_for_initial_interval: bool

    # Whether the job should be staggered to avoid a load spike at the app's
    # start
    soft_start: bool

    # Tracks past runs. All jobs in here have returned, either successfully or
    # with an exception. In-flight jobs are not tracked.
    #
    # Older runs are at the start of the list, newer runs at the end.
    past_runs: list[Run]

    # The next time the job is due to run
    _next_run_at: datetime | t.Literal["never"]

    # If the scheduler is running, this is the asyncio task driving this
    # particular job
    _task: asyncio.Task | None


class JobScheduler(rio.Extension):
    """
    An extension for scheduling recurring jobs with Rio.

    It's common for apps to have jobs that must be run at regular intervals.
    This extension provides a way to schedule such jobs, in a way that is both
    easy and guards against crashes.
    """

    def __init__(
        self,
        *,
        keep_past_runs_for: timedelta = timedelta(days=1),
        keep_past_n_runs: int = 10,
    ) -> None:
        """
        Creates a new job scheduler.

        Creates a new scheduler for background jobs in your Rio app. Jobs can be
        scheduled to run periodically, with the scheduler taking care of
        catching exceptions and rescheduling the job for the future.

        The scheduler will keep track of past and current runs so they can be
        inspected for debugging and monitoring. (TODO: There is no public API
        for this yet.)

        ## Parameters

        `keep_past_runs_for`: All jobs will be kept in memory for at least this
            long so they can be inspected.

        `keep_past_n_runs`: How many runs of each job to keep in memory,
            regardless of how long ago they happened. This is useful for
            debugging and monitoring.
        """

        # Chain up
        super().__init__()

        # These control for how long past runs are kept, so they can be
        # inspected e.g. in an admin interface.
        self._keep_past_runs_for = keep_past_runs_for
        self._keep_past_n_runs = keep_past_n_runs

        # Stores all scheduled jobs
        self._job_objects: list[ScheduledJob] = []

        # Whether the app has already started
        self._is_running = False

    def add_job(
        self,
        job: JobFunction,
        interval: timedelta,
        *,
        name: str | None = None,
        wait_for_initial_interval: bool = True,
        soft_start: bool = True,
    ) -> JobScheduler:
        """
        Schedules a job to run periodically.

        Schedules `job` to be called every `interval`. This function takes care
        not to crash, even if the job fails. Exceptions are caught, logged and
        the job will be scheduled again in the future. Runs are not overlapping,
        meaning the next run will only start after the previous one has
        finished, plus the configured interval.

        If `wait_for_initial_interval` is `True`, the job will wait for
        `interval` before being called for the first time. If `False`, it will
        be run immediately instead, and the interval only starts counting after
        that.

        When an app starts for the first time, many jobs can be scheduled to run
        simultaneously. This can lead to undue load spikes on the system. If
        `soft_start` is `True`, the jobs will be rolled out over time, with a
        few seconds of delay between each.

        The job may optionally return a result. If it returns a `datetime`, that
        time will be taken as the next time to run the job. If it returns a
        `timedelta` it will be used as the next interval (though subsequent runs
        will still use the default interval, unless they also return a
        `timedelta`). If it returns the string `"never"`, the job will be
        unscheduled and never runs again.

        Returns the scheduler object, for easy chaining.

        Note: A decorator version of this function is available as
            `JobScheduler.job`.

        ## Parameters

        `job`: The function to run.

        `interval`: How often to run the job. If the job returns a time or
            timedelta, that will be used for the next run instead, before
            returning to this interval.

        `name`: An optional name for the job. This can help with debugging and
            deciphering logs.

        `wait_for_initial_interval`: Whether the job should wait for the
            interval duration before running for the first time. If `False`, it
            will run immediately instead, and the interval only starts counting
            after that.

        `soft_start`: Whether the job should be staggered to avoid a load spike
            at the app's start.

        ## Raises

        `ValueError`: If `interval` is less than or equal to zero.
        """
        # Validate the inputs
        if not callable(job):
            raise ValueError(f"The `job` should be a callable, not {job}")

        if interval <= timedelta(0):
            raise ValueError(
                f"The job's `interval` should be greater than zero, not {interval}"
            )

        if not isinstance(name, str) and name is not None:
            raise ValueError(
                f"The job's `name` should be a string or `None`, not {name}"
            )

        if not isinstance(wait_for_initial_interval, bool):
            raise ValueError(
                f"`wait_for_initial_interval` should be a boolean, not {wait_for_initial_interval}"
            )

        if not isinstance(soft_start, bool):
            raise ValueError(f"`soft_start` should be a boolean, not {soft_start}")

        # Get a name
        if name is None:
            name = _get_function_name(job)

        # Add the job to the list of all jobs
        job_object = ScheduledJob(
            callback=job,
            interval=interval,
            name=name,
            wait_for_initial_interval=wait_for_initial_interval,
            soft_start=soft_start,
            past_runs=[],
            # Initialize the next run time with a dummy value. This will be
            # overwritten when the task for it is created.
            _next_run_at=datetime(1970, 1, 1, tzinfo=timezone.utc),
            _task=None,
        )
        self._job_objects.append(job_object)

        # If the app has already started, queue the job
        if self._is_running:
            self._create_asyncio_task_for_job(job_object)

        # Return self for chaining
        return self

    def job(
        self,
        interval: timedelta,
        *,
        name: str | None = None,
        wait_for_initial_interval: bool = True,
        soft_start: bool = True,
    ) -> t.Callable[[J], J]:
        """
        Schedules a job to run periodically.

        Schedules `job` to be called every `interval`. This function takes care
        not to crash, even if the job fails. Exceptions are caught, logged and
        the job will be scheduled again in the future. Runs are not overlapping,
        meaning the next run will only start after the previous one has
        finished, plus the configured interval.

        If `wait_for_initial_interval` is `True`, the job will wait for
        `interval` before being called for the first time. If `False`, it will
        be run immediately instead, and the interval only starts counting after
        that.

        When an app starts for the first time, many jobs can be scheduled to run
        simultaneously. This can lead to undue load spikes on the system. If
        `soft_start` is `True`, the jobs will be rolled out over time, with a
        few seconds of delay between each.

        The job may optionally return a result. If it returns a `datetime`, that
        time will be taken as the next time to run the job. If it returns a
        `timedelta` it will be used as the next interval (though subsequent runs
        will still use the default interval, unless they also return a
        `timedelta`). If it returns the string `"never"`, the job will be
        unscheduled and never runs again.

        Note: A non-decorator version of this function is available as
            `JobScheduler.add_job`.

        ## Parameters

        `interval`: How often to run the job. If the job returns a time or
            timedelta, that will be used for the next run instead, before
            returning to this interval.

        `name`: An optional name for the job. This can help with debugging and
            deciphering logs.

        `wait_for_initial_interval`: Whether the job should wait for the
            interval duration before running for the first time. If `False`, it
            will run immediately instead, and the interval only starts counting
            after that.

        `soft_start`: Whether the job should be staggered to avoid a load spike
            at the app's start.

        ## Raises

        `ValueError`: If `interval` is less than or equal to zero.
        """

        def decorator(job_function: J) -> J:
            self.add_job(
                job_function,
                interval,
                name=name,
                wait_for_initial_interval=wait_for_initial_interval,
                soft_start=soft_start,
            )
            return job_function

        return decorator

    @rio.extension_event.on_app_start
    async def _on_app_start(
        self,
        _: rio.extension_event.ExtensionAppStartEvent,
    ) -> None:
        """
        Schedule all jobs when the app starts.
        """
        revel.debug("On app start")
        assert not self._is_running, (
            "Called on app start, but the extension is already running!?"
        )
        self._is_running = True

        # Allow the code below to assume there's at least one job
        if not self._job_objects:
            return

        # Come up with a game plan for when to start all jobs without causing
        # an undue load spike.
        #
        # All jobs that don't allow for a soft start are easy - run them now.
        soft_start_jobs: list[tuple[ScheduledJob, datetime]] = []
        now = datetime.now(timezone.utc)

        for job in self._job_objects:
            # When does this job _want to_ run?
            if job.wait_for_initial_interval:
                run_at = now + job.interval
            else:
                run_at = now

            # Remember soft-start jobs for later
            if job.soft_start:
                soft_start_jobs.append((job, run_at))
                continue

            # Queue other jobs immediately
            self._create_asyncio_task_for_job(
                job,
                run_at=run_at,
            )

        # Sort the soft-start jobs by when they'll first run
        soft_start_jobs.sort(key=lambda x: x[1])

        # Start the first job immediately
        first_job, prev_job_start_time = soft_start_jobs[0]

        self._create_asyncio_task_for_job(
            first_job,
            run_at=prev_job_start_time,
        )

        # Ensure a minimum spacing between the remainder
        for ii in range(1, len(soft_start_jobs)):
            cur_job, cur_job_start_time = soft_start_jobs[ii]

            cur_job_start_time = max(
                cur_job_start_time,
                prev_job_start_time + timedelta(seconds=30),
            )

            prev_job_start_time = cur_job_start_time

            self._create_asyncio_task_for_job(
                cur_job,
                run_at=cur_job_start_time,
            )

        # Done
        for job in self._job_objects:
            assert job._task is not None, job

    @rio.extension_event.on_app_close
    async def _on_app_close(
        self,
        _: rio.extension_event.ExtensionAppCloseEvent,
    ) -> None:
        """
        Shut down all jobs when the app closes.
        """
        revel.debug("On app close 1")

        # Cancel all jobs
        for job in self._job_objects:
            if job._task is not None:
                job._task.cancel()

        # Wait for them to finish
        await asyncio.gather(
            *[job._task for job in self._job_objects if job._task is not None],
            return_exceptions=True,
        )
        revel.debug("On app close 2")

    def _create_asyncio_task_for_job(
        self,
        job: ScheduledJob,
        *,
        run_at: datetime | None = None,
    ) -> None:
        """
        Creates an asyncio task for a job, optionally waiting until a specific
        point in time for the first run. If `run_at` is `None`, the job will be
        wait for its initial interval (if configured to do so) before running.
        """
        assert job._task is None, job

        # When to run?
        now = datetime.now(timezone.utc)

        if run_at is None:
            if job.wait_for_initial_interval:
                run_at = now + job.interval
            else:
                run_at = now
        else:
            run_at = run_at.astimezone(timezone.utc)

        # Store the time the job will next run in the job itself
        job._next_run_at = run_at

        # Log what's going on
        if run_at <= now:
            _logger.debug(f'Job "{job.name}" has been scheduled to run immediately.')
        else:
            _logger.debug(f'Job "{job.name}" has been scheduled to run at {run_at}.')

        # Create the task
        job._task = asyncio.create_task(self._job_worker(job))

    async def _wait_until(
        self,
        point_in_time: datetime,
    ) -> None:
        """
        Does the obvious. `point_in_time` must have a timezone set.
        """
        assert point_in_time.tzinfo is not None, point_in_time

        while True:
            # How long to wait?
            now = datetime.now(timezone.utc)
            wait_time = (point_in_time - now).total_seconds()

            # Done?
            if wait_time <= 0:
                break

            # Wait, but never for too long. This helps if the wall clock time
            # changes, the system doesn't handle sleeping well, or similar
            # shenanigans.
            wait_time = min(wait_time, 3600)
            await asyncio.sleep(wait_time)

    def _get_next_run_time(
        self,
        job: ScheduledJob,
        result: t.Any,
    ) -> datetime | t.Literal["never"]:
        """
        Given a job and the result of its last run, returns when it should run
        next.
        """
        now = datetime.now(timezone.utc)

        # If nothing was returned, stick to its regularly configured interval
        if result is None:
            return now + job.interval

        # If the job wants to run at a specific time, do that
        if isinstance(result, datetime):
            # Support both naive and aware datetimes
            return result.astimezone(timezone.utc)

        # If the job wants to run after a certain amount of time, accommodate
        # that
        if isinstance(result, timedelta):
            return now + result

        # Killjoy
        if result == "never":
            return "never"

        # Invalid result
        _logger.warning(
            f'Job "{job.name}" will be rescheduled with its default interval, because it has returned an invalid result: {result}'
        )
        return now + job.interval

    def _limit_past_runs_inplace(self, runs: list[Run]) -> None:
        """
        Cuts down the list of past runs to the configured maximum.
        """

        # Prepare a threshold value
        drop_older_than = datetime.now(timezone.utc) - self._keep_past_runs_for

        # Filter out old runs
        #
        # Skip the most recent runs, as they are supposed to be kept. This
        # assumes that the list is sorted from oldest to newest.
        ii = 0
        while ii < len(runs) - self._keep_past_n_runs:
            run = runs[ii]

            # Too young?
            if run.started_at > drop_older_than:
                ii += 1
                continue

            # Drop it
            del runs[ii]

    async def _job_worker(
        self,
        job: ScheduledJob,
    ) -> None:
        """
        Wrapper that handles the safe running of a job.
        """
        assert isinstance(job._next_run_at, datetime), job._next_run_at

        while True:
            # Wait until it's time to run the job
            await self._wait_until(job._next_run_at)

            _logger.debug(f'Running job "{job.name}"')

            # Run it, taking care not to crash
            started_at = time.monotonic()

            run = Run(
                started_at=datetime.now(timezone.utc),
                finished_at=None,
                log_messages=[],
                result=None,
            )
            job.past_runs.append(run)

            try:
                result = await _call_sync_or_async_function(job.callback)

            except asyncio.CancelledError as err:
                job._next_run_at = "never"
                run.result = err

                _logger.debug(
                    f'Job "{job.name}" has been unscheduled due to cancellation. (Raised `asyncio.CancelledError`)'
                )
                return

            except Exception as err:
                _logger.exception(f'Job "{job.name}" has crashed. Rescheduling.')
                run.result = err
                result = None
            else:
                run.result = result

            # How long did the run take?
            finished_at = time.monotonic()
            run_duration = timedelta(seconds=finished_at - started_at)
            _logger.debug(f'Job "{job.name}" has completed in {run_duration}.')

            # Update the run
            run.finished_at = run.started_at + run_duration

            # Don't let the list of past runs get too long
            self._limit_past_runs_inplace(job.past_runs)

            # When should it run next?
            now = datetime.now(timezone.utc)
            job._next_run_at = self._get_next_run_time(job, result)

            # Killjoy?
            if job._next_run_at == "never":
                _logger.debug(
                    f'Job "{job.name}" has been unscheduled due to returning "never".'
                )

                return

            if job._next_run_at < now:
                _logger.debug(
                    f'Job "{job.name}" has returned a time in the past. It will be scheduled to run again as soon as possible.'
                )
            else:
                _logger.debug(
                    f'Job "{job.name}" has been scheduled to run at {job._next_run_at}.'
                )
