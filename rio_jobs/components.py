import asyncio
from datetime import timedelta, datetime, timezone
import rio
import functools
import rio_jobs


def _repr_timestamp(timestamp: datetime) -> str:
    now = datetime.now()

    time_only = timestamp.strftime("%H:%M")

    # If this was today, only show the time
    if timestamp.date() == now.date():
        return f"{time_only}"

    # Yesterday?
    if timestamp.date() == now.date() - timedelta(days=1):
        return f"yesterday at {time_only}"

    # Otherwise, show the full date
    return timestamp.strftime("%Y-%m-%d %H:%M")


def _repr_duration(duration: timedelta) -> str:
    # Work with integers
    seconds = int(duration.total_seconds())

    # Durations can often not be guaranteed to be positive due to changes to the
    # system clock. Just treat negative durations as 0.
    if seconds < 1:
        return "less than a second"

    units = (
        ("second", 60),
        ("minute", 60),
        ("hour", 24),
        ("day", None),
    )

    parts = []

    amount = seconds
    for unit_info in units:
        unit_name, unit_factor = unit_info

        if unit_factor is None:
            cur = amount
        else:
            cur = amount % unit_factor
            amount = amount // unit_factor

        if cur == 0:
            continue

        parts.append((unit_name, cur))

    chunks = []
    for unit_name, amount in reversed(parts):
        if amount == 1:
            chunks.append(f"1 {unit_name}")

        else:
            chunks.append(f"{amount} {unit_name}s")

    return " ".join(chunks)


class JobsView(rio.Component):
    """
    Displays the jobs of a scheduler.

    This component displays all components in a scheduler as a list and also
    offers the user to start jobs immediately when they aren't currently
    running.

    Note: This component has no way of knowing when exactly the state of the
        scheduler changes. It refreshes on a fixed interval to keep the UI
        current.
    """

    scheduler: rio_jobs.JobScheduler

    @rio.event.periodic(10)
    def _on_periodic(self) -> None:
        self.force_refresh()

    async def _on_run_job_now(self, job: rio_jobs.ScheduledJob) -> None:
        # The UI may not be up-to-date with the actual state of the job. If the
        # job has started in the meantime, don't do anything.
        if job.past_runs and job.past_runs[-1].is_running:
            self.force_refresh()
            return

        # Cancel the task responsible for running the job
        if job._task is not None:
            job._task.cancel()
            job._task = None

        # Create a fresh task
        job._task = self.scheduler._create_asyncio_task_for_job(
            job,
            run_at=datetime.now(timezone.utc),
        )

        # Give the task some time to start, then refresh the UI
        await asyncio.sleep(2)
        self.force_refresh()

    def build(self) -> rio.Component:
        # If no jobs are scheduled, display a placeholder
        if not self.scheduler._job_objects:
            return rio.Card(
                rio.Column(
                    rio.Icon(
                        "material/history_toggle_off",
                        fill="dim",
                        min_width=3,
                        min_height=3,
                        align_x=0.5,
                        align_y=0.5,
                    ),
                    rio.Text(
                        "No jobs are scheduled",
                        justify="center",
                        style="dim",
                    ),
                    spacing=1,
                    margin=1,
                )
            )

        # Build a UI for the runs
        runs_list = rio.ListView()

        for job in self.scheduler._job_objects:
            # If the job is running display when it started
            if job.past_runs and job.past_runs[-1].is_running:
                secondary_text = (
                    f"Running since {_repr_timestamp(job.past_runs[-1].started_at)}"
                )
                left_child = rio.ProgressCircle(min_size=3)
                right_child = None
            # Otherwise display the scheduled time and offer millennials to run
            # it right now
            else:
                if isinstance(job._next_run_at, datetime):
                    secondary_text = (
                        f"Scheduled for {_repr_timestamp(job._next_run_at)}"
                    )
                else:
                    assert job._next_run_at == "never"
                    secondary_text = (
                        "This job has been unscheduled and will not run again"
                    )

                left_child = rio.Icon(
                    "material/schedule",
                    fill="dim",
                    min_width=3,
                    min_height=3,
                )

                right_child = rio.Tooltip(
                    rio.IconButton(
                        "material/play_arrow:fill",
                        style="plain-text",
                        on_press=functools.partial(self._on_run_job_now, job),
                    ),
                    tip="Run Now",
                )

            runs_list.add(
                rio.SimpleListItem(
                    text=job.name,
                    secondary_text=secondary_text,
                    left_child=left_child,
                    right_child=right_child,
                    key=id(job),
                )
            )

        return runs_list


class RunsView(rio.Component):
    """
    Displays the run history of jobs in a scheduler.

    This component displays the most recent runs of all jobs in a scheduler,
    allowing the viewer to see their outcome.

    Note: This component has no way of knowing when exactly the state of the
        scheduler changes. It refreshes on a fixed interval to keep the UI
        current.
    """

    scheduler: rio_jobs.JobScheduler

    @rio.event.periodic(10)
    def _on_periodic(self) -> None:
        self.force_refresh()

    def build(self) -> rio.Component:
        # Prepare all runs
        runs: list[tuple[rio_jobs.ScheduledJob, rio_jobs.Run]] = []

        for job in self.scheduler._job_objects:
            for run in job.past_runs:
                runs.append((job, run))

        runs.sort(key=lambda x: x[1].started_at, reverse=True)
        runs = runs[:50]

        # If there are no runs, display a placeholder
        if not runs:
            return rio.Card(
                rio.Column(
                    rio.Icon(
                        "material/history_toggle_off",
                        fill="dim",
                        min_width=3,
                        min_height=3,
                        align_x=0.5,
                        align_y=0.5,
                    ),
                    rio.Text(
                        "No jobs have run yet",
                        justify="center",
                        style="dim",
                    ),
                    spacing=1,
                    margin=1,
                )
            )

        # Build a UI for the runs
        runs_list = rio.ListView()

        for job, run in runs:
            if run.is_running:
                main_text = f"{job.name} is currently running"
                finished_in_str = ""
                left_child = rio.ProgressCircle(min_size=3)
            elif run.has_succeeded:
                assert run.finished_at is not None
                main_text = f"{job.name} has completed successfully"
                finished_in_str = (
                    f", finished in {_repr_duration(run.finished_at - run.started_at)}"
                )
                left_child = rio.Icon(
                    "material/check",
                    fill="success",
                    min_width=3,
                    min_height=3,
                )
            else:
                assert run.finished_at is not None
                main_text = f"{job.name} has failed with {run.result!r}"
                finished_in_str = (
                    f", failed after {_repr_duration(run.finished_at - run.started_at)}"
                )
                left_child = rio.Icon(
                    "material/error",
                    fill="danger",
                    min_width=3,
                    min_height=3,
                )

            runs_list.add(
                rio.SimpleListItem(
                    text=main_text,
                    secondary_text=f"Started {_repr_timestamp(run.started_at)}{finished_in_str}",
                    left_child=left_child,
                    key=id(run),
                )
            )

        return runs_list

