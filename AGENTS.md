# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`rio-jobs` is a job scheduler extension for [Rio](https://rio.dev), a pure Python web framework. It allows scheduling Python functions to run periodically with automatic exception handling and rescheduling. The extension is currently experimental and requires Rio version 0.11 or later.

Key characteristics:
- Jobs start when the app starts and stop when it stops
- Supports both synchronous and asynchronous job functions
- Jobs can run at regular intervals or dynamically reschedule themselves
- Exception-safe: jobs are automatically rescheduled even if they crash
- Supports initial delays and soft starts to avoid load spikes

## Development Commands

**Development environment uses `uv` for dependency management.**

### Running the development app
```bash
rio run
```
This runs the development application defined in `devel.py` (configured in `rio.toml`).

### Linting and formatting
```bash
# Install pre-commit hooks
pre-commit install

# Run manually
pre-commit run --all-files
```
The project uses Ruff for linting and formatting (configured in `.pre-commit-config.yaml`):
- Enforces import sorting
- Removes unused imports (F401)
- Auto-formats code

### Building the package
```bash
# Build with hatch (the project uses hatchling as build backend)
python -m build
```

## Architecture

### Core Components

**JobScheduler (rio_jobs/__init__.py)**
- The main extension class that inherits from `rio.Extension`
- Manages the lifecycle of all scheduled jobs
- Handles job registration via `@scheduler.job` decorator or `add_job()` method
- Listens to Rio extension events: `on_app_start` and `on_app_close`
- Stores jobs in `_job_objects: list[ScheduledJob]`
- Implements soft start logic to stagger job execution at app startup (30 second intervals)

**Run (rio_jobs/__init__.py:96)**
- Passed to each job function as the only argument
- Provides context about the current job execution:
  - `started_at`: When the job was started (timezone-aware)
  - `app`: Reference to the Rio app instance
  - `status_message`: Can be set by the job to report current activity
  - `progress`: Optional float (0-1) for progress indication
- Tracks execution history with `_finished_at`, `_result`, and `_log_messages`

**ScheduledJob (rio_jobs/__init__.py:270)**
- Internal dataclass representing a scheduled job
- Stores job metadata: callback, interval, name, configuration flags
- Tracks `past_runs` list (oldest first) with automatic cleanup based on `keep_past_runs_for` and `keep_past_n_runs`
- Manages asyncio task lifecycle via `_task` field
- Stores `_next_run_at` as either a `datetime` or `"never"`

### Job Execution Flow

1. **Registration**: Jobs are added via `@scheduler.job` decorator or `add_job()`
2. **App Start**: When Rio app starts, `_on_app_start()` calculates initial run times
3. **Soft Start**: Jobs with `soft_start=True` are staggered by 30 seconds each, sorted by their desired start time
4. **Task Creation**: Each job gets an asyncio task via `_create_asyncio_task_for_job()`
5. **Job Worker**: `_job_worker()` runs in a loop:
   - Waits until scheduled time using `_wait_until()`
   - Creates a `Run` object and appends to `past_runs`
   - Executes job function (sync or async) via `_call_sync_or_async_function()`
   - Catches exceptions (except `CancelledError`) and logs them
   - Determines next run time based on job's return value
   - Cleans up old runs via `_limit_past_runs_inplace()`
6. **Rescheduling**: Jobs can return:
   - `None`: Run again after default interval
   - `datetime`: Run at specific time
   - `timedelta`: Wait for duration
   - `"never"`: Stop the job permanently

### UI Components (rio_jobs/components.py)

**JobsView**
- Displays all scheduled jobs in a list
- Shows running jobs with progress indicators
- Offers "Run Now" button for scheduled jobs
- Refreshes every 10 seconds via `@rio.event.periodic(10)`
- Can manually trigger jobs by canceling and recreating their task

**RunsView**
- Shows history of up to 50 most recent job runs
- Displays success/failure status with icons
- Shows run duration and start times
- Auto-refreshes every 10 seconds

Both components work with the private `_job_objects` and job state since there's no public API for monitoring yet.

### Extension Integration

Since Rio's extension interface is experimental, the scheduler is added manually:
```python
app = rio.App(build=MyRoot)
app._add_extension(scheduler)
```

The scheduler hooks into Rio's extension events:
- `@rio.extension_event.on_app_start`: Starts all jobs
- `@rio.extension_event.on_app_close`: Cancels all job tasks and waits for cleanup

## Important Implementation Notes

- Jobs are non-overlapping: next run waits for current run to complete plus interval
- All datetime operations use UTC timezone internally
- `_wait_until()` sleeps in max 1-hour chunks to handle clock changes gracefully
- Job return values override the default interval only for the next run
- Invalid return values are logged as warnings and the job continues with default interval
- Jobs that return times in the past are scheduled to run immediately
- The `Run.app` reference allows jobs to access app-level state and other extensions
