# Rio Job Scheduler 🗓️

`cron` for [Rio](https://rio.dev), with syntax you can actually remember!

This is a job scheduler / task scheduler for use with [Rio](https://rio.dev),
the **pure Python web framework.** It allows you to schedule Python functions to
run periodically, with the scheduler handling exceptions and rescheduling jobs
as needed.

> ⚠️ Rio extensions are currently experimental. You'll need Rio version 0.11 or
> later and use internal APIs to add this extension to your app.

This extension is designed to be easy to use and robust, while also allowing you
to schedule jobs with arbitrarily fancy logic.

- All jobs are **started when your app starts** and **stopped when your app
  stops**
- Jobs can run at **regular intervals or reschedule themselves**
- Supports **synchronous and asynchronous** functions
- **Handles exceptions gracefully**, ensuring jobs are rescheduled even if they
  fail
- Supports initial delay and **staggered starts** to avoid load spikes
- **Open Source & Free forever**

## Installation 🛠️

`rio-jobs` is available on [PyPI](https://pypi.org/project/rio-jobs/):

```sh
python -m pip install rio-jobs
```

## Quickstart 🚀

```python
import asyncio
from datetime import timedelta
import rio
import rio_jobs

# Regular code to create your Rio app. This one is just an example that displays
# a static text.
class MyRoot(rio.Component):
    def build(self) -> rio.Component:
        return rio.Text(
            "Hello, world!",
            justify="center",
        )


# Create a scheduler
scheduler = rio_jobs.JobScheduler()

# Create a function for the scheduler to run. This function can by synchronous
# or asynchronous. The `@scheduler.job` decorator adds the function to the
# scheduler.
@scheduler.job(
    timedelta(hours=1),
)
async def my_job(run: rio_jobs.Run) -> timedelta:
    # Do some work here
    print('Working hard!')
    await asyncio.sleep(100)

    # Optionally reschedule the job. This can return
    #
    # - a `datetime` object to schedule the job at a specific time
    # - a `timedelta` object to wait for a specific amount of time
    # - literal `"never"` to stop the job
    #
    # ... or simply return nothing to keep running the job at the configured
    # interval.
    return timedelta(hours=3)

# Pass the scheduler to the Rio app. Since Rio's extension interface isn't
# stable yet, we'll add the extension manually after the app has been created.
app = rio.App(
    build=MyRoot,
)

app._add_extension(scheduler)

# Run your Rio app as usual. If you want to use `rio run`, remove this line
app.run_in_browser()
```

In this example, a job `my_job` is scheduled to run every hour. The job pretends
to be working (who doesn't) and then reschedules itself, overriding the default
interval.

## API 📚

### `class JobScheduler`

Create a `JobScheduler` object to manage your jobs. This object takes no
parameters and can be added to your app

### `JobScheduler.job(...)`

This is a decorator that allows you to schedule a job to run periodically. It
ensures that the job will continue to be scheduled even if it fails, by catching
and logging exceptions.

**Parameters:**

- `interval`: How often to run the job. If the job returns a time or timedelta,
  that will be used for the next run instead.

- `name`: An optional name for the job, useful for debugging and logs.

- `wait_for_initial_interval`: If `True`, the job waits for the interval before
  running for the first time. If `False`, it runs immediately.

- `soft_start`: If `True`, jobs are staggered to avoid load spikes at the app's
  start.

**Raises:** `ValueError` if `interval` is less than or equal to zero.

The job can optionally return a result:

- `None`: To keep running at the configured interval
- `datetime`: Explicit time to run the job again
- `timedelta`: How long to wait before running the job again
- `"never"`: The job will be unscheduled and never run again

### `JobScheduler.add_job(...)`

A non-decorator version of `JobScheduler.job`. It works the same way as
`JobScheduler.job`, but with alternate syntax, if you prefer to avoid
decorators.

This function takes the function to schedule as the first argument, followed by
the same arguments as `JobScheduler.job`.

### `class Run`

This object is passed to the job function as the first and only argument. It
contains information about this particular run of the job, such as the time it
started, the jobs progress and similar.

**Attributes:**

- `started_at: datetime.datetime`: When the job was started. Always has a
  timezone set.

- `status_message: str`: You can set this to an arbitrary message to report to
  the outside world. This can be used e.g. by a debugger or admin interface to
  show what the job is currently doing.

- `progress: float | None`: A number between 0 and 1 indicating the progress of
  the job. If `None`, the progress is indeterminate. This can be used to show a
  progress bar in e.g. a an admin interface.
