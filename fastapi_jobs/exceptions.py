from __future__ import annotations


class FastAPIJobsError(Exception):
    """Base class for every error raised by fastapi-jobs."""


class JobNotFoundError(FastAPIJobsError):
    def __init__(self, job_id: str) -> None:
        super().__init__(f"no job found with id '{job_id}'")
        self.job_id = job_id


class InvalidJobStateError(FastAPIJobsError):
    """The job exists but isn't in a state that allows the requested operation."""


class JobAlreadyCompletedError(InvalidJobStateError):
    """Raised when trying to cancel a job that already reached a terminal state."""


class JobSerializationError(FastAPIJobsError):
    """A task's arguments or return value couldn't be encoded as JSON.

    fastapi-jobs only supports JSON-serializable arguments and results. Pickling job
    data would let anything with write access to the job store execute arbitrary code
    when a worker deserializes it, which isn't a risk this library takes on.
    """


class JobTimeoutError(FastAPIJobsError):
    """Raised internally when a task exceeds its configured timeout."""


class JobsNotConfiguredError(FastAPIJobsError):
    """enqueue() was called before a Jobs instance had been constructed in this process."""
