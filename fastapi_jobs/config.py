from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class JobsConfig:
    database: str = "sqlite:///jobs.db"

    # Applied to a task when it doesn't set its own `timeout=`. Every job needs a
    # bound -- it's what lets a crashed worker's lease expire and be reclaimed.
    default_timeout: float = 300.0

    # A worker's lease on a claimed job lasts `timeout_seconds + lease_buffer`. The
    # buffer covers scheduling jitter and clock drift between workers so a job in
    # flight isn't reclaimed out from under a worker that's still within its timeout.
    lease_buffer: float = 30.0

    poll_interval: float = 1.0
