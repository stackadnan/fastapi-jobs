# Contributing to fastapi-jobs

Thanks for taking the time to contribute.

## Before you start

For anything beyond a small fix, open an issue first to discuss the change. This
project deliberately stays small (see the Project Goals section of the README) --
some features that seem reasonable in isolation (a new backend, a dashboard, a
scheduler) are intentionally out of scope for now, and it's better to align before
you put work into a PR.

## Development setup

```bash
git clone https://github.com/stackadnan/fastapi-jobs.git
cd fastapi-jobs
pip install -e ".[dev]"
```

Run the checks CI runs:

```bash
pytest
ruff check .
ruff format --check .
mypy fastapi_jobs
```

All four must pass before a PR can be merged.

## Code style

* Type hints everywhere; `mypy --strict` is enforced in CI.
* Formatting and linting are handled by Ruff (`ruff format` / `ruff check`), not by
  hand -- run `ruff format .` before committing.
* Prefer modifying an existing function over adding a new abstraction layer. This
  codebase favors a few extra lines of straightforward code over a generic helper
  used once.
* Comments explain *why*, not *what* -- skip a comment if the code already makes the
  "what" obvious.

## Tests

Tests live under three directories, and where a test goes says something about what
it's proving:

* `tests/unit/` -- pure functions with no backend or event loop involved (e.g.
  backoff calculation).
* `tests/integration/` -- a `JobManager`/`Worker`/REST API exercised end-to-end
  against a real (temp-file) SQLite backend.
* `tests/concurrency/` -- races and invariants that only show up with multiple
  claims, workers, or jobs in flight at once.

When testing concurrent or cooperative behavior (e.g. proving two jobs run at the
same time, or that a cancellation actually interrupts a running task), use
`asyncio.Event` to synchronize rather than `asyncio.sleep` guesses -- see
`tests/concurrency/test_worker_concurrency.py` for the pattern. Sleeps in tests
should only ever assert something did *not* happen within a bounded window, never
used to prove ordering.

## Commit messages and PRs

Commit messages in this repo follow `type(scope): summary` (see `git log` for
examples: `feat(worker): ...`, `fix(backend): ...`, `docs: ...`, `ci: ...`). Keep the
subject line under ~70 characters and explain *why* in the body if the change isn't
self-evident from the diff.

If your change is user-facing, add an entry to the `[Unreleased]` section of
`CHANGELOG.md` (see the existing `[0.2.0]` entry for the expected level of detail).

Open the PR against `main`. CI runs the same checks listed above automatically.

## Reporting bugs

Open a GitHub issue with what you expected, what happened instead, and a minimal
reproduction if you can put one together. For security vulnerabilities, see
[SECURITY.md](SECURITY.md) instead of opening a public issue.
