from __future__ import annotations

from fastapi_jobs.models import RetryPolicy
from fastapi_jobs.worker import compute_backoff


def test_exponential_backoff_doubles_each_attempt():
    policy = RetryPolicy(backoff="exponential", base_delay=2.0, max_delay=1000.0, jitter=False)
    assert compute_backoff(1, policy) == 2.0
    assert compute_backoff(2, policy) == 4.0
    assert compute_backoff(3, policy) == 8.0


def test_exponential_backoff_caps_at_max_delay():
    policy = RetryPolicy(backoff="exponential", base_delay=10.0, max_delay=15.0, jitter=False)
    assert compute_backoff(10, policy) == 15.0


def test_fixed_backoff_ignores_attempt_number():
    policy = RetryPolicy(backoff="fixed", base_delay=5.0, jitter=False)
    assert compute_backoff(1, policy) == 5.0
    assert compute_backoff(9, policy) == 5.0


def test_jitter_stays_within_bounds():
    policy = RetryPolicy(backoff="exponential", base_delay=1.0, max_delay=100.0, jitter=True)
    for attempt in range(1, 6):
        delay = compute_backoff(attempt, policy)
        assert 0.0 <= delay <= min(1.0 * 2 ** (attempt - 1), 100.0)
