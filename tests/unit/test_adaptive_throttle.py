from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import patch

import pytest

from radar_core.adaptive_throttle import AdaptiveThrottler


def test_acquire_applies_delay() -> None:
    throttler = AdaptiveThrottler(min_delay=0.2, max_delay=5.0, jitter_factor=0.0)

    with (
        patch("radar_core.adaptive_throttle.time.monotonic", side_effect=[10.0, 10.05]),
        patch("radar_core.adaptive_throttle.time.sleep") as mock_sleep,
    ):
        first_sleep = throttler.acquire("source-a")
        second_sleep = throttler.acquire("source-a")

    assert first_sleep == 0.0
    assert second_sleep == pytest.approx(0.15)
    mock_sleep.assert_called_once_with(pytest.approx(0.15))


def test_record_success_reduces_delay() -> None:
    throttler = AdaptiveThrottler(
        min_delay=0.5,
        max_delay=5.0,
        success_reduction_factor=0.8,
        failure_increase_factor=2.0,
        jitter_factor=0.0,
    )

    throttler.record_failure("source-a")
    before = throttler.get_current_delay("source-a")
    throttler.record_success("source-a")
    after = throttler.get_current_delay("source-a")

    assert before == 1.0
    assert after == pytest.approx(0.9)


def test_record_failure_increases_delay() -> None:
    throttler = AdaptiveThrottler(min_delay=0.5, max_delay=5.0, jitter_factor=0.0)

    throttler.record_failure("source-a")

    assert throttler.get_current_delay("source-a") == 1.0


def test_jitter_applied_to_delay() -> None:
    throttler = AdaptiveThrottler(min_delay=1.0, max_delay=5.0, jitter_factor=0.25)

    with (
        patch("radar_core.adaptive_throttle.random.uniform", return_value=1.25),
        patch("radar_core.adaptive_throttle.time.monotonic", side_effect=[1.0, 1.2]),
        patch("radar_core.adaptive_throttle.time.sleep") as mock_sleep,
    ):
        _ = throttler.acquire("source-a")
        sleep_for = throttler.acquire("source-a")

    assert sleep_for == pytest.approx(1.05)
    mock_sleep.assert_called_once_with(pytest.approx(1.05))


def test_retry_after_integer_parsing() -> None:
    throttler = AdaptiveThrottler(min_delay=0.5, max_delay=60.0, jitter_factor=0.0)

    throttler.record_failure("source-a", retry_after="12")

    assert throttler.get_current_delay("source-a") == 12.0


def test_retry_after_http_date_parsing() -> None:
    throttler = AdaptiveThrottler(min_delay=0.5, max_delay=60.0, jitter_factor=0.0)
    now_dt = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    retry_dt = now_dt + timedelta(seconds=30)
    retry_after = format_datetime(retry_dt)

    with patch(
        "radar_core.adaptive_throttle.time.time", return_value=now_dt.timestamp()
    ):
        throttler.record_failure("source-a", retry_after=retry_after)

    assert throttler.get_current_delay("source-a") == pytest.approx(30.0, abs=0.1)


def test_per_source_state_isolation() -> None:
    throttler = AdaptiveThrottler(min_delay=0.5, max_delay=10.0, jitter_factor=0.0)

    throttler.record_failure("source-a")
    throttler.record_failure("source-a")
    throttler.record_success("source-b")

    assert throttler.get_current_delay("source-a") == 2.0
    assert throttler.get_current_delay("source-b") == 0.5


def test_delay_capped_at_max() -> None:
    throttler = AdaptiveThrottler(min_delay=0.5, max_delay=3.0, jitter_factor=0.0)

    for _ in range(10):
        throttler.record_failure("source-a")

    assert throttler.get_current_delay("source-a") == 3.0


def test_delay_floored_at_min() -> None:
    throttler = AdaptiveThrottler(
        min_delay=0.5,
        max_delay=10.0,
        success_reduction_factor=0.0,
        jitter_factor=0.0,
    )

    for _ in range(4):
        throttler.record_failure("source-a")

    for _ in range(20):
        throttler.record_success("source-a")

    assert throttler.get_current_delay("source-a") == 0.5
