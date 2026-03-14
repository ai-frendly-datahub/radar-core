from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
import random
import threading
import time


@dataclass
class SourceThrottleState:
    current_delay: float
    last_request_time: float | None
    consecutive_successes: int
    consecutive_failures: int


class AdaptiveThrottler:
    def __init__(
        self,
        min_delay: float = 0.5,
        max_delay: float = 60.0,
        success_reduction_factor: float = 0.9,
        failure_increase_factor: float = 2.0,
        jitter_factor: float = 0.25,
    ):
        if min_delay <= 0:
            raise ValueError("min_delay must be greater than 0")
        if max_delay < min_delay:
            raise ValueError("max_delay must be greater than or equal to min_delay")
        if not 0.0 <= success_reduction_factor <= 1.0:
            raise ValueError("success_reduction_factor must be between 0 and 1")
        if failure_increase_factor <= 1.0:
            raise ValueError("failure_increase_factor must be greater than 1")
        if not 0.0 <= jitter_factor <= 1.0:
            raise ValueError("jitter_factor must be between 0 and 1")

        self.min_delay = min_delay
        self.max_delay = max_delay
        self.success_reduction_factor = success_reduction_factor
        self.failure_increase_factor = failure_increase_factor
        self.jitter_factor = jitter_factor

        self._success_decrease_step = self.min_delay * (
            1.0 - self.success_reduction_factor
        )
        self._states: dict[str, SourceThrottleState] = {}
        self._lock = threading.Lock()

    def acquire(self, source_name: str) -> float:
        with self._lock:
            state = self._get_or_create_state(source_name)

            now = time.monotonic()
            requested_delay = self._apply_jitter(state.current_delay)

            if state.last_request_time is None:
                sleep_for = 0.0
            else:
                elapsed = now - state.last_request_time
                sleep_for = max(0.0, requested_delay - elapsed)

            state.last_request_time = now + sleep_for

        if sleep_for > 0.0:
            time.sleep(sleep_for)

        return sleep_for

    def record_success(self, source_name: str) -> None:
        with self._lock:
            state = self._get_or_create_state(source_name)
            state.consecutive_successes += 1
            state.consecutive_failures = 0

            state.current_delay = max(
                self.min_delay,
                state.current_delay - self._success_decrease_step,
            )

    def record_failure(
        self, source_name: str, retry_after: int | str | None = None
    ) -> None:
        with self._lock:
            state = self._get_or_create_state(source_name)
            state.consecutive_failures += 1
            state.consecutive_successes = 0

            next_delay = min(
                self.max_delay,
                state.current_delay * self.failure_increase_factor,
            )

            if retry_after is not None:
                retry_after_delay = self._parse_retry_after(retry_after)
                next_delay = min(self.max_delay, max(next_delay, retry_after_delay))

            state.current_delay = max(self.min_delay, next_delay)

    def record_rate_limit(
        self, source_name: str, retry_after: int | str | None
    ) -> None:
        self.record_failure(source_name=source_name, retry_after=retry_after)

    def get_current_delay(self, source_name: str) -> float:
        with self._lock:
            state = self._get_or_create_state(source_name)
            return state.current_delay

    def _get_or_create_state(self, source_name: str) -> SourceThrottleState:
        state = self._states.get(source_name)
        if state is not None:
            return state

        state = SourceThrottleState(
            current_delay=self.min_delay,
            last_request_time=None,
            consecutive_successes=0,
            consecutive_failures=0,
        )
        self._states[source_name] = state
        return state

    def _parse_retry_after(self, retry_after: int | str) -> float:
        if isinstance(retry_after, int):
            return max(self.min_delay, min(float(retry_after), self.max_delay))

        stripped = retry_after.strip()
        if stripped.isdigit():
            return max(self.min_delay, min(float(stripped), self.max_delay))

        parsed_dt = parsedate_to_datetime(stripped)
        if parsed_dt is None:
            return self.max_delay

        if parsed_dt.tzinfo is None:
            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)

        seconds_until_retry = parsed_dt.timestamp() - time.time()
        bounded = max(0.0, seconds_until_retry)
        return max(self.min_delay, min(bounded, self.max_delay))

    def _apply_jitter(self, delay: float) -> float:
        if self.jitter_factor == 0.0:
            return max(self.min_delay, min(delay, self.max_delay))

        min_multiplier = max(0.0, 1.0 - self.jitter_factor)
        max_multiplier = 1.0 + self.jitter_factor
        jittered_delay = delay * random.uniform(min_multiplier, max_multiplier)
        return max(self.min_delay, min(jittered_delay, self.max_delay))
