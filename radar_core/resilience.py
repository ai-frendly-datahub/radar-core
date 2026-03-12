from __future__ import annotations

import threading

import structlog
from pybreaker import (
    CircuitBreaker,
    CircuitBreakerListener as PyBreakerCircuitBreakerListener,
)

logger = structlog.get_logger(__name__)


class CircuitBreakerListener(PyBreakerCircuitBreakerListener):
    def state_change(
        self, cb: CircuitBreaker, old_state: object, new_state: object
    ) -> None:
        logger.info(
            "circuit_breaker_state_change",
            source=cb.name,
            before=old_state,
            after=new_state,
        )

    def before_call(
        self, cb: CircuitBreaker, func: object, *args: object, **kwargs: object
    ) -> None:
        _ = (cb, func, args, kwargs)

    def failure(self, cb: CircuitBreaker, exc: BaseException) -> None:
        logger.warning(
            "circuit_breaker_failure",
            source=cb.name,
            exception=type(exc).__name__,
            message=str(exc),
        )

    def success(self, cb: CircuitBreaker) -> None:
        logger.debug("circuit_breaker_success", source=cb.name)


class CircuitBreakerManager:
    def __init__(self) -> None:
        self._instances: dict[str, CircuitBreaker] = {}
        self._lock: threading.RLock = threading.RLock()
        self._listener: CircuitBreakerListener = CircuitBreakerListener()

    def get_breaker(self, source_name: str) -> CircuitBreaker:
        if source_name in self._instances:
            return self._instances[source_name]

        with self._lock:
            if source_name in self._instances:
                return self._instances[source_name]

            breaker = CircuitBreaker(
                fail_max=5,
                reset_timeout=60,
                success_threshold=2,
                listeners=[self._listener],
                name=source_name,
                exclude=[ValueError, KeyError, AttributeError],
            )
            self._instances[source_name] = breaker
            return breaker

    def reset_breaker(self, source_name: str) -> None:
        with self._lock:
            if source_name in self._instances:
                reset_fn = getattr(self._instances[source_name], "reset", None)
                if callable(reset_fn):
                    _ = reset_fn()
                logger.info("circuit_breaker_reset", source=source_name)

    def reset_all(self) -> None:
        with self._lock:
            for breaker in self._instances.values():
                reset_fn = getattr(breaker, "reset", None)
                if callable(reset_fn):
                    _ = reset_fn()
            logger.info("circuit_breaker_reset_all", count=len(self._instances))

    def get_status(self) -> dict[str, str]:
        with self._lock:
            return {
                name: str(breaker.state) for name, breaker in self._instances.items()
            }


SourceCircuitBreakerManager = CircuitBreakerManager

_manager: CircuitBreakerManager | None = None
_manager_lock = threading.Lock()


def get_circuit_breaker_manager() -> CircuitBreakerManager:
    global _manager
    if _manager is None:
        with _manager_lock:
            if _manager is None:
                _manager = CircuitBreakerManager()
    return _manager
