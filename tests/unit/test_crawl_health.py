from __future__ import annotations

import threading
from pathlib import Path

import pytest

from radar_core.crawl_health import CrawlHealthStore


def test_record_success_increments_count(tmp_path: Path) -> None:
    store = CrawlHealthStore(str(tmp_path / "health.duckdb"), batch_size=10)
    try:
        store.record_success("source-a", delay=0.75)
        record = store.get_health("source-a")
    finally:
        store.close()

    assert record is not None
    assert record.success_count == 1
    assert record.failure_count == 0
    assert record.current_delay == 0.75
    assert record.last_error is None
    assert record.disabled is False


def test_record_failure_increments_count(tmp_path: Path) -> None:
    store = CrawlHealthStore(str(tmp_path / "health.duckdb"), batch_size=10)
    try:
        store.record_failure("source-a", error="timeout", delay=1.25)
        record = store.get_health("source-a")
    finally:
        store.close()

    assert record is not None
    assert record.success_count == 0
    assert record.failure_count == 1
    assert record.current_delay == 1.25
    assert record.last_error == "timeout"
    assert record.disabled is False


def test_disable_after_threshold(tmp_path: Path) -> None:
    store = CrawlHealthStore(
        str(tmp_path / "health.duckdb"), batch_size=10, failure_threshold=2
    )
    try:
        store.record_failure("source-a", error="error-1", delay=1.0)
        store.record_failure("source-a", error="error-2", delay=2.0)
        record = store.get_health("source-a")
    finally:
        store.close()

    assert record is not None
    assert record.failure_count == 2
    assert record.disabled is True


def test_batch_flush_on_threshold(tmp_path: Path) -> None:
    store = CrawlHealthStore(str(tmp_path / "health.duckdb"), batch_size=2)
    try:
        store.record_success("source-a", delay=0.6)
        assert len(store._buffer) == 1

        store.record_success("source-a", delay=0.7)
        assert len(store._buffer) == 0

        row = store.conn.execute(
            "SELECT success_count, current_delay FROM crawl_health WHERE source_name = ?",
            ["source-a"],
        ).fetchone()
    finally:
        store.close()

    assert row is not None
    assert row[0] == 2
    assert row[1] == pytest.approx(0.7)


def test_context_manager_flushes_on_exit(tmp_path: Path) -> None:
    db_path = tmp_path / "health.duckdb"

    with CrawlHealthStore(str(db_path), batch_size=100) as store:
        store.record_success("source-a", delay=0.5)
        store.record_failure("source-a", error="boom", delay=1.0)

    with CrawlHealthStore(str(db_path), batch_size=100) as store:
        record = store.get_health("source-a")

    assert record is not None
    assert record.success_count == 1
    assert record.failure_count == 1
    assert record.last_error == "boom"


def test_upsert_updates_existing_record(tmp_path: Path) -> None:
    store = CrawlHealthStore(str(tmp_path / "health.duckdb"), batch_size=1)
    try:
        store.record_failure("source-a", error="initial failure", delay=1.2)
        store.record_success("source-a", delay=0.8)
        record = store.get_health("source-a")
    finally:
        store.close()

    assert record is not None
    assert record.success_count == 1
    assert record.failure_count == 1
    assert record.current_delay == pytest.approx(0.8)
    assert record.last_error == "initial failure"


def test_is_disabled_returns_correct_state(tmp_path: Path) -> None:
    store = CrawlHealthStore(
        str(tmp_path / "health.duckdb"), batch_size=1, failure_threshold=1
    )
    try:
        assert store.is_disabled("missing-source") is False

        store.record_failure("source-a", error="fatal", delay=1.0)
        assert store.is_disabled("source-a") is True
    finally:
        store.close()


def test_thread_safety_concurrent_writes(tmp_path: Path) -> None:
    store = CrawlHealthStore(str(tmp_path / "health.duckdb"), batch_size=5000)
    successes_per_thread = 200
    failures_per_thread = 80
    thread_count = 16

    def worker() -> None:
        for _ in range(successes_per_thread):
            store.record_success("source-a", delay=0.4)
        for _ in range(failures_per_thread):
            store.record_failure("source-a", error="transient", delay=1.0)

    threads = [threading.Thread(target=worker) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    try:
        record = store.get_health("source-a")
    finally:
        store.close()

    assert record is not None
    assert record.success_count == thread_count * successes_per_thread
    assert record.failure_count == thread_count * failures_per_thread
    assert record.last_error == "transient"
