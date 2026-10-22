from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from radar_core.crawl_health import CrawlHealthStore


def disable(store, error="503 Server Error: unavailable for url: https://example.org"):
    store.record_failure("feed", error, 1.0)
    store.flush()
    store.conn.execute(
        "UPDATE crawl_health SET updated_at = ? WHERE source_name = 'feed'",
        [datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=2)],
    )


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_cooldown_claim_keeps_disabled_and_history(tmp_path, status):
    with CrawlHealthStore(str(tmp_path / "health.db"), failure_threshold=1) as store:
        disable(store, f"{status} Server Error: outage for url: https://example.org")
        assert store.is_disabled("feed")
        assert store.claim_retry("feed")
        assert not store.claim_retry("feed")
        record = store.get_health("feed")
        assert record.disabled
        assert record.failure_count == 1
        assert record.last_error.startswith(str(status))


@pytest.mark.parametrize(
    "error",
    [
        "403 Client Error: forbidden for url: https://example.org",
        "401 Client Error: unauthorized",
        "404 Client Error: missing",
        "timeout",
        "robots blocked 503",
        "unknown",
        "",
    ],
)
def test_unknown_and_permanent_failures_stay_disabled(tmp_path, error):
    with CrawlHealthStore(str(tmp_path / "health.db"), failure_threshold=1) as store:
        disable(store, error)
        assert not store.claim_retry("feed")
        assert store.is_disabled("feed")


def test_not_due_missing_and_enabled_are_not_claimed(tmp_path):
    with CrawlHealthStore(str(tmp_path / "health.db"), failure_threshold=1) as store:
        assert not store.claim_retry("absent")
        store.record_failure("feed", "429 Client Error: limited", 1.0)
        assert not store.claim_retry("feed")
        store.conn.execute("UPDATE crawl_health SET updated_at = NULL")
        assert not store.claim_retry("feed")
        disable(store)
        store.conn.execute("UPDATE crawl_health SET disabled = FALSE")
        assert not store.claim_retry("feed")


def test_claim_survives_reopen_and_competing_threads(tmp_path):
    path = str(tmp_path / "health.db")
    with CrawlHealthStore(path, failure_threshold=1) as store:
        disable(store)
        with ThreadPoolExecutor(max_workers=8) as pool:
            assert sum(pool.map(lambda _: store.claim_retry("feed"), range(8))) == 1
    with CrawlHealthStore(path) as store:
        assert not store.claim_retry("feed")
        assert store.is_disabled("feed")


def test_valid_recovery_preserves_history_and_next_failure_disables(tmp_path):
    with CrawlHealthStore(str(tmp_path / "health.db"), failure_threshold=1) as store:
        disable(store)
        assert store.claim_retry("feed")
        # An HTTP success alone must not restore an empty or unparsable feed.
        store.record_success("feed", 0.5)
        assert store.is_disabled("feed")
        store.finish_retry("feed")
        for _ in range(2):
            store.record_success("feed", 0.5)
            assert not store.is_disabled("feed")
        record = store.get_health("feed")
        assert record.failure_count == 1
        assert record.success_count == 3
        store.record_failure("feed", "403 Client Error: forbidden", 0.5)
        assert store.is_disabled("feed")
        assert store.get_health("feed").failure_count == 2


def test_retry_after_defers_claim_across_restart(tmp_path):
    path = str(tmp_path / "health.db")
    future = datetime.now(timezone.utc) + timedelta(days=7)
    with CrawlHealthStore(path, failure_threshold=1) as store:
        disable(store)
        store.postpone_retry("feed", future)
        assert not store.claim_retry("feed")
    with CrawlHealthStore(path) as store:
        assert not store.claim_retry("feed")


@pytest.mark.parametrize("seconds", [0, -1, 3600, float("inf"), float("nan")])
def test_invalid_cooldown_rejected(tmp_path, seconds):
    with CrawlHealthStore(str(tmp_path / "health.db")) as store:
        with pytest.raises(ValueError):
            store.claim_retry("feed", seconds)
