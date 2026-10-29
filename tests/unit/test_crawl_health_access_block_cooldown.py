from datetime import datetime, timedelta, timezone

import pytest

from radar_core.crawl_health import CrawlHealthStore

WEEK = 7 * 86400


def disable(store, error, age_days):
    store.record_failure("feed", error, 1.0)
    store.flush()
    store.conn.execute(
        "UPDATE crawl_health SET updated_at = ? WHERE source_name = 'feed'",
        [datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=age_days)],
    )


ERR403 = "403 Client Error: Forbidden for url: https://example.org/feed"


def test_403_stays_closed_by_default(tmp_path):
    with CrawlHealthStore(str(tmp_path / "h.db"), failure_threshold=1) as store:
        disable(store, ERR403, 30)
        assert not store.claim_retry("feed")
        assert not store.claim_retry("feed", WEEK)
        assert store.is_disabled("feed")


def test_403_opt_in_is_claimed_once_per_week(tmp_path):
    with CrawlHealthStore(str(tmp_path / "h.db"), failure_threshold=1) as store:
        disable(store, ERR403, 8)
        assert store.claim_retry("feed", WEEK, retry_statuses=[403])
        assert not store.claim_retry("feed", WEEK, retry_statuses=[403])
        record = store.get_health("feed")
        assert record.disabled and record.failure_count == 1


def test_403_opt_in_waits_for_full_week(tmp_path):
    with CrawlHealthStore(str(tmp_path / "h.db"), failure_threshold=1) as store:
        disable(store, ERR403, 2)
        assert not store.claim_retry("feed", WEEK, retry_statuses=[403])


def test_403_opt_in_needs_week_long_cooldown(tmp_path):
    with CrawlHealthStore(str(tmp_path / "h.db"), failure_threshold=1) as store:
        with pytest.raises(ValueError):
            store.claim_retry("feed", 86400, retry_statuses=[403])


@pytest.mark.parametrize("status", [400, 401, 404, 410, 451, 200])
def test_only_403_can_be_added(tmp_path, status):
    with CrawlHealthStore(str(tmp_path / "h.db"), failure_threshold=1) as store:
        with pytest.raises(ValueError):
            store.claim_retry("feed", WEEK, retry_statuses=[status])


@pytest.mark.parametrize(
    "error",
    [
        "404 Client Error: Not Found for url: https://example.org/feed",
        "410 Client Error: Gone for url: https://example.org/feed",
        "401 Client Error: Unauthorized for url: https://example.org/feed",
        "robots blocked 403",
        "unknown",
    ],
)
def test_permanent_errors_stay_disabled_even_with_403_opt_in(tmp_path, error):
    with CrawlHealthStore(str(tmp_path / "h.db"), failure_threshold=1) as store:
        disable(store, error, 30)
        assert not store.claim_retry("feed", WEEK, retry_statuses=[403])
        assert store.is_disabled("feed")


def test_transient_status_still_claimed_with_403_opt_in(tmp_path):
    with CrawlHealthStore(str(tmp_path / "h.db"), failure_threshold=1) as store:
        disable(store, "503 Server Error: unavailable for url: https://example.org", 8)
        assert store.claim_retry("feed", WEEK, retry_statuses=[403])


def test_403_recovery_reenables_and_keeps_history(tmp_path):
    with CrawlHealthStore(str(tmp_path / "h.db"), failure_threshold=1) as store:
        disable(store, ERR403, 8)
        assert store.claim_retry("feed", WEEK, retry_statuses=[403])
        store.finish_retry("feed")
        assert not store.is_disabled("feed")
        assert store.get_health("feed").failure_count == 1
        store.record_failure("feed", ERR403, 1.0)
        assert store.is_disabled("feed")
