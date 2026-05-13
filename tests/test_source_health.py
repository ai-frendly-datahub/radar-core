from __future__ import annotations

from datetime import UTC, datetime, timedelta

from radar_core.source_health import (
    SourceReliability,
    compute_source_reliability,
    reliability_to_dict,
)


def _rows() -> list[dict[str, object]]:
    today = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
    return [
        # Reliable source — 9 success, 1 timeout, last seen today.
        *[
            {"source": "A", "fetch_status": "ok", "collected_at": today - timedelta(hours=i)}
            for i in range(9)
        ],
        {"source": "A", "fetch_status": "timeout", "collected_at": today - timedelta(hours=3)},
        # Stale source — last seen 20 days ago.
        {"source": "B", "fetch_status": "ok", "collected_at": today - timedelta(days=20)},
        # Failure-heavy source — 1 success, 4 errors, fresh.
        {"source": "C", "fetch_status": "ok", "collected_at": today - timedelta(hours=1)},
        *[
            {"source": "C", "fetch_status": "error", "collected_at": today - timedelta(hours=i)}
            for i in range(4)
        ],
        # Row with missing source — ignored.
        {"source": None, "fetch_status": "ok", "collected_at": today},
    ]


def test_compute_source_reliability_basic() -> None:
    rows = _rows()
    today = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
    records = compute_source_reliability(rows, today=today)
    by_src = {r.source: r for r in records}

    assert set(by_src) == {"A", "B", "C"}

    a = by_src["A"]
    assert a.article_count == 9
    assert a.success_rate == 0.9
    assert a.last_seen is not None
    assert a.score > 80

    b = by_src["B"]
    assert b.staleness_days == 20.0
    # recency component is 0 → score = 70 * success_rate(1.0) = 70
    assert b.score == 70.0

    c = by_src["C"]
    assert c.success_rate == 0.2
    assert c.score < 50


def test_compute_source_reliability_sorted_desc() -> None:
    rows = _rows()
    today = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
    records = compute_source_reliability(rows, today=today)
    scores = [r.score for r in records]
    assert scores == sorted(scores, reverse=True)


def test_reliability_to_dict_round_trip() -> None:
    record = SourceReliability(
        source="X",
        article_count=10,
        success_rate=0.95,
        last_seen=datetime(2026, 5, 13, tzinfo=UTC),
        staleness_days=1.0,
        score=92.5,
    )
    payload = reliability_to_dict([record])
    assert payload == [
        {
            "source": "X",
            "article_count": 10,
            "success_rate": 0.95,
            "last_seen": "2026-05-13T00:00:00+00:00",
            "staleness_days": 1.0,
            "score": 92.5,
        }
    ]


def test_reliability_handles_iso_strings() -> None:
    today = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
    rows = [
        {"source": "S", "fetch_status": "ok", "collected_at": "2026-05-13T11:00:00+00:00"},
        {"source": "S", "fetch_status": "ok", "collected_at": "2026-05-13T10:00:00"},
    ]
    records = compute_source_reliability(rows, today=today)
    assert records[0].source == "S"
    assert records[0].article_count == 2
