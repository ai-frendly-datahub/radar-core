"""Published-date parsing. The KST crawler bug is time.mktime on a UTC struct."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from radar_core.collector import _extract_datetime
from radar_core.dates import (
    DAILY_PARTITION_TIMEZONE,
    daily_partition_date,
    parse_feed_entry,
    parse_html_published,
    parse_published,
    resolve_default_timezone,
    to_utc_iso,
)
from radar_core.models import Article, Source
from radar_core.storage import RadarStorage

_UTC_STRUCT = time.struct_time((2026, 10, 29, 0, 0, 0, 3, 302, 0))
_GMT = "Thu, 29 Oct 2026 00:00:00 GMT"
_PLUS_0900 = "Thu, 29 Oct 2026 09:00:00 +0900"
_KST = timezone(timedelta(hours=9))


def _seoul(monkeypatch) -> None:
    monkeypatch.setenv("TZ", "Asia/Seoul")
    time.tzset()


def _restore_tz(monkeypatch) -> None:
    monkeypatch.undo()
    time.tzset()


def test_gmt_rss_stays_utc_when_local_zone_is_kst(monkeypatch) -> None:
    _seoul(monkeypatch)
    try:
        wrong = datetime.fromtimestamp(time.mktime(_UTC_STRUCT), tz=UTC)
        assert wrong == datetime(2026, 10, 28, 15, 0, tzinfo=UTC)

        parsed, raw = parse_feed_entry(
            {"published": _GMT, "published_parsed": _UTC_STRUCT}
        )
        assert raw == _GMT
        assert parsed == datetime(2026, 10, 29, 0, 0, tzinfo=UTC)
        assert _extract_datetime(
            {"published": _GMT, "published_parsed": _UTC_STRUCT}
        ) == datetime(2026, 10, 29, 0, 0, tzinfo=UTC)
    finally:
        _restore_tz(monkeypatch)


def test_plus_0900_keeps_its_offset(monkeypatch) -> None:
    _seoul(monkeypatch)
    try:
        parsed, raw = parse_feed_entry({"published": _PLUS_0900})
        assert raw == _PLUS_0900
        assert parsed is not None
        assert parsed.utcoffset() == timedelta(hours=9)
        assert parsed.astimezone(UTC) == datetime(2026, 10, 29, 0, 0, tzinfo=UTC)
    finally:
        _restore_tz(monkeypatch)


def test_zoneless_korean_date_uses_seoul_for_kr_host() -> None:
    source = Source(
        name="인벤",
        type="rss",
        url="https://www.inven.co.kr/rss/news.xml",
    )
    assert resolve_default_timezone(source_url=source.url) == "Asia/Seoul"
    parsed, raw = parse_feed_entry(
        {"published": "2026-10-29 18:00:00", "published_parsed": _UTC_STRUCT},
        default_timezone=resolve_default_timezone(source_url=source.url),
    )
    assert raw == "2026-10-29 18:00:00"
    assert parsed == datetime(2026, 10, 29, 18, 0, tzinfo=_KST)
    assert parsed.astimezone(UTC) == datetime(2026, 10, 29, 9, 0, tzinfo=UTC)


def test_explicit_default_timezone_overrides_host() -> None:
    zone = resolve_default_timezone(
        source_url="https://example.com/feed",
        default_timezone="America/New_York",
    )
    assert zone == "America/New_York"
    parsed = parse_published("2026-07-15 12:00:00", default_timezone=zone)
    assert parsed is not None
    assert parsed.utcoffset() == timedelta(hours=-4)
    assert parsed.astimezone(UTC) == datetime(2026, 7, 15, 16, 0, tzinfo=UTC)


def test_config_default_timezone_is_honored() -> None:
    zone = resolve_default_timezone(
        source_url="https://news.example.com/rss",
        config={"default_timezone": "Asia/Seoul"},
    )
    assert zone == "Asia/Seoul"


def test_feedparser_struct_time_is_utc_via_timegm(monkeypatch) -> None:
    _seoul(monkeypatch)
    try:
        parsed, raw = parse_feed_entry({"published_parsed": _UTC_STRUCT})
        assert raw is None
        assert parsed == datetime(2026, 10, 29, 0, 0, tzinfo=UTC)
        assert DAILY_PARTITION_TIMEZONE == "Asia/Seoul"
    finally:
        _restore_tz(monkeypatch)


def test_dst_region_feed_keeps_each_offset() -> None:
    summer, _ = parse_feed_entry({"published": "Wed, 15 Jul 2026 12:00:00 -0400"})
    winter, _ = parse_feed_entry({"published": "Thu, 15 Jan 2026 12:00:00 -0500"})
    assert summer is not None and winter is not None
    assert summer.utcoffset() == timedelta(hours=-4)
    assert winter.utcoffset() == timedelta(hours=-5)
    assert summer.astimezone(UTC) == datetime(2026, 7, 15, 16, 0, tzinfo=UTC)
    assert winter.astimezone(UTC) == datetime(2026, 1, 15, 17, 0, tzinfo=UTC)

    edt, _ = parse_feed_entry({"published": "Wed, 15 Jul 2026 12:00:00 EDT"})
    assert edt is not None
    assert edt.utcoffset() == timedelta(hours=-4)


def test_html_time_without_zone_uses_source_default() -> None:
    html = '<article><time datetime="2026-10-29T18:00:00">2026년 10월 29일</time></article>'
    parsed, raw = parse_html_published(html, default_timezone="Asia/Seoul")
    assert raw == "2026-10-29T18:00:00"
    assert parsed == datetime(2026, 10, 29, 18, 0, tzinfo=_KST)


def test_korean_date_text_and_kst_abbreviation() -> None:
    parsed = parse_published("2026년 10월 29일 오후 6시", default_timezone="UTC")
    assert parsed == datetime(2026, 10, 29, 18, 0, tzinfo=UTC)
    kst = parse_published("Thu, 29 Oct 2026 18:00:00 KST", default_timezone="UTC")
    assert kst is not None
    assert kst.utcoffset() == timedelta(hours=9)
    assert kst.astimezone(UTC) == datetime(2026, 10, 29, 9, 0, tzinfo=UTC)


def test_daily_partition_is_kst_across_utc_midnight() -> None:
    assert daily_partition_date(
        datetime(2026, 10, 28, 14, 30, tzinfo=UTC)
    ).isoformat() == ("2026-10-28")
    assert daily_partition_date(
        datetime(2026, 10, 28, 15, 30, tzinfo=UTC)
    ).isoformat() == ("2026-10-29")


def test_storage_round_trip_keeps_utc_instant_and_raw(tmp_path: Path) -> None:
    storage = RadarStorage(tmp_path / "radar.duckdb")
    try:
        article = Article(
            title="패치",
            link="https://www.inven.co.kr/board/1",
            summary="요약",
            published=datetime(2026, 10, 29, 18, 0, tzinfo=_KST),
            source="인벤",
            category="game",
            published_raw="2026-10-29 18:00:00",
        )
        storage.upsert_articles([article])
        row = storage.conn.execute(
            "SELECT published, published_raw FROM articles"
        ).fetchone()
        assert row is not None
        assert row[0] == datetime(2026, 10, 29, 9, 0)
        assert row[1] == "2026-10-29 18:00:00"
        loaded = storage.recent_articles("game", days=3650)
        assert loaded[0].published == datetime(2026, 10, 29, 9, 0, tzinfo=UTC)
        assert loaded[0].published_raw == "2026-10-29 18:00:00"
        assert to_utc_iso(loaded[0].published) == "2026-10-29T09:00:00+00:00"
    finally:
        storage.close()


def test_migration_adds_published_raw(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE articles (
            id BIGINT,
            category TEXT,
            source TEXT,
            title TEXT,
            link TEXT,
            summary TEXT,
            published TIMESTAMP,
            collected_at TIMESTAMP,
            entities_json TEXT
        )
        """)
    conn.close()
    storage = RadarStorage(db_path)
    try:
        columns = {
            str(row[1])
            for row in storage.conn.execute("PRAGMA table_info('articles')").fetchall()
        }
        assert "published_raw" in columns
    finally:
        storage.close()
