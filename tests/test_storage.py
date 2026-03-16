"""Tests for RadarStorage — upsert, query, delete, context manager."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from radar_core.models import Article
from radar_core.storage import RadarStorage


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture()
def storage(tmp_path: Path):
    db = RadarStorage(tmp_path / "test.duckdb")
    yield db
    db.close()


@pytest.fixture()
def sample_article() -> Article:
    return Article(
        title="테스트 기사",
        link="https://example.com/1",
        summary="요약",
        published=datetime(2026, 1, 1, tzinfo=UTC),
        source="TestSource",
        category="test",
    )


def _make_article(
    *,
    title: str = "Article",
    link: str = "https://example.com/default",
    summary: str = "summary",
    published: datetime | None = None,
    source: str = "TestSource",
    category: str = "test",
    matched_entities: dict[str, list[str]] | None = None,
) -> Article:
    return Article(
        title=title,
        link=link,
        summary=summary,
        published=published or datetime.now(UTC),
        source=source,
        category=category,
        matched_entities=matched_entities or {},
    )


# ── Tests ─────────────────────────────────────────────────────────────────


def test_upsert_single_article(storage: RadarStorage, sample_article: Article) -> None:
    """단일 기사 저장 후 조회."""
    storage.upsert_articles([sample_article])
    results = storage.recent_articles("test", days=365)

    assert len(results) == 1
    assert results[0].title == "테스트 기사"
    assert results[0].link == "https://example.com/1"


def test_upsert_deduplication(storage: RadarStorage) -> None:
    """동일 링크 기사를 두 번 저장하면 1건만 남고 최신 값으로 갱신."""
    first = _make_article(title="Original", link="https://example.com/dup")
    second = _make_article(title="Updated", link="https://example.com/dup")

    storage.upsert_articles([first])
    storage.upsert_articles([second])
    results = storage.recent_articles("test", days=30)

    assert len(results) == 1
    assert results[0].title == "Updated"


def test_recent_articles_default(storage: RadarStorage) -> None:
    """기본 7일 필터 — 최근 기사만 반환."""
    recent = _make_article(
        title="Recent",
        link="https://example.com/recent",
        published=datetime.now(UTC) - timedelta(days=2),
    )
    old = _make_article(
        title="Old",
        link="https://example.com/old",
        published=datetime.now(UTC) - timedelta(days=10),
    )

    storage.upsert_articles([recent, old])
    results = storage.recent_articles("test")  # default days=7

    assert len(results) == 1
    assert results[0].title == "Recent"


def test_recent_articles_custom_days(storage: RadarStorage) -> None:
    """커스텀 days 파라미터로 기간 변경."""
    article = _make_article(
        title="Mid-range",
        link="https://example.com/mid",
        published=datetime.now(UTC) - timedelta(days=15),
    )

    storage.upsert_articles([article])

    assert len(storage.recent_articles("test", days=7)) == 0
    assert len(storage.recent_articles("test", days=30)) == 1


def test_recent_articles_limit(storage: RadarStorage) -> None:
    """limit 파라미터로 결과 수 제한."""
    articles = [
        _make_article(
            title=f"A{i}",
            link=f"https://example.com/limit-{i}",
            published=datetime.now(UTC) - timedelta(hours=i),
        )
        for i in range(5)
    ]

    storage.upsert_articles(articles)
    results = storage.recent_articles("test", days=30, limit=3)

    assert len(results) == 3


def test_delete_older_than(storage: RadarStorage) -> None:
    """오래된 기사 삭제 — 삭제 건수 반환."""
    old = _make_article(
        title="Old",
        link="https://example.com/del-old",
        published=datetime.now(UTC) - timedelta(days=60),
    )
    recent = _make_article(
        title="Recent",
        link="https://example.com/del-recent",
        published=datetime.now(UTC) - timedelta(days=1),
    )

    storage.upsert_articles([old, recent])
    deleted = storage.delete_older_than(days=30)
    remaining = storage.recent_articles("test", days=365)

    assert deleted == 1
    assert len(remaining) == 1
    assert remaining[0].title == "Recent"


def test_context_manager(tmp_path: Path) -> None:
    """with 문으로 사용 시 정상 동작 후 close."""
    db_path = tmp_path / "ctx.duckdb"
    with RadarStorage(db_path) as store:
        article = _make_article(link="https://example.com/ctx")
        store.upsert_articles([article])
        results = store.recent_articles("test", days=30)
        assert len(results) == 1

    # close 후 재사용 시 에러
    with pytest.raises(Exception):
        store.upsert_articles([article])


def test_empty_db_returns_empty_list(storage: RadarStorage) -> None:
    """빈 DB 조회 시 빈 리스트 반환."""
    results = storage.recent_articles("test", days=30)
    assert results == []


def test_upsert_multiple_articles(storage: RadarStorage) -> None:
    """여러 기사 배치 저장."""
    articles = [
        _make_article(title=f"Batch-{i}", link=f"https://example.com/batch-{i}")
        for i in range(10)
    ]

    storage.upsert_articles(articles)
    results = storage.recent_articles("test", days=30, limit=200)

    assert len(results) == 10


def test_category_filter(storage: RadarStorage) -> None:
    """카테고리별 필터링."""
    tech = _make_article(
        title="Tech", link="https://example.com/cat-tech", category="tech"
    )
    game = _make_article(
        title="Game", link="https://example.com/cat-game", category="game"
    )

    storage.upsert_articles([tech, game])

    assert len(storage.recent_articles("tech", days=30)) == 1
    assert len(storage.recent_articles("game", days=30)) == 1
    assert len(storage.recent_articles("nonexistent", days=30)) == 0


def test_upsert_preserves_entities_json(storage: RadarStorage) -> None:
    """matched_entities가 JSON으로 저장되고 다시 복원."""
    article = _make_article(
        link="https://example.com/ent",
        matched_entities={"Nintendo": ["닌텐도", "nintendo"], "Sony": ["sony"]},
    )

    storage.upsert_articles([article])
    results = storage.recent_articles("test", days=30)

    assert results[0].matched_entities == {
        "Nintendo": ["닌텐도", "nintendo"],
        "Sony": ["sony"],
    }


def test_upsert_with_run_id_metadata(storage: RadarStorage) -> None:
    """run_id, collector_version, fetch_status 메타데이터 저장."""
    article = _make_article(link="https://example.com/meta")

    storage.upsert_articles(
        [article],
        run_id="run-001",
        collector_version="0.2.0",
        fetch_status="success",
    )
    results = storage.recent_articles("test", days=30)

    assert len(results) == 1


def test_upsert_empty_iterable_is_noop(storage: RadarStorage) -> None:
    """빈 이터러블 전달 시 아무 동작 없음."""
    storage.upsert_articles([])
    assert storage.recent_articles("test", days=30) == []


def test_article_without_published_uses_collected_at(storage: RadarStorage) -> None:
    """published가 None인 기사도 collected_at 기준으로 조회 가능."""
    article = _make_article(
        link="https://example.com/no-pub",
        published=None,
    )

    storage.upsert_articles([article])
    results = storage.recent_articles("test", days=7)

    assert len(results) == 1
