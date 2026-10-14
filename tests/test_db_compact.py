"""DuckDB compaction keeps committed databases under GitHub's file limit."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from radar_core.date_storage import apply_date_storage_policy
from radar_core.db_compact import GITHUB_FILE_LIMIT_BYTES, compact_duckdb_file
from radar_core.models import Article
from radar_core.storage import RadarStorage


def _article(index: int, *, published: datetime) -> Article:
    # Unique text so DuckDB cannot compress the file back down to its allocation floor.
    return Article(
        title=f"article {index}",
        link=f"https://example.com/{index}",
        summary=f"{index:08d}" * 400,
        published=published,
        source="test",
        category="test",
    )


def _seed_history(path: Path, *, old: int, recent: int) -> None:
    now = datetime.now(UTC)
    articles = [_article(i, published=now - timedelta(days=120)) for i in range(old)]
    articles.extend(
        _article(old + i, published=now - timedelta(days=1)) for i in range(recent)
    )
    storage = RadarStorage(path)
    try:
        storage.upsert_articles(articles)
    finally:
        # Bypass close() compaction so the test controls the budget itself.
        storage.conn.close()


def test_compact_keeps_recent_rows_and_shrinks_file(tmp_path: Path) -> None:
    path = tmp_path / "radar_data.duckdb"
    _seed_history(path, old=800, recent=8)
    before = path.stat().st_size
    assert before > 3_000_000

    result = compact_duckdb_file(path, max_bytes=2_500_000, min_keep=4)

    assert result["compacted"] is True
    assert result["reason"] == "compacted"
    assert int(result["bytes"]) <= 2_500_000
    assert int(result["bytes"]) < before
    assert int(result["rows_deleted"]) > 0

    storage = RadarStorage(path)
    try:
        recent = storage.recent_articles("test", days=7, limit=50)
        stored = storage.conn.execute("SELECT COUNT(*) FROM articles").fetchone()
    finally:
        storage.conn.close()

    assert stored is not None
    assert int(stored[0]) < 408
    assert len(recent) == 8
    assert {article.link for article in recent} == {
        f"https://example.com/{800 + i}" for i in range(8)
    }


def test_stale_database_is_trimmed_not_emptied(tmp_path: Path) -> None:
    """A database with no rows inside the age window keeps its newest history."""
    path = tmp_path / "stale.duckdb"
    now = datetime.now(UTC)
    articles = [
        _article(index, published=now - timedelta(days=200, seconds=800 - index))
        for index in range(800)
    ]
    storage = RadarStorage(path)
    try:
        storage.upsert_articles(articles)
    finally:
        storage.conn.close()
    before = path.stat().st_size
    assert before > 2_000_000

    result = compact_duckdb_file(path, max_bytes=2_000_000, min_keep=40)

    assert result["compacted"] is True
    assert result["reason"] == "compacted"
    assert int(result["bytes"]) <= 2_000_000
    assert int(result["rows_deleted"]) < 800

    storage = RadarStorage(path)
    try:
        stored = storage.conn.execute(
            "SELECT link FROM articles ORDER BY published DESC"
        ).fetchall()
    finally:
        storage.conn.close()

    assert stored is not None
    assert 40 <= len(stored) < 800
    assert stored[0][0] == "https://example.com/799"


def test_small_database_is_not_rewritten(tmp_path: Path) -> None:
    path = tmp_path / "small.duckdb"
    _seed_history(path, old=2, recent=1)
    before = path.stat().st_size

    result = compact_duckdb_file(path, max_bytes=GITHUB_FILE_LIMIT_BYTES)

    assert result["compacted"] is False
    assert result["reason"] == "within_limit"
    assert path.stat().st_size == before


def test_storage_close_compacts_when_budget_is_low(
    tmp_path: Path, monkeypatch: object
) -> None:
    monkeypatch.setenv("RADAR_DUCKDB_MAX_BYTES", "2500000")  # type: ignore[attr-defined]
    path = tmp_path / "closed.duckdb"
    now = datetime.now(UTC)
    articles = [_article(i, published=now - timedelta(days=200)) for i in range(800)]
    articles.append(
        Article(
            title="fresh",
            link="https://example.com/999",
            summary="fresh",
            published=now - timedelta(hours=2),
            source="test",
            category="test",
        )
    )
    storage = RadarStorage(path)
    storage.upsert_articles(articles)
    storage.close()

    assert path.stat().st_size <= 2_500_000
    reopened = RadarStorage(path)
    try:
        recent = reopened.recent_articles("test", days=7, limit=10)
    finally:
        reopened.conn.close()
    assert [article.link for article in recent] == ["https://example.com/999"]


def test_apply_date_storage_policy_snapshots_compacted_file(tmp_path: Path) -> None:
    path = tmp_path / "policy.duckdb"
    _seed_history(path, old=800, recent=3)
    raw_dir = tmp_path / "raw"
    report_dir = tmp_path / "reports"
    raw_dir.mkdir()
    report_dir.mkdir()

    result = apply_date_storage_policy(
        database_path=path,
        raw_data_dir=raw_dir,
        report_dir=report_dir,
        keep_raw_days=7,
        keep_report_days=7,
        snapshot_db=True,
        max_bytes=2_500_000,
    )

    assert result["db_compacted"] is True
    assert int(result["db_bytes"]) <= 2_500_000
    snapshot = result["snapshot_path"]
    assert isinstance(snapshot, str)
    snapshot_path = Path(snapshot)
    assert snapshot_path.exists()
    assert snapshot_path.stat().st_size <= 2_500_000
    assert snapshot_path.stat().st_size == path.stat().st_size


def test_foreign_key_parent_is_kept_while_child_rows_trim(tmp_path: Path) -> None:
    path = tmp_path / "fk.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE products (
            product_id VARCHAR PRIMARY KEY,
            first_seen TIMESTAMP,
            blob VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE price_scores (
            score_id INTEGER PRIMARY KEY,
            product_id VARCHAR NOT NULL REFERENCES products(product_id),
            ts TIMESTAMP,
            blob VARCHAR
        )
        """
    )
    now = datetime.now(UTC).replace(tzinfo=None)
    for index in range(20):
        conn.execute(
            "INSERT INTO products VALUES (?, ?, ?)",
            [str(index), now - timedelta(days=150), "p" * 200],
        )
    for index in range(400):
        conn.execute(
            "INSERT INTO price_scores VALUES (?, ?, ?, ?)",
            [
                index,
                str(index % 20),
                now - timedelta(days=150, seconds=400 - index),
                f"{index:04d}" * 200,
            ],
        )
    conn.close()

    before = path.stat().st_size
    result = compact_duckdb_file(path, max_bytes=before - 1, min_keep=20)

    assert result["compacted"] is True
    assert int(result["bytes"]) < before
    assert int(result["rows_deleted"]) > 0
    conn = duckdb.connect(str(path), read_only=True)
    try:
        products = conn.execute("SELECT COUNT(*) FROM products").fetchone()
        scores = conn.execute("SELECT COUNT(*) FROM price_scores").fetchone()
    finally:
        conn.close()
    assert products is not None and int(products[0]) == 20
    assert scores is not None and 20 <= int(scores[0]) < 400
