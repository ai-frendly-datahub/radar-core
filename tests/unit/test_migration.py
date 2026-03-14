from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import duckdb

from radar_core.migration import migrate
from radar_core.models import Article
from radar_core.storage import RadarStorage


def _create_legacy_articles_schema(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        _ = conn.execute(
            """
            CREATE SEQUENCE IF NOT EXISTS articles_id_seq START 1;
            CREATE TABLE IF NOT EXISTS articles (
                id BIGINT PRIMARY KEY DEFAULT nextval('articles_id_seq'),
                category TEXT NOT NULL,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT NOT NULL UNIQUE,
                summary TEXT,
                published TIMESTAMP,
                collected_at TIMESTAMP NOT NULL,
                entities_json TEXT
            );
            """
        )
    finally:
        conn.close()


def _column_names(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = cast(
        list[tuple[object, ...]],
        conn.execute(f"PRAGMA table_info('{table_name}')").fetchall(),
    )
    return {str(row[1]) for row in rows}


def test_migration_adds_lineage_columns_to_legacy_articles(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_schema.duckdb"
    _create_legacy_articles_schema(db_path)

    conn = duckdb.connect(str(db_path))
    try:
        applied_versions = migrate(conn)
        columns = _column_names(conn, "articles")
    finally:
        conn.close()

    assert "v001_lineage_columns" in applied_versions
    assert "run_id" in columns
    assert "collector_version" in columns
    assert "fetch_status" in columns
    assert "fetched_at" in columns


def test_migration_preserves_old_data_and_new_columns_are_null(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_data.duckdb"
    _create_legacy_articles_schema(db_path)

    legacy_collected = datetime(2026, 3, 11, 8, 0, tzinfo=UTC)
    conn = duckdb.connect(str(db_path))
    try:
        _ = conn.execute(
            """
            INSERT INTO articles (category, source, title, link, summary, published, collected_at, entities_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "tech",
                "Example RSS",
                "Legacy article",
                "https://example.com/legacy",
                "legacy summary",
                None,
                legacy_collected,
                '{""topic"": [""ai""]}',
            ],
        )
        _ = migrate(conn)
        row = cast(
            tuple[object, object, object, object, object] | None,
            conn.execute(
                """
            SELECT title, run_id, collector_version, fetch_status, fetched_at
            FROM articles
            WHERE link = ?
            """,
                ["https://example.com/legacy"],
            ).fetchone(),
        )
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "Legacy article"
    assert row[1] is None
    assert row[2] is None
    assert row[3] is None
    assert row[4] is None


def test_migration_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "idempotent.duckdb"
    _create_legacy_articles_schema(db_path)

    conn = duckdb.connect(str(db_path))
    try:
        first_applied = migrate(conn)
        second_applied = migrate(conn)
    finally:
        conn.close()

    assert first_applied == ["v001_lineage_columns", "v002_crawl_health"]
    assert second_applied == []


def test_migration_records_version_in_migrations_table(tmp_path: Path) -> None:
    db_path = tmp_path / "migration_record.duckdb"
    _create_legacy_articles_schema(db_path)

    conn = duckdb.connect(str(db_path))
    try:
        _ = migrate(conn)
        row = cast(
            tuple[object, object] | None,
            conn.execute(
                "SELECT version, applied_at FROM _migrations WHERE version = ?",
                ["v001_lineage_columns"],
            ).fetchone(),
        )
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "v001_lineage_columns"
    assert row[1] is not None


def test_upsert_articles_accepts_lineage_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "lineage_upsert.duckdb"
    storage = RadarStorage(db_path)
    article = Article(
        title="Lineage write",
        link="https://example.com/lineage",
        summary="lineage metadata",
        published=datetime(2026, 3, 11, 11, 0, tzinfo=UTC),
        source="Example RSS",
        category="tech",
        matched_entities={"topic": ["ai"]},
    )

    try:
        storage.upsert_articles(
            [article],
            run_id="00000000-0000-0000-0000-000000000017",
            collector_version="0.1.0",
            fetch_status="success",
        )
        row = cast(
            tuple[object, object, object, object] | None,
            storage.conn.execute(
                """
                SELECT run_id, collector_version, fetch_status, fetched_at
                FROM articles
                WHERE link = ?
                """,
                [article.link],
            ).fetchone(),
        )
    finally:
        storage.close()

    assert row is not None
    assert row[0] == "00000000-0000-0000-0000-000000000017"
    assert row[1] == "0.1.0"
    assert row[2] == "success"
    assert row[3] is not None
