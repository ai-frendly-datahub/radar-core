from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import cast

import duckdb

from .exceptions import StorageError
from .migration import migrate
from .models import Article


def _utc_naive(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


class RadarStorage:
    def __init__(self, db_path: Path):
        self.db_path: Path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(str(self.db_path))
        self._ensure_tables()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> RadarStorage:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _ensure_tables(self) -> None:
        _ = self.conn.execute(
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
                entities_json TEXT,
                run_id TEXT,
                collector_version TEXT,
                fetch_status TEXT,
                fetched_at TIMESTAMP
            );
            """
        )
        _ = self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_category_time ON articles (category, published, collected_at);"
        )
        _ = migrate(self.conn)

    def upsert_articles(
        self,
        articles: Iterable[Article],
        *,
        run_id: str | None = None,
        collector_version: str | None = None,
        fetch_status: str | None = None,
    ) -> None:
        now = _utc_naive(datetime.now(timezone.utc))
        fetched_at = now if (run_id or collector_version or fetch_status) else None
        rows: list[tuple[object, ...]] = []
        for article in articles:
            rows.append(
                (
                    article.category,
                    article.source,
                    article.title,
                    article.link,
                    article.summary,
                    _utc_naive(article.published),
                    now,
                    json.dumps(article.matched_entities, ensure_ascii=False),
                    run_id,
                    collector_version,
                    fetch_status,
                    fetched_at,
                )
            )

        if not rows:
            return

        try:
            _ = self.conn.begin()
            _ = self.conn.executemany(
                """
                INSERT INTO articles (
                    category,
                    source,
                    title,
                    link,
                    summary,
                    published,
                    collected_at,
                    entities_json,
                    run_id,
                    collector_version,
                    fetch_status,
                    fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(link) DO UPDATE SET
                    title = EXCLUDED.title,
                    summary = EXCLUDED.summary,
                    published = EXCLUDED.published,
                    collected_at = EXCLUDED.collected_at,
                    entities_json = EXCLUDED.entities_json,
                    run_id = EXCLUDED.run_id,
                    collector_version = EXCLUDED.collector_version,
                    fetch_status = EXCLUDED.fetch_status,
                    fetched_at = EXCLUDED.fetched_at
                """,
                rows,
            )
            _ = self.conn.commit()
        except Exception as exc:
            try:
                _ = self.conn.rollback()
            except duckdb.Error:
                pass
            raise StorageError("Failed to upsert articles") from exc

    def recent_articles(
        self, category: str, *, days: int = 7, limit: int = 200
    ) -> list[Article]:
        since = _utc_naive(datetime.now(timezone.utc) - timedelta(days=days))
        cur = self.conn.execute(
            """
            SELECT category, source, title, link, summary, published, collected_at, entities_json
            FROM articles
            WHERE category = ? AND COALESCE(published, collected_at) >= ?
            ORDER BY COALESCE(published, collected_at) DESC
            LIMIT ?
            """,
            [category, since, limit],
        )
        rows = cast(
            list[
                tuple[
                    str,
                    str,
                    str,
                    str,
                    str | None,
                    datetime | None,
                    datetime | None,
                    str | None,
                ]
            ],
            cur.fetchall(),
        )

        results: list[Article] = []
        for row in rows:
            (
                category_value,
                source,
                title,
                link,
                summary,
                published,
                collected_at,
                raw_entities,
            ) = row
            published_at = published if isinstance(published, datetime) else None
            collected = collected_at if isinstance(collected_at, datetime) else None

            entities: dict[str, list[str]] = {}
            if raw_entities:
                try:
                    parsed_entities = cast(object, json.loads(raw_entities))
                    if isinstance(parsed_entities, dict):
                        parsed_map = cast(dict[object, object], parsed_entities)
                        entities = {}
                        for name, keywords in parsed_map.items():
                            if not isinstance(name, str) or not isinstance(
                                keywords, list
                            ):
                                continue
                            normalized_keywords: list[str] = []
                            for keyword in cast(list[object], keywords):
                                normalized_keywords.append(str(keyword))
                            entities[name] = normalized_keywords
                except json.JSONDecodeError:
                    entities = {}

            results.append(
                Article(
                    title=str(title),
                    link=str(link),
                    summary=str(summary) if summary is not None else "",
                    published=published_at,
                    source=str(source),
                    category=str(category_value),
                    matched_entities=entities,
                    collected_at=collected,
                )
            )
        return results

    def delete_older_than(self, days: int) -> int:
        cutoff = _utc_naive(datetime.now(timezone.utc) - timedelta(days=days))
        count_row = self.conn.execute(
            "SELECT COUNT(*) FROM articles WHERE COALESCE(published, collected_at) < ?",
            [cutoff],
        ).fetchone()
        to_delete = count_row[0] if count_row else 0
        _ = self.conn.execute(
            "DELETE FROM articles WHERE COALESCE(published, collected_at) < ?", [cutoff]
        )
        return to_delete
