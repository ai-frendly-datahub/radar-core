from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
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
        return dt.astimezone(UTC).replace(tzinfo=None)
    return dt


class RadarStorage:
    def __init__(self, db_path: Path):
        self.db_path: Path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(str(self.db_path))
        self._ensure_tables()

    def close(self) -> None:
        self.conn.close()

    def _validate_ontology(
        self,
        articles: list[Article],
        *,
        repo_name: str,
        strict: bool,
        violations: list[dict[str, object]] | None,
    ) -> None:
        from .ontology import load_runtime_contract, validate_article_ontology

        contract = load_runtime_contract(repo_name, search_from=self.db_path)
        if contract is None:
            return
        for article in articles:
            errors = validate_article_ontology(article.ontology, contract=contract)
            if not errors:
                continue
            record: dict[str, object] = {
                "repo": repo_name,
                "link": article.link,
                "source": article.source,
                "errors": list(errors),
            }
            if violations is not None:
                violations.append(record)
            if strict:
                raise StorageError(
                    f"ontology validation failed for {article.link!r}: {errors}"
                )

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
                ontology_json TEXT,
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
        repo_name: str | None = None,
        strict_ontology: bool = False,
        ontology_violations: list[dict[str, object]] | None = None,
    ) -> None:
        articles = list(articles)
        if repo_name:
            self._validate_ontology(
                articles,
                repo_name=repo_name,
                strict=strict_ontology,
                violations=ontology_violations,
            )

        now = _utc_naive(datetime.now(UTC))
        fetched_at = now if (run_id or collector_version or fetch_status) else None
        from .url_utils import canonical_url

        rows: list[tuple[object, ...]] = []
        for article in articles:
            # Canonicalize link for deduplication (strips utm_*, normalizes
            # scheme/host/port/path/query/fragment). Falls back to the
            # original link if the helper returns empty.
            link = canonical_url(article.link) or article.link
            rows.append(
                (
                    article.category,
                    article.source,
                    article.title,
                    link,
                    article.summary,
                    _utc_naive(article.published),
                    now,
                    json.dumps(article.matched_entities, ensure_ascii=False),
                    json.dumps(article.ontology, ensure_ascii=False),
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
                    ontology_json,
                    run_id,
                    collector_version,
                    fetch_status,
                    fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(link) DO UPDATE SET
                    title = EXCLUDED.title,
                    summary = EXCLUDED.summary,
                    published = EXCLUDED.published,
                    collected_at = EXCLUDED.collected_at,
                    entities_json = EXCLUDED.entities_json,
                    ontology_json = EXCLUDED.ontology_json,
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
        since = _utc_naive(datetime.now(UTC) - timedelta(days=days))
        cur = self.conn.execute(
            """
            SELECT category, source, title, link, summary, published, collected_at, entities_json, ontology_json
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
                raw_ontology,
            ) = row
            published_at = published if isinstance(published, datetime) else None
            collected = collected_at if isinstance(collected_at, datetime) else None

            entities = _parse_entities_json(raw_entities)
            ontology = _parse_ontology_json(raw_ontology)

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
                    ontology=ontology,
                )
            )
        return results

    def compute_cluster_ids(
        self,
        category: str | None = None,
        *,
        days: int = 7,
        threshold: float = 0.85,
    ) -> int:
        """Cluster recent article titles and persist a stable ``cluster_id``.

        Reads titles in the window (optionally filtered to ``category``),
        runs :func:`radar_core.dedup.cluster_titles`, then derives a
        cross-run stable id per cluster by hashing the alphabetically-first
        normalized representative title. Writes the result back to the
        ``articles.cluster_id`` column.

        Returns the number of rows updated.
        """
        import hashlib

        from .dedup import cluster_titles, normalize_title

        since = _utc_naive(datetime.now(UTC) - timedelta(days=days))
        params: list[object] = [since]
        sql = (
            "SELECT link, title FROM articles "
            "WHERE COALESCE(published, collected_at) >= ?"
        )
        if category:
            sql += " AND category = ?"
            params.append(category)
        sql += " ORDER BY link"

        rows = cast(
            list[tuple[str, str]],
            self.conn.execute(sql, params).fetchall(),
        )
        if not rows:
            return 0

        links = [str(r[0]) for r in rows]
        titles = [str(r[1]) for r in rows]
        cluster_local_ids = cluster_titles(titles, threshold=threshold)

        members: dict[int, list[str]] = {}
        for cid, title in zip(cluster_local_ids, titles, strict=True):
            members.setdefault(cid, []).append(title)

        stable_ids: dict[int, str] = {}
        for cid, member_titles in members.items():
            reps = sorted(
                (" ".join(normalize_title(t)) for t in member_titles if t),
                key=lambda s: (len(s), s),
            )
            seed = reps[0] if reps else f"cluster-{cid}"
            stable_ids[cid] = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]

        updates = [
            (stable_ids[cid], link)
            for cid, link in zip(cluster_local_ids, links, strict=True)
        ]
        try:
            _ = self.conn.executemany(
                "UPDATE articles SET cluster_id = ? WHERE link = ?",
                updates,
            )
            _ = self.conn.commit()
        except duckdb.Error as exc:
            try:
                _ = self.conn.rollback()
            except duckdb.Error:
                pass
            raise StorageError("Failed to write cluster ids") from exc
        return len(updates)

    def delete_older_than(self, days: int) -> int:
        cutoff = _utc_naive(datetime.now(UTC) - timedelta(days=days))
        count_row = self.conn.execute(
            "SELECT COUNT(*) FROM articles WHERE COALESCE(published, collected_at) < ?",
            [cutoff],
        ).fetchone()
        to_delete = count_row[0] if count_row else 0
        _ = self.conn.execute(
            "DELETE FROM articles WHERE COALESCE(published, collected_at) < ?",
            [cutoff],
        )
        return to_delete


def _parse_entities_json(raw_entities: str | None) -> dict[str, list[str]]:
    entities: dict[str, list[str]] = {}
    if not raw_entities:
        return entities
    try:
        parsed_entities = cast(object, json.loads(raw_entities))
    except json.JSONDecodeError:
        return entities
    if not isinstance(parsed_entities, dict):
        return entities
    parsed_map = cast(dict[object, object], parsed_entities)
    for name, keywords in parsed_map.items():
        if not isinstance(name, str) or not isinstance(keywords, list):
            continue
        entities[name] = [str(keyword) for keyword in cast(list[object], keywords)]
    return entities


def _parse_ontology_json(raw_ontology: str | None) -> dict[str, object]:
    if not raw_ontology:
        return {}
    try:
        parsed_ontology = cast(object, json.loads(raw_ontology))
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed_ontology, dict):
        return {}
    return {
        str(key): value
        for key, value in cast(dict[object, object], parsed_ontology).items()
        if str(key).strip()
    }
