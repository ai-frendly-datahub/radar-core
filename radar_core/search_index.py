from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import cast


@dataclass
class SearchResult:
    link: str
    title: str
    snippet: str
    rank: float


class SearchIndex:
    _db_path: Path
    _conn: sqlite3.Connection | None

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._create_schema()

    def __enter__(self) -> SearchIndex:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        _ = (exc_type, exc_value, traceback)
        self.close()

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise sqlite3.ProgrammingError("SearchIndex connection is closed")
        return self._conn

    def _create_schema(self) -> None:
        conn = self._connection()
        _ = conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS documents (
                link TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                body TEXT NOT NULL
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                title, body, content='documents', content_rowid='rowid'
            );

            CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
                INSERT INTO documents_fts(rowid, title, body)
                VALUES (new.rowid, new.title, new.body);
            END;

            CREATE TRIGGER IF NOT EXISTS documents_ad AFTER DELETE ON documents BEGIN
                INSERT INTO documents_fts(documents_fts, rowid, title, body)
                VALUES ('delete', old.rowid, old.title, old.body);
            END;

            CREATE TRIGGER IF NOT EXISTS documents_au AFTER UPDATE ON documents BEGIN
                INSERT INTO documents_fts(documents_fts, rowid, title, body)
                VALUES ('delete', old.rowid, old.title, old.body);
                INSERT INTO documents_fts(rowid, title, body)
                VALUES (new.rowid, new.title, new.body);
            END;
            """
        )
        conn.commit()

    def upsert(self, link: str, title: str, body: str) -> None:
        conn = self._connection()
        _ = conn.execute("DELETE FROM documents WHERE link = ?", (link,))
        _ = conn.execute(
            "INSERT INTO documents(link, title, body) VALUES (?, ?, ?)",
            (link, title, body),
        )
        conn.commit()

    def search(self, query: str, *, limit: int = 20) -> list[SearchResult]:
        if limit <= 0:
            return []

        conn = self._connection()
        cursor = conn.execute(
            """
            SELECT
                d.link AS link,
                d.title AS title,
                snippet(documents_fts, 1, '<b>', '</b>', '...', 32) AS snippet,
                bm25(documents_fts) AS rank
            FROM documents_fts
            JOIN documents AS d ON d.rowid = documents_fts.rowid
            WHERE documents_fts MATCH ?
            ORDER BY rank ASC
            LIMIT ?
            """,
            (query, limit),
        )

        rows = cast(list[tuple[str, str, str, float]], cursor.fetchall())
        results: list[SearchResult] = []
        for row in rows:
            link, title, snippet_text, rank = row
            results.append(
                SearchResult(
                    link=str(link),
                    title=str(title),
                    snippet=str(snippet_text),
                    rank=float(rank),
                )
            )
        return results

    def close(self) -> None:
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None
