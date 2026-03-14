from __future__ import annotations

from collections.abc import Callable
from typing import cast

import duckdb

MigrationFn = Callable[[duckdb.DuckDBPyConnection], None]


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = cast(
        tuple[object] | None,
        conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = CURRENT_SCHEMA() AND table_name = ?
            LIMIT 1
            """,
            [table_name],
        ).fetchone(),
    )
    return row is not None


def _ensure_migrations_table(conn: duckdb.DuckDBPyConnection) -> None:
    _ = conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _migrations (
            version TEXT PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL
        )
        """
    )


def _applied_versions(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = cast(
        list[tuple[object]], conn.execute("SELECT version FROM _migrations").fetchall()
    )
    return {str(row[0]) for row in rows}


def _migration_v001_lineage_columns(conn: duckdb.DuckDBPyConnection) -> None:
    if not _table_exists(conn, "articles"):
        return

    table_info_rows = cast(
        list[tuple[object, ...]],
        conn.execute("PRAGMA table_info('articles')").fetchall(),
    )
    columns = {str(row[1]) for row in table_info_rows}
    expected: tuple[tuple[str, str], ...] = (
        ("run_id", "TEXT"),
        ("collector_version", "TEXT"),
        ("fetch_status", "TEXT"),
        ("fetched_at", "TIMESTAMP"),
    )

    for name, data_type in expected:
        if name in columns:
            continue
        _ = conn.execute(f"ALTER TABLE articles ADD COLUMN {name} {data_type}")


def _migration_v002_crawl_health(conn: duckdb.DuckDBPyConnection) -> None:
    _ = conn.execute(
        """
        CREATE TABLE IF NOT EXISTS crawl_health (
            source_name TEXT PRIMARY KEY,
            success_count INTEGER DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            current_delay REAL DEFAULT 0.0,
            last_error TEXT,
            disabled BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


_MIGRATIONS: tuple[tuple[str, MigrationFn], ...] = (
    ("v001_lineage_columns", _migration_v001_lineage_columns),
    ("v002_crawl_health", _migration_v002_crawl_health),
)


def migrate(conn: duckdb.DuckDBPyConnection) -> list[str]:
    _ensure_migrations_table(conn)
    applied = _applied_versions(conn)

    newly_applied: list[str] = []
    for version, migration_fn in _MIGRATIONS:
        if version in applied:
            continue
        migration_fn(conn)
        _ = conn.execute(
            "INSERT INTO _migrations (version, applied_at) VALUES (?, CURRENT_TIMESTAMP)",
            [version],
        )
        newly_applied.append(version)

    return newly_applied
