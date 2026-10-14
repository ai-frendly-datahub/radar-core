"""Keep DuckDB files under GitHub's 100 MiB per-file limit.

Collectors commit ``*.duckdb`` to the repository. GitHub rejects any file
at or above 100 MiB, which fails the data push even when collection itself
succeeded. Compaction deletes the oldest timestamped rows and rewrites the
file so the next commit stays under the limit.
"""

from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

# GitHub rejects blobs at or above 100 MiB (1024-based).
GITHUB_FILE_LIMIT_BYTES = 100 * 1024 * 1024
# Leave headroom for the rows inserted on the same run, before compaction.
DEFAULT_DUCKDB_MAX_BYTES = 90 * 1024 * 1024

_TIMESTAMP_PREFERENCE: tuple[str, ...] = (
    "collected_at",
    "published",
    "event_ts",
    "snapshot_at",
    "snapshot_time",
    "created_at",
    "updated_at",
    "fetched_at",
    "observed_at",
    "ts",
    "date",
    "timestamp",
)
# Never drop rows newer than 7 days just to satisfy the byte budget.
# That window is what the daily report reads.
_AGE_CUTOFF_DAYS: tuple[int, ...] = (90, 60, 45, 30, 21, 14, 7)
_MIN_REPORT_DAYS = 7


def resolve_duckdb_max_bytes(max_bytes: int | None = None) -> int:
    """Return the byte budget for a committed DuckDB file.

    ``RADAR_DUCKDB_COMPACT=0`` disables compaction. ``RADAR_DUCKDB_MAX_BYTES``
    overrides the default 90 MiB budget.
    """
    if max_bytes is not None:
        return max_bytes
    if os.environ.get("RADAR_DUCKDB_COMPACT", "1").strip() == "0":
        return GITHUB_FILE_LIMIT_BYTES * 100
    raw = os.environ.get("RADAR_DUCKDB_MAX_BYTES", "").strip()
    if not raw:
        return DEFAULT_DUCKDB_MAX_BYTES
    try:
        parsed = int(raw)
    except ValueError:
        return DEFAULT_DUCKDB_MAX_BYTES
    if parsed <= 0:
        return GITHUB_FILE_LIMIT_BYTES * 100
    return parsed


def compact_duckdb_file(
    db_path: Path | str,
    *,
    max_bytes: int | None = None,
    min_keep: int = 500,
) -> dict[str, object]:
    """Shrink ``db_path`` until it is within ``max_bytes``.

    Recently published rows are kept ahead of older history. The newest
    ``min_keep`` rows of each table are kept even when that still exceeds
    the budget. A database that is already small is left untouched.
    """
    path = Path(db_path)
    budget = resolve_duckdb_max_bytes(max_bytes)
    if not path.exists():
        return {
            "compacted": False,
            "reason": "missing",
            "bytes": 0,
            "bytes_before": 0,
            "max_bytes": budget,
            "rows_deleted": 0,
        }

    before = path.stat().st_size
    if before <= budget:
        return {
            "compacted": False,
            "reason": "within_limit",
            "bytes": before,
            "bytes_before": before,
            "max_bytes": budget,
            "rows_deleted": 0,
        }

    rows_deleted = 0
    size = before
    for days in _AGE_CUTOFF_DAYS:
        if size <= budget:
            break
        removed = _delete_older_than(path, days=days)
        rows_deleted += removed
        if removed <= 0:
            continue
        # DuckDB's allocator can grow on the first rewrite. The cutoff list
        # is finite, so keep going instead of treating that as a dead end.
        size = _rewrite_inplace(path)

    for include_untimed in (False, True):
        stalls = 0
        while size > budget:
            removed = _delete_oldest_fraction(
                path,
                fraction=0.2,
                min_keep=min_keep,
                include_untimed=include_untimed,
                protect_days=_MIN_REPORT_DAYS,
            )
            rows_deleted += removed
            if removed <= 0:
                break
            new_size = _rewrite_inplace(path)
            if new_size < size:
                stalls = 0
            else:
                # One larger rewrite is normal. Two in a row means further
                # deletes are not reclaiming space.
                stalls += 1
            size = new_size
            if stalls >= 2:
                break

    return {
        "compacted": rows_deleted > 0,
        "reason": "compacted" if size <= budget else "still_over_budget",
        "bytes": size,
        "bytes_before": before,
        "max_bytes": budget,
        "rows_deleted": rows_deleted,
    }


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _is_temporal(data_type: str) -> bool:
    upper = data_type.upper()
    return "TIMESTAMP" in upper or upper == "DATE" or upper.startswith("DATE")


def _connect(path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(path))


def _tables_in_delete_order(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Child tables first, so a foreign key does not block the size trim.

    Parent rows are attempted afterwards. A constraint failure on a parent
    is skipped; the child rows that reference it are the usual bulk.
    """
    tables = _user_tables(conn)
    referenced: set[str] = set()
    try:
        rows = conn.execute(
            """
            SELECT referenced_table
            FROM duckdb_constraints()
            WHERE constraint_type = 'FOREIGN KEY'
              AND referenced_table IS NOT NULL
            """
        ).fetchall()
    except duckdb.Error:
        rows = []
    for row in rows:
        if row[0]:
            referenced.add(str(row[0]))
    children = [name for name in tables if name not in referenced]
    parents = [name for name in tables if name in referenced]
    return children + parents


def _user_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    ).fetchall()
    names: list[str] = []
    for row in rows:
        name = str(row[0])
        if name.startswith(("duckdb_", "sqlite_", "pg_")):
            continue
        names.append(name)
    return names


def _columns(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table],
    ).fetchall()
    return {str(name): str(dtype) for name, dtype in rows}


def _time_expr(columns: dict[str, str]) -> str | None:
    if "published" in columns and "collected_at" in columns:
        return "COALESCE(published, collected_at)"
    for name in _TIMESTAMP_PREFERENCE:
        dtype = columns.get(name)
        if dtype is not None and _is_temporal(dtype):
            return _quote_ident(name)
    for name, dtype in columns.items():
        if _is_temporal(dtype):
            return _quote_ident(name)
    return None


def _delete_older_than(path: Path, *, days: int) -> int:
    """Delete rows older than ``days``.

    A table whose newest row is already outside the window is left untouched.
    Price history that has not been committed for months is still the only
    history those radars have; the fractional trimmer drops just enough of
    the oldest rows to fit the byte budget instead of emptying the file.
    """
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days)
    conn = _connect(path)
    deleted = 0
    try:
        for table in _tables_in_delete_order(conn):
            quoted = _quote_ident(table)
            expr = _time_expr(_columns(conn, table))
            if expr is None:
                continue
            newer = conn.execute(
                f"SELECT COUNT(*) FROM {quoted} WHERE {expr} >= ?",
                [cutoff],
            ).fetchone()
            if newer is None or int(newer[0]) <= 0:
                continue
            before = conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()
            before_count = int(before[0]) if before else 0
            try:
                conn.execute(
                    f"DELETE FROM {quoted} WHERE {expr} < ?",
                    [cutoff],
                )
            except duckdb.ConstraintException:
                continue
            after = conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()
            after_count = int(after[0]) if after else 0
            deleted += max(0, before_count - after_count)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return deleted


def _delete_oldest_fraction(
    path: Path,
    *,
    fraction: float,
    min_keep: int,
    include_untimed: bool,
    protect_days: int,
) -> int:
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=protect_days)
    conn = _connect(path)
    deleted = 0
    try:
        for table in _tables_in_delete_order(conn):
            columns = _columns(conn, table)
            expr = _time_expr(columns)
            if expr is None and not include_untimed:
                continue
            quoted = _quote_ident(table)
            count_row = conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()
            count = int(count_row[0]) if count_row else 0
            if count <= min_keep:
                continue
            batch = max(1, int(count * fraction))
            batch = min(batch, count - min_keep)
            if batch <= 0:
                continue
            try:
                if expr is None:
                    conn.execute(
                        f"""
                        DELETE FROM {quoted}
                        WHERE rowid IN (
                            SELECT rowid FROM {quoted}
                            ORDER BY rowid ASC
                            LIMIT {int(batch)}
                        )
                        """
                    )
                else:
                    conn.execute(
                        f"""
                        DELETE FROM {quoted}
                        WHERE rowid IN (
                            SELECT rowid FROM {quoted}
                            WHERE {expr} < ?
                            ORDER BY {expr} ASC NULLS FIRST
                            LIMIT {int(batch)}
                        )
                        """,
                        [cutoff],
                    )
            except duckdb.ConstraintException:
                continue
            deleted += batch
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return deleted


def _cleanup_rewrite_artifacts(*paths: Path) -> None:
    for leftover in paths:
        if leftover.is_dir():
            shutil.rmtree(leftover, ignore_errors=True)
        elif leftover.exists():
            leftover.unlink()


def _rewrite_via_export(path: Path, tmp: Path, export_dir: Path) -> None:
    """Rebuild through SQL + Parquet so foreign keys survive the rewrite.

    ``COPY FROM DATABASE`` inserts child tables before their parents and
    rejects a database that declares foreign keys.
    """
    _cleanup_rewrite_artifacts(export_dir, tmp, Path(str(tmp) + ".wal"))
    conn = _connect(path)
    try:
        conn.execute(f"EXPORT DATABASE {_sql_string(str(export_dir))} (FORMAT PARQUET)")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    out = _connect(tmp)
    try:
        out.execute(f"IMPORT DATABASE {_sql_string(str(export_dir))}")
        out.execute("CHECKPOINT")
    except Exception:
        out.close()
        _cleanup_rewrite_artifacts(tmp, Path(str(tmp) + ".wal"))
        raise
    out.close()


def _rewrite_inplace(path: Path) -> int:
    """Copy the database into a fresh file so DELETE reclaims disk space."""
    tmp = path.with_name(path.name + ".compacttmp")
    export_dir = path.with_name(path.name + ".compactexport")
    wal = Path(str(tmp) + ".wal")
    _cleanup_rewrite_artifacts(tmp, wal, export_dir)
    conn = _connect(path)
    copied = False
    try:
        db_name = conn.execute("SELECT current_database()").fetchone()
        if db_name is None:
            raise duckdb.Error(f"could not read database name for {path}")
        conn.execute(f"ATTACH {_sql_string(str(tmp))} AS compact_db")
        conn.execute(f"COPY FROM DATABASE {_quote_ident(str(db_name[0]))} TO compact_db")
        conn.execute("DETACH compact_db")
        conn.execute("CHECKPOINT")
        copied = True
    except duckdb.ConstraintException:
        copied = False
    except Exception:
        conn.close()
        _cleanup_rewrite_artifacts(tmp, wal, export_dir)
        raise
    conn.close()
    if not copied:
        _cleanup_rewrite_artifacts(tmp, wal)
        _rewrite_via_export(path, tmp, export_dir)
    os.replace(tmp, path)
    _cleanup_rewrite_artifacts(wal, Path(str(path) + ".wal"), export_dir)
    return path.stat().st_size
