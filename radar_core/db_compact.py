"""Keep DuckDB files under GitHub's 100 MiB per-file limit.

Collectors commit ``*.duckdb`` to the repository. GitHub rejects any file
at or above 100 MiB, which fails the data push even when collection itself
succeeded. Compaction deletes the oldest timestamped rows and rewrites the
file so the next commit stays under the limit.

An age cutoff never empties a table whose newest row is already outside the
window. When the file is still over budget, the oldest rows of the largest
tables are removed until the file fits, while each table keeps its newest
rows and, when that is enough, its latest day.
"""

from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from .logger import get_logger

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
# Never drop rows newer than 7 days during the age pass.
# That window is what the daily report reads. The budget pass may still
# trim inside it when the file would otherwise stay over GitHub's limit.
_AGE_CUTOFF_DAYS: tuple[int, ...] = (90, 60, 45, 30, 21, 14, 7)
_MAX_BUDGET_PASSES = 24
_TableStats = dict[str, dict[str, int]]

logger = get_logger(__name__)


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

    Recently published rows are kept ahead of older history. Each table keeps
    at least its newest ``min_keep`` rows (and the latest day, when that fits
    the budget). A database that is already small is left untouched.

    Failures are returned in the stats dict. This function does not raise, so
    a snapshot taken after compaction is not blocked by a compact error.
    """
    path = Path(db_path)
    budget = resolve_duckdb_max_bytes(max_bytes)
    keep_floor = max(1, int(min_keep))
    try:
        return _compact_duckdb_file(path, budget=budget, min_keep=keep_floor)
    except Exception as exc:
        logger.warning("duckdb_compact_failed", path=str(path), error=str(exc))
        size = _file_size(path)
        return _result(
            compacted=False,
            reason="error",
            size=size,
            before=size,
            budget=budget,
            rows_deleted=0,
            tables={},
            age_rows_deleted=0,
            budget_rows_deleted=0,
            keep_floor=keep_floor,
            error=str(exc),
        )


def _compact_duckdb_file(
    path: Path,
    *,
    budget: int,
    min_keep: int,
) -> dict[str, object]:
    if not path.exists():
        return _result(
            compacted=False,
            reason="missing",
            size=0,
            before=0,
            budget=budget,
            rows_deleted=0,
            tables={},
            age_rows_deleted=0,
            budget_rows_deleted=0,
            keep_floor=min_keep,
        )

    before = path.stat().st_size
    if before <= budget:
        return _result(
            compacted=False,
            reason="within_limit",
            size=before,
            before=before,
            budget=budget,
            rows_deleted=0,
            tables={},
            age_rows_deleted=0,
            budget_rows_deleted=0,
            keep_floor=min_keep,
        )

    tables: _TableStats = {}
    error: str | None = None
    age_deleted = 0
    try:
        age_deleted = _age_pass(path, budget=budget, tables=tables)
    except Exception as exc:
        error = str(exc)
        logger.warning("duckdb_compact_age_failed", path=str(path), error=error)

    size = _file_size(path)
    budget_deleted = 0
    keep_floor = min_keep
    if size > budget:
        try:
            size, budget_deleted, keep_floor = _budget_pass(
                path,
                budget=budget,
                min_keep=min_keep,
                tables=tables,
            )
        except Exception as exc:
            error = str(exc)
            logger.warning("duckdb_compact_budget_failed", path=str(path), error=error)
            size = _file_size(path)

    rows_deleted = age_deleted + budget_deleted
    under_budget = size <= budget
    if error and rows_deleted == 0 and not under_budget:
        reason = "error"
    elif rows_deleted > 0 and under_budget:
        reason = "compacted"
    elif under_budget:
        reason = "within_limit"
    else:
        reason = "still_over_budget"
    return _result(
        compacted=rows_deleted > 0 and size < before,
        reason=reason,
        size=size,
        before=before,
        budget=budget,
        rows_deleted=rows_deleted,
        tables=tables,
        age_rows_deleted=age_deleted,
        budget_rows_deleted=budget_deleted,
        keep_floor=keep_floor,
        error=error if reason == "error" else None,
    )


def _result(
    *,
    compacted: bool,
    reason: str,
    size: int,
    before: int,
    budget: int,
    rows_deleted: int,
    tables: _TableStats,
    age_rows_deleted: int,
    budget_rows_deleted: int,
    keep_floor: int,
    error: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "compacted": compacted,
        "reason": reason,
        "bytes": size,
        "bytes_before": before,
        "max_bytes": budget,
        "rows_deleted": rows_deleted,
        "under_budget": size <= budget,
        "age_rows_deleted": age_rows_deleted,
        "budget_rows_deleted": budget_rows_deleted,
        "keep_floor": keep_floor,
        "tables": {name: dict(stats) for name, stats in sorted(tables.items())},
    }
    if error:
        payload["error"] = error
    return payload


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size if path.exists() else 0
    except OSError:
        return 0


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _is_temporal(data_type: str) -> bool:
    upper = data_type.upper()
    return "TIMESTAMP" in upper or upper == "DATE" or upper.startswith("DATE")


def _is_text(data_type: str) -> bool:
    upper = data_type.upper()
    return any(token in upper for token in ("CHAR", "VARCHAR", "TEXT", "STRING", "JSON", "BLOB"))


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


def _count(conn: duckdb.DuckDBPyConnection, quoted: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()
    return int(row[0]) if row else 0


def _record_delete(tables: _TableStats, table: str, before: int, after: int) -> int:
    removed = max(0, before - after)
    if removed <= 0:
        return 0
    entry = tables.setdefault(
        table,
        {"rows_before": before, "rows_deleted": 0, "rows_after": before},
    )
    entry["rows_deleted"] = int(entry["rows_deleted"]) + removed
    entry["rows_after"] = after
    return removed


def _age_pass(path: Path, *, budget: int, tables: _TableStats) -> int:
    deleted = 0
    size = path.stat().st_size
    for days in _AGE_CUTOFF_DAYS:
        if size <= budget:
            break
        removed = _delete_older_than(path, days=days, tables=tables)
        deleted += removed
        if removed <= 0:
            continue
        size = _reclaim_space(path)
    return deleted


def _delete_older_than(path: Path, *, days: int, tables: _TableStats) -> int:
    """Delete rows older than ``days``.

    A table whose newest row is already outside the window is left untouched.
    Price history that has not been committed for months is still the only
    history those radars have; the budget pass drops just enough of the
    oldest rows to fit the byte budget instead of emptying the file.
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
            before = _count(conn, quoted)
            try:
                conn.execute(
                    f"DELETE FROM {quoted} WHERE {expr} < ?",
                    [cutoff],
                )
            except duckdb.ConstraintException:
                continue
            deleted += _record_delete(tables, table, before, _count(conn, quoted))
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return deleted


def _budget_pass(
    path: Path,
    *,
    budget: int,
    min_keep: int,
    tables: _TableStats,
) -> tuple[int, int, int]:
    """Delete the oldest rows of the largest tables until ``path`` fits.

    The first attempts keep each table's latest day as well as its newest
    ``min_keep`` rows. If that set still exceeds the budget, the latest-day
    protection is lifted and ``min_keep`` is halved, down to one row. A table
    that still has rows is not emptied.
    """
    keep = max(1, min_keep)
    protect_latest_day = True
    deleted = 0
    size = path.stat().st_size
    for _ in range(_MAX_BUDGET_PASSES):
        if size <= budget:
            break
        removed = _delete_budget_chunk(
            path,
            budget=budget,
            min_keep=keep,
            protect_latest_day=protect_latest_day,
            tables=tables,
        )
        if removed <= 0:
            if protect_latest_day:
                protect_latest_day = False
                continue
            if keep > 1:
                keep = max(1, keep // 2)
                continue
            break
        deleted += removed
        size = _reclaim_space(path)
    return size, deleted, keep


def _delete_budget_chunk(
    path: Path,
    *,
    budget: int,
    min_keep: int,
    protect_latest_day: bool,
    tables: _TableStats,
) -> int:
    size = path.stat().st_size
    if size <= budget:
        return 0
    conn = _connect(path)
    deleted = 0
    try:
        ranked = _largest_tables(conn)
        total_rows = sum(count for _, count, _, _ in ranked)
        total_logical = sum(logical for _, _, logical, _ in ranked)
        scale = size / total_logical if total_logical > 0 else 1.0
        global_row_bytes = max(1, size // max(total_rows, 1))
        need = (size - budget) + _block_size(conn)
        for table, count, logical, columns in ranked:
            if need <= 0 or count <= 0:
                break
            row_bytes = max(global_row_bytes, int((logical / count) * scale) or 1)
            batch = max(1, (need + row_bytes - 1) // row_bytes)
            removed = _delete_oldest_rows(
                conn,
                table,
                columns,
                batch=batch,
                min_keep=min_keep,
                protect_latest_day=protect_latest_day,
                tables=tables,
            )
            if removed <= 0:
                continue
            deleted += removed
            need -= removed * row_bytes
        if deleted:
            conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return deleted


def _largest_tables(
    conn: duckdb.DuckDBPyConnection,
) -> list[tuple[str, int, int, dict[str, str]]]:
    """User tables ordered by estimated stored bytes, largest first."""
    ranked: list[tuple[int, str, int, int, dict[str, str]]] = []
    for table in _user_tables(conn):
        columns = _columns(conn, table)
        quoted = _quote_ident(table)
        try:
            count = _count(conn, quoted)
            logical = _logical_bytes(conn, quoted, columns, count)
        except duckdb.Error:
            continue
        ranked.append((logical, table, count, logical, columns))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [(table, count, logical, columns) for _, table, count, logical, columns in ranked]


def _logical_bytes(
    conn: duckdb.DuckDBPyConnection,
    quoted: str,
    columns: dict[str, str],
    count: int,
) -> int:
    if count <= 0:
        return 0
    terms = [f"{int(count)} * 32"]
    for name, dtype in columns.items():
        if not _is_text(dtype):
            continue
        terms.append(f"COALESCE(SUM(COALESCE(length({_quote_ident(name)}), 0)), 0)")
    try:
        row = conn.execute(f"SELECT {' + '.join(terms)} FROM {quoted}").fetchone()
    except duckdb.Error:
        return count * 64
    if row is None or row[0] is None:
        return count * 64
    return max(int(row[0]), count)


def _block_size(conn: duckdb.DuckDBPyConnection) -> int:
    try:
        row = conn.execute("SELECT block_size FROM pragma_database_size()").fetchone()
    except duckdb.Error:
        return 262_144
    if row is None or row[0] is None:
        return 262_144
    try:
        size = int(row[0])
    except (TypeError, ValueError):
        return 262_144
    return size if size > 0 else 262_144


def _keep_floor(
    conn: duckdb.DuckDBPyConnection,
    quoted: str,
    expr: str | None,
    *,
    count: int,
    min_keep: int,
    protect_latest_day: bool,
) -> int:
    floor = max(1, min_keep)
    if not protect_latest_day or expr is None or count <= floor:
        return floor
    latest = conn.execute(
        f"""
        SELECT COUNT(*) FROM {quoted}
        WHERE {expr} IS NOT NULL
          AND CAST({expr} AS DATE) = (
              SELECT MAX(CAST({expr} AS DATE))
              FROM {quoted}
              WHERE {expr} IS NOT NULL
          )
        """
    ).fetchone()
    latest_count = int(latest[0]) if latest else 0
    return max(floor, latest_count)


def _delete_oldest_rows(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    columns: dict[str, str],
    *,
    batch: int,
    min_keep: int,
    protect_latest_day: bool,
    tables: _TableStats,
) -> int:
    quoted = _quote_ident(table)
    before = _count(conn, quoted)
    expr = _time_expr(columns)
    try:
        floor = _keep_floor(
            conn,
            quoted,
            expr,
            count=before,
            min_keep=min_keep,
            protect_latest_day=protect_latest_day,
        )
    except duckdb.Error:
        floor = max(1, min_keep)
    if before <= floor:
        return 0
    limit = min(max(1, int(batch)), before - floor)
    if limit <= 0:
        return 0
    order = "rowid ASC" if expr is None else f"{expr} ASC NULLS FIRST, rowid ASC"
    try:
        conn.execute(
            f"""
            DELETE FROM {quoted}
            WHERE rowid IN (
                SELECT rowid FROM {quoted}
                ORDER BY {order}
                LIMIT {int(limit)}
            )
            """
        )
    except duckdb.ConstraintException:
        return 0
    except duckdb.Error:
        if not _delete_oldest_by_primary_key(conn, table, quoted, expr, limit):
            return 0
    return _record_delete(tables, table, before, _count(conn, quoted))


def _delete_oldest_by_primary_key(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    quoted: str,
    expr: str | None,
    limit: int,
) -> bool:
    key = _single_primary_key(conn, table)
    if key is None:
        return False
    key_sql = _quote_ident(key)
    order = "rowid ASC" if expr is None else f"{expr} ASC NULLS FIRST, {key_sql} ASC"
    try:
        conn.execute(
            f"""
            DELETE FROM {quoted}
            WHERE {key_sql} IN (
                SELECT {key_sql} FROM {quoted}
                ORDER BY {order}
                LIMIT {int(limit)}
            )
            """
        )
    except duckdb.Error:
        return False
    return True


def _single_primary_key(conn: duckdb.DuckDBPyConnection, table: str) -> str | None:
    try:
        rows = conn.execute(
            """
            SELECT constraint_column_names
            FROM duckdb_constraints()
            WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'
            """,
            [table],
        ).fetchall()
    except duckdb.Error:
        return None
    for row in rows:
        columns = row[0]
        if isinstance(columns, (list, tuple)) and len(columns) == 1 and columns[0]:
            return str(columns[0])
    return None


def _reclaim_space(path: Path) -> int:
    """CHECKPOINT, try VACUUM, then rewrite so DELETE frees disk space.

    DuckDB keeps deleted rows in the same file. VACUUM on current builds does
    not shrink it; exporting into a fresh file does.
    """
    conn: duckdb.DuckDBPyConnection | None = None
    try:
        conn = _connect(path)
        conn.execute("CHECKPOINT")
        try:
            conn.execute("VACUUM")
        except duckdb.Error:
            pass
        conn.execute("CHECKPOINT")
    except duckdb.Error as exc:
        logger.warning("duckdb_compact_checkpoint_failed", path=str(path), error=str(exc))
    finally:
        if conn is not None:
            conn.close()
    try:
        return _rewrite_inplace(path)
    except Exception as exc:
        logger.warning("duckdb_compact_rewrite_failed", path=str(path), error=str(exc))
        return _file_size(path)


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
