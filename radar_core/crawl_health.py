from __future__ import annotations

import math
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Iterable, cast

import duckdb

from .exceptions import StorageError
from .migration import migrate


# Transient HTTP failures that any caller may probe after the 24h minimum cooldown.
TRANSIENT_RETRY_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
# Access-block statuses a caller must opt into explicitly. 403 can be a temporary
# bot/IP block, but it can also be a policy decision, so it needs a long cooldown.
OPT_IN_RETRY_STATUSES = frozenset({403})
ACCESS_BLOCK_MIN_COOLDOWN_SECONDS = 7 * 86400
_ERROR_STATUS = re.compile(r"^(\d{3}) (?:Client|Server) Error:")


@dataclass
class CrawlHealthRecord:
    source_name: str
    success_count: int
    failure_count: int
    current_delay: float
    last_error: str | None
    disabled: bool
    updated_at: datetime | None


@dataclass
class _HealthUpdate:
    source_name: str
    success_delta: int
    failure_delta: int
    current_delay: float
    last_error: str | None


class CrawlHealthStore:
    def __init__(
        self,
        db_path: str,
        batch_size: int = 1000,
        failure_threshold: int = 10,
    ):
        self.db_path: Path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.batch_size: int = batch_size
        self.failure_threshold: int = failure_threshold
        self.conn: duckdb.DuckDBPyConnection = self._safe_connect(self.db_path)

        self._buffer: list[_HealthUpdate] = []
        self._buffer_lock = Lock()
        self._write_lock = Lock()

        self._ensure_tables()

    @staticmethod
    def _safe_connect(db_path: Path) -> duckdb.DuckDBPyConnection:
        """Open a DuckDB connection, quarantining a corrupt WAL on failure.

        Strategy:
            1. Try ``duckdb.connect(str(db_path))`` once.
            2. On ``duckdb.Error`` / ``IOError`` / ``OSError``, look for
               ``<db_path>.wal``. If present, rename it to
               ``<db_path>.wal.broken-<UTC>`` (never delete; operators may
               want to inspect it). Then retry ``duckdb.connect`` exactly once.
            3. If the second attempt also fails, the original exception is
               re-raised so the caller sees the unfixed failure.

        The happy path performs no extra IO beyond the first connect call.
        """
        try:
            return duckdb.connect(str(db_path))
        except (duckdb.Error, IOError, OSError) as exc:
            wal_path = Path(str(db_path) + ".wal")
            if not wal_path.exists():
                raise
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            quarantine_path = wal_path.with_name(f"{wal_path.name}.broken-{timestamp}")
            try:
                wal_path.rename(quarantine_path)
            except OSError:
                # Quarantine failed; surface the original DuckDB exception so
                # the operator can investigate without touching the WAL.
                raise exc
            print(
                f"[radar_core.crawl_health] WAL quarantined: {wal_path} -> "
                f"{quarantine_path} ({exc})",
                file=sys.stderr,
            )
            try:
                return duckdb.connect(str(db_path))
            except (duckdb.Error, IOError, OSError):
                # Re-raise the ORIGINAL connect failure so the operator
                # sees the root cause that triggered quarantine.
                raise exc

    def _ensure_tables(self) -> None:
        _ = migrate(self.conn)

    def close(self) -> None:
        self.flush()
        self.conn.close()

    def __enter__(self) -> CrawlHealthStore:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()

    def record_success(self, source_name: str, delay: float) -> None:
        self._enqueue(
            _HealthUpdate(
                source_name=source_name,
                success_delta=1,
                failure_delta=0,
                current_delay=delay,
                last_error=None,
            )
        )

    def record_failure(self, source_name: str, error: str, delay: float) -> None:
        self._enqueue(
            _HealthUpdate(
                source_name=source_name,
                success_delta=0,
                failure_delta=1,
                current_delay=delay,
                last_error=error,
            )
        )

    def _enqueue(self, update: _HealthUpdate) -> None:
        should_flush = False
        with self._buffer_lock:
            self._buffer.append(update)
            if len(self._buffer) >= self.batch_size:
                should_flush = True
        if should_flush:
            self.flush()

    def flush(self) -> None:
        with self._buffer_lock:
            pending = self._buffer
            self._buffer = []

        if not pending:
            return

        rows: list[tuple[object, ...]] = [
            (
                update.source_name,
                update.success_delta,
                update.failure_delta,
                update.current_delay,
                update.last_error,
            )
            for update in pending
        ]

        try:
            with self._write_lock:
                _ = self.conn.begin()
                _ = self.conn.executemany(
                    """
                    INSERT INTO crawl_health (
                        source_name,
                        success_count,
                        failure_count,
                        current_delay,
                        last_error,
                        disabled,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        CASE WHEN ? >= ? THEN TRUE ELSE FALSE END,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT(source_name) DO UPDATE SET
                        success_count = crawl_health.success_count + EXCLUDED.success_count,
                        failure_count = crawl_health.failure_count + EXCLUDED.failure_count,
                        current_delay = EXCLUDED.current_delay,
                        last_error = CASE
                            WHEN EXCLUDED.last_error IS NULL THEN crawl_health.last_error
                            ELSE EXCLUDED.last_error
                        END,
                        disabled = CASE
                            WHEN crawl_health.disabled THEN TRUE
                            WHEN EXCLUDED.failure_count > 0
                                AND crawl_health.failure_count + EXCLUDED.failure_count >= ? THEN TRUE
                            ELSE EXCLUDED.disabled
                        END,
                        updated_at = NOW()
                    """,
                    [
                        (
                            source_name,
                            success_delta,
                            failure_delta,
                            current_delay,
                            last_error,
                            failure_delta,
                            self.failure_threshold,
                            self.failure_threshold,
                        )
                        for (
                            source_name,
                            success_delta,
                            failure_delta,
                            current_delay,
                            last_error,
                        ) in rows
                    ],
                )
                _ = self.conn.commit()
        except Exception as exc:
            try:
                _ = self.conn.rollback()
            except duckdb.Error:
                pass
            with self._buffer_lock:
                self._buffer = pending + self._buffer
            raise StorageError("Failed to flush crawl health updates") from exc

    def get_health(self, source_name: str) -> CrawlHealthRecord | None:
        self.flush()
        try:
            with self._write_lock:
                row = cast(
                    tuple[object, ...] | None,
                    self.conn.execute(
                        """
                        SELECT
                            source_name,
                            success_count,
                            failure_count,
                            current_delay,
                            last_error,
                            disabled,
                            updated_at
                        FROM crawl_health
                        WHERE source_name = ?
                        """,
                        [source_name],
                    ).fetchone(),
                )
        except Exception as exc:
            raise StorageError("Failed to read crawl health") from exc

        if not row:
            return None

        return CrawlHealthRecord(
            source_name=str(row[0]),
            success_count=int(cast(int, row[1])),
            failure_count=int(cast(int, row[2])),
            current_delay=float(cast(float, row[3])),
            last_error=str(row[4]) if row[4] is not None else None,
            disabled=bool(row[5]),
            updated_at=cast(datetime | None, row[6]),
        )

    def is_disabled(self, source_name: str) -> bool:
        record = self.get_health(source_name)
        if record is None:
            return False
        return record.disabled

    def claim_retry(
        self,
        source_name: str,
        cooldown_seconds: float = 86400,
        *,
        retry_statuses: Iterable[int] | None = None,
    ) -> bool:
        """Reserve one caller-managed probe for a disabled transient HTTP source.

        Opt-in only: ``is_disabled`` retains its existing meaning. The caller
        must limit transport retries and call ``finish_retry`` only after valid
        nonempty content. A claim stays disabled and advances ``updated_at``,
        so failures, empty results and crashed probes wait another cooldown.
        Unknown errors, 401, 404/410 and missing timestamps fail closed.

        By default only ``TRANSIENT_RETRY_STATUSES`` are probed. A caller may
        additionally pass ``retry_statuses=[403]`` for a source whose 403 is a
        suspected temporary access block; that requires ``cooldown_seconds`` of
        at least 7 days, so a still-blocked source costs one request per week.
        Any other extra status raises ``ValueError``.
        """
        if not math.isfinite(cooldown_seconds) or cooldown_seconds < 86400:
            raise ValueError("crawl health cooldown must be at least 86400 seconds")
        extra = frozenset(retry_statuses or ())
        if not extra <= OPT_IN_RETRY_STATUSES:
            raise ValueError("retry_statuses may only add " + ", ".join(
                str(status) for status in sorted(OPT_IN_RETRY_STATUSES)
            ))
        if extra and cooldown_seconds < ACCESS_BLOCK_MIN_COOLDOWN_SECONDS:
            raise ValueError("access-block retry needs a cooldown of at least 604800 seconds")
        allowed = TRANSIENT_RETRY_STATUSES | extra
        self.flush()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        cutoff = now - timedelta(seconds=cooldown_seconds)
        try:
            with self._write_lock:
                row = self.conn.execute(
                    "SELECT last_error FROM crawl_health WHERE source_name = ?",
                    [source_name],
                ).fetchone()
                match = _ERROR_STATUS.match(str(row[0] or "")) if row else None
                if match is None or int(match.group(1)) not in allowed:
                    return False
                claimed = self.conn.execute(
                    """
                    UPDATE crawl_health SET updated_at = ?
                    WHERE source_name = ? AND disabled = TRUE
                        AND updated_at <= ?
                        AND last_error = ?
                    RETURNING source_name
                    """,
                    [now, source_name, cutoff, row[0]],
                ).fetchone()
                return claimed is not None
        except Exception as exc:
            raise StorageError("Failed to claim crawl health retry") from exc

    def finish_retry(self, source_name: str) -> None:
        """Re-enable after a claimed probe yields valid nonempty content.

        Historical success/failure counts and last_error are retained. Since
        the failure budget is already exhausted, the next failure disables the
        source again immediately; successful fetches do not disable it again.
        """
        self.flush()
        try:
            with self._write_lock:
                self.conn.execute(
                    "UPDATE crawl_health SET disabled = FALSE WHERE source_name = ?",
                    [source_name],
                )
        except Exception as exc:
            raise StorageError("Failed to finish crawl health retry") from exc

    def postpone_retry(self, source_name: str, retry_at: datetime) -> None:
        """Honor a probe's Retry-After conservatively, plus the next cooldown."""
        if retry_at.tzinfo is None:
            raise ValueError("retry_at must be timezone-aware")
        self.flush()
        retry_at = retry_at.astimezone(timezone.utc).replace(tzinfo=None)
        try:
            with self._write_lock:
                self.conn.execute(
                    """
                    UPDATE crawl_health SET updated_at = GREATEST(updated_at, ?)
                    WHERE source_name = ? AND disabled = TRUE
                    """,
                    [retry_at, source_name],
                )
        except Exception as exc:
            raise StorageError("Failed to postpone crawl health retry") from exc
