from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import cast

import duckdb

from .exceptions import StorageError
from .migration import migrate


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
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(str(self.db_path))

        self._buffer: list[_HealthUpdate] = []
        self._buffer_lock = Lock()
        self._write_lock = Lock()

        self._ensure_tables()

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
                            WHEN crawl_health.failure_count + EXCLUDED.failure_count >= ? THEN TRUE
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
