"""Rolling source-reliability scoring.

Each Radar already stores per-article rows with ``source``, ``fetch_status``,
``collected_at`` and ``fetched_at``. This module computes a 30-day rolling
reliability snapshot per source that downstream dashboards or workflows can
publish:

- ``success_rate``  : fraction of fetches with status not in {"error", "timeout"}.
- ``article_count`` : total successful fetches.
- ``last_seen``     : last successful collected_at.
- ``staleness_days``: days since last_seen.
- ``score``         : 0..100 composite (70% success_rate, 30% recency).

Designed to run over a DuckDB connection that exposes the standard
``articles`` table.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Optional


@dataclass(frozen=True)
class SourceReliability:
    source: str
    article_count: int
    success_rate: float
    last_seen: Optional[datetime]
    staleness_days: float
    score: float


def _coerce_dt(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    return None


def compute_source_reliability(
    rows: Iterable[dict[str, object]],
    *,
    today: Optional[datetime] = None,
) -> list[SourceReliability]:
    """Aggregate a list of article rows into per-source reliability records.

    Each row must expose ``source`` (str), ``fetch_status`` (str | None) and
    ``collected_at`` (datetime | str | None). Missing fetch_status counts as
    success (legacy rows).
    """
    now = (today or datetime.now(UTC)).astimezone(UTC)
    by_source: dict[str, dict[str, object]] = {}

    for row in rows:
        src = row.get("source")
        if not isinstance(src, str) or not src:
            continue
        bucket = by_source.setdefault(
            src,
            {"total": 0, "errors": 0, "last_seen": None},
        )
        bucket["total"] = int(bucket["total"]) + 1  # type: ignore[arg-type]
        status = row.get("fetch_status")
        if isinstance(status, str) and status.lower() in {"error", "timeout", "failure"}:
            bucket["errors"] = int(bucket["errors"]) + 1  # type: ignore[arg-type]
        else:
            collected = _coerce_dt(row.get("collected_at"))
            if collected is not None:
                prev = bucket.get("last_seen")
                if not isinstance(prev, datetime) or collected > prev:
                    bucket["last_seen"] = collected

    out: list[SourceReliability] = []
    for src, bucket in by_source.items():
        total = int(bucket["total"])  # type: ignore[arg-type]
        errors = int(bucket["errors"])  # type: ignore[arg-type]
        last_seen = bucket["last_seen"] if isinstance(bucket["last_seen"], datetime) else None
        success_rate = ((total - errors) / total) if total else 0.0
        if last_seen is None:
            staleness = float("inf")
            recency = 0.0
        else:
            delta = (now - last_seen).total_seconds() / 86400.0
            staleness = max(0.0, delta)
            # Linear decay from 1.0 at 0 days to 0.0 at 14 days, then capped.
            recency = max(0.0, 1.0 - min(staleness, 14.0) / 14.0)
        score = 100.0 * (0.7 * success_rate + 0.3 * recency)
        out.append(
            SourceReliability(
                source=src,
                article_count=total - errors,
                success_rate=round(success_rate, 4),
                last_seen=last_seen,
                staleness_days=round(staleness, 2) if staleness != float("inf") else float("inf"),
                score=round(score, 2),
            )
        )
    out.sort(key=lambda r: r.score, reverse=True)
    return out


def reliability_to_dict(records: Iterable[SourceReliability]) -> list[dict[str, object]]:
    """Plain-dict export so callers can JSON-serialize the result."""
    out: list[dict[str, object]] = []
    for r in records:
        out.append(
            {
                "source": r.source,
                "article_count": r.article_count,
                "success_rate": r.success_rate,
                "last_seen": r.last_seen.isoformat() if r.last_seen else None,
                "staleness_days": (
                    r.staleness_days if r.staleness_days != float("inf") else None
                ),
                "score": r.score,
            }
        )
    return out


__all__ = [
    "SourceReliability",
    "compute_source_reliability",
    "reliability_to_dict",
]


def select_recent_rows(
    conn,  # type: ignore[no-untyped-def]
    *,
    days: int = 30,
    table: str = "articles",
) -> list[dict[str, object]]:
    """Convenience helper: pull the last `days` of article rows out of a
    DuckDB connection. Kept untyped so radar-core doesn't import duckdb at
    module load."""
    cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat(sep=" ")
    cursor = conn.execute(
        f"SELECT source, fetch_status, collected_at FROM {table} "
        "WHERE collected_at >= ?",
        [cutoff],
    )
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]
