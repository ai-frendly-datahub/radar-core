"""Callers may pass their own article model without ``published_raw``."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from radar_core.raw_logger import RawLogger
from radar_core.storage import RadarStorage


@dataclass
class _LegacyArticle:
    """Minimal stand-in for models such as PaperRadar's Paper."""

    title: str
    link: str
    summary: str
    published: datetime | None
    source: str
    category: str
    matched_entities: dict[str, list[str]]
    ontology: dict[str, object]


def test_upsert_and_raw_log_without_published_raw(tmp_path: Path) -> None:
    article = _LegacyArticle(
        title="논문",
        link="https://example.com/paper",
        summary="요약",
        published=datetime(2026, 10, 29, 0, 0, tzinfo=UTC),
        source="arxiv",
        category="paper",
        matched_entities={},
        ontology={},
    )
    assert not hasattr(article, "published_raw")

    storage = RadarStorage(tmp_path / "radar.duckdb")
    try:
        storage.upsert_articles([article])
        row = storage.conn.execute(
            "SELECT published_raw FROM articles WHERE link = ?",
            [article.link],
        ).fetchone()
    finally:
        storage.close()

    assert row is not None
    assert row[0] is None

    output = RawLogger(tmp_path / "raw").log([article], source_name="arxiv")
    record = json.loads(output.read_text(encoding="utf-8").splitlines()[0])
    assert record["published_raw"] is None
