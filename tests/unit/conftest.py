from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest


@pytest.fixture
def sample_source() -> object:
    from radar_core.models import Source

    return Source(name="Example RSS", type="rss", url="https://example.com/feed.xml")


@pytest.fixture
def sample_entity() -> object:
    from radar_core.models import EntityDefinition

    return EntityDefinition(
        name="topic", display_name="Topic", keywords=["ai", "cloud", "python"]
    )


@pytest.fixture
def sample_article() -> object:
    from radar_core.models import Article

    return Article(
        title="AI and cloud market update",
        link="https://example.com/article-1",
        summary="Python tooling and AI adoption continue to grow.",
        published=datetime(2026, 3, 10, 9, 0, tzinfo=timezone.utc),
        source="Example RSS",
        category="tech",
        matched_entities={"topic": ["ai", "cloud", "python"]},
    )


@pytest.fixture
def tmp_duckdb(tmp_path: Path) -> Path:
    return tmp_path / "test_radar_data.duckdb"


@pytest.fixture
def tmp_search_db(tmp_path: Path) -> Path:
    return tmp_path / "test_search_index.db"


@pytest.fixture
def tmp_storage(tmp_duckdb: Path) -> object:
    from radar_core.storage import RadarStorage

    storage = RadarStorage(tmp_duckdb)
    try:
        yield storage
    finally:
        storage.close()


@pytest.fixture
def tmp_search_index(tmp_search_db: Path) -> object:
    from radar_core.search_index import SearchIndex

    index = SearchIndex(tmp_search_db)
    try:
        yield index
    finally:
        index.close()
