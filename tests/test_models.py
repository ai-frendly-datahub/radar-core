"""Tests for radar_core.models — dataclass creation, defaults, serialization."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from radar_core.models import (
    Article,
    CategoryConfig,
    CrawlHealthAlert,
    EmailSettings,
    EntityDefinition,
    NotificationConfig,
    RadarSettings,
    Source,
    TelegramSettings,
)


# ── Tests ─────────────────────────────────────────────────────────────────


def test_article_defaults() -> None:
    """Article 기본값 확인 — matched_entities 빈 dict, collected_at None."""
    article = Article(
        title="Test",
        link="https://example.com/1",
        summary="Summary",
        published=datetime(2026, 1, 1, tzinfo=UTC),
        source="Src",
        category="cat",
    )

    assert article.matched_entities == {}
    assert article.collected_at is None


def test_article_matched_entities_default_empty() -> None:
    """matched_entities 기본값이 빈 dict이고 인스턴스 간 공유되지 않음."""
    a1 = Article(
        title="A", link="a", summary="", published=None, source="s", category="c"
    )
    a2 = Article(
        title="B", link="b", summary="", published=None, source="s", category="c"
    )

    a1.matched_entities["test"] = ["kw"]

    assert a2.matched_entities == {}  # 다른 인스턴스에 영향 없음


def test_article_with_all_fields() -> None:
    """모든 필드를 명시적으로 설정한 Article."""
    now = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
    article = Article(
        title="Full",
        link="https://example.com/full",
        summary="Full summary",
        published=now,
        source="FullSource",
        category="full_cat",
        matched_entities={"Entity": ["kw1", "kw2"]},
        collected_at=now,
    )

    assert article.title == "Full"
    assert article.link == "https://example.com/full"
    assert article.summary == "Full summary"
    assert article.published == now
    assert article.source == "FullSource"
    assert article.category == "full_cat"
    assert article.matched_entities == {"Entity": ["kw1", "kw2"]}
    assert article.collected_at == now


def test_category_config_creation() -> None:
    """CategoryConfig 생성 및 필드 확인."""
    sources = [Source(name="RSS", type="rss", url="https://example.com/feed")]
    entities = [
        EntityDefinition(name="topic", display_name="Topic", keywords=["ai", "ml"])
    ]
    config = CategoryConfig(
        category_name="tech",
        display_name="Tech Radar",
        sources=sources,
        entities=entities,
    )

    assert config.category_name == "tech"
    assert config.display_name == "Tech Radar"
    assert len(config.sources) == 1
    assert len(config.entities) == 1
    assert config.sources[0].name == "RSS"
    assert config.entities[0].keywords == ["ai", "ml"]


def test_source_creation() -> None:
    """Source 데이터클래스 생성."""
    source = Source(name="IGN", type="rss", url="https://ign.com/feed.xml")

    assert source.name == "IGN"
    assert source.type == "rss"
    assert source.url == "https://ign.com/feed.xml"


def test_entity_definition_creation() -> None:
    """EntityDefinition 생성 및 keywords 리스트 확인."""
    entity = EntityDefinition(
        name="nintendo",
        display_name="Nintendo",
        keywords=["닌텐도", "nintendo", "switch"],
    )

    assert entity.name == "nintendo"
    assert entity.display_name == "Nintendo"
    assert len(entity.keywords) == 3
    assert "닌텐도" in entity.keywords


def test_crawl_health_alert_creation() -> None:
    """CrawlHealthAlert 데이터클래스 생성."""
    now = datetime(2026, 3, 15, tzinfo=UTC)
    alert = CrawlHealthAlert(
        source_name="broken_rss",
        failure_count=5,
        last_error="Connection timeout",
        disabled_at=now,
    )

    assert alert.source_name == "broken_rss"
    assert alert.failure_count == 5
    assert alert.last_error == "Connection timeout"
    assert alert.disabled_at == now


def test_radar_settings_paths() -> None:
    """RadarSettings에 Path 객체 저장."""
    settings = RadarSettings(
        database_path=Path("data/radar.duckdb"),
        report_dir=Path("reports"),
        raw_data_dir=Path("data/raw"),
        search_db_path=Path("data/search.db"),
    )

    assert isinstance(settings.database_path, Path)
    assert settings.report_dir == Path("reports")


def test_notification_config_defaults() -> None:
    """NotificationConfig 선택 필드 기본값."""
    config = NotificationConfig(enabled=False, channels=[])

    assert config.email is None
    assert config.webhook_url is None
    assert config.telegram is None
    assert config.rules == {}


def test_notification_config_with_all_channels() -> None:
    """모든 알림 채널이 설정된 NotificationConfig."""
    email = EmailSettings(
        smtp_host="smtp.example.com",
        smtp_port=587,
        username="user",
        password="pass",
        from_address="from@ex.com",
        to_addresses=["to@ex.com"],
    )
    telegram = TelegramSettings(bot_token="token", chat_id="123")
    config = NotificationConfig(
        enabled=True,
        channels=["email", "telegram", "webhook"],
        email=email,
        telegram=telegram,
        webhook_url="https://hooks.example.com/notify",
        rules={"min_score": 5},
    )

    assert config.enabled is True
    assert len(config.channels) == 3
    assert config.email is not None
    assert config.telegram is not None
    assert config.webhook_url == "https://hooks.example.com/notify"
    assert config.rules == {"min_score": 5}


def test_email_settings_fields() -> None:
    """EmailSettings 필드 정확성."""
    email = EmailSettings(
        smtp_host="smtp.gmail.com",
        smtp_port=465,
        username="test@gmail.com",
        password="secret",
        from_address="test@gmail.com",
        to_addresses=["a@b.com", "c@d.com"],
    )

    assert email.smtp_port == 465
    assert len(email.to_addresses) == 2
