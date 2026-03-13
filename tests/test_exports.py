from __future__ import annotations

import radar_core


def test_version_is_0_2_0() -> None:
    assert radar_core.__version__ == "0.2.0"


def test_all_models_importable_from_package() -> None:
    from radar_core import (
        Article,
        CategoryConfig,
        EmailSettings,
        EntityDefinition,
        NotificationConfig,
        RadarSettings,
        Source,
        TelegramSettings,
    )

    assert isinstance(Article, type)
    assert isinstance(CategoryConfig, type)
    assert isinstance(EmailSettings, type)
    assert isinstance(EntityDefinition, type)
    assert isinstance(NotificationConfig, type)
    assert isinstance(RadarSettings, type)
    assert isinstance(Source, type)
    assert isinstance(TelegramSettings, type)


def test_all_exceptions_importable_from_package() -> None:
    from radar_core import (
        NetworkError,
        NotificationError,
        ParseError,
        ReportError,
        SearchError,
        SourceError,
        StorageError,
    )

    assert issubclass(NetworkError, Exception)
    assert issubclass(NotificationError, Exception)
    assert issubclass(ParseError, Exception)
    assert issubclass(ReportError, Exception)
    assert issubclass(SearchError, Exception)
    assert issubclass(SourceError, Exception)
    assert issubclass(StorageError, Exception)


def test_all_exports_match_public_api() -> None:
    expected_exports = {
        "__version__",
        "apply_entity_rules",
        "Article",
        "CategoryConfig",
        "collect_sources",
        "EmailSettings",
        "EntityDefinition",
        "NetworkError",
        "NotificationConfig",
        "NotificationError",
        "ParseError",
        "RadarSettings",
        "RadarStorage",
        "RateLimiter",
        "RawLogger",
        "ReportError",
        "SearchError",
        "SearchIndex",
        "SearchResult",
        "Source",
        "SourceError",
        "StorageError",
        "TelegramSettings",
    }

    assert set(radar_core.__all__) == expected_exports
