from __future__ import annotations

import radar_core


def test_version_is_pinned() -> None:
    assert radar_core.__version__ == "0.4.1"


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
        "AdaptiveThrottler",
        "apply_entity_rules",
        "Article",
        "annotate_articles_with_ontology",
        "backfill_duckdb_ontology",
        "build_article_ontology_metadata",
        "build_event_model_payload",
        "build_summary_ontology_metadata",
        "get_event_model_field_spec",
        "CategoryConfig",
        "CompositeNotifier",
        "compact_duckdb_file",
        "configure_logging",
        "DAILY_PARTITION_TIMEZONE",
        "daily_partition_date",
        "DEFAULT_DUCKDB_MAX_BYTES",
        "GITHUB_FILE_LIMIT_BYTES",
        "collect_sources",
        "collect_reddit_source",
        "collect_reddit_sources",
        "CrawlHealthAlert",
        "CrawlHealthRecord",
        "CrawlHealthStore",
        "EmailConfig",
        "EmailSettings",
        "EmailNotifier",
        "EntityDefinition",
        "EnumValueError",
        "ExtractedContent",
        "extract_url_content",
        "extract_url_content_safe",
        "filter_sources",
        "get_logger",
        "Html2TextExtractor",
        "JinaExtractor",
        "json_default",
        "json_dumps",
        "load_category_config",
        "load_notification_config",
        "load_settings",
        "NetworkError",
        "Notifier",
        "NotificationConfig",
        "NotificationError",
        "NotificationPayload",
        "ParsedQuery",
        "parse_feed_entry",
        "parse_html_published",
        "parse_published",
        "parse_query",
        "parse_unix_timestamp",
        "resolve_default_timezone",
        "ParseError",
        "RadarSettings",
        "RadarStorage",
        "RateLimiter",
        "RawLogger",
        "ReadabilityExtractor",
        "ReportError",
        "SearchError",
        "SearchIndex",
        "SearchResult",
        "Source",
        "SourceError",
        "SourceThrottleState",
        "StandardNotificationConfig",
        "StorageError",
        "TelegramNotifier",
        "TelegramSettings",
        "to_utc_iso",
        "TrafilaturaExtractor",
        "URLExtractor",
        "URLExtractorChain",
        "load_runtime_contract",
        "WebhookConfig",
        "WebhookNotifier",
        "RunHealthVerdict",
        "build_run_health",
        "count_stored_items",
        "evaluate_run_health",
        "merge_run_health",
        "write_run_health",
    }

    assert set(radar_core.__all__) == expected_exports
