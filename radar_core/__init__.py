from __future__ import annotations

from .adaptive_throttle import AdaptiveThrottler, SourceThrottleState
from .analyzer import apply_entity_rules
from .collector import RateLimiter, collect_sources
from .config_loader import (
    filter_sources,
    load_category_config,
    load_notification_config,
    load_settings,
)
from .crawl_health import CrawlHealthRecord, CrawlHealthStore
from .dates import (
    DAILY_PARTITION_TIMEZONE,
    daily_partition_date,
    parse_feed_entry,
    parse_html_published,
    parse_published,
    parse_unix_timestamp,
    resolve_default_timezone,
    to_utc_iso,
)
from .db_compact import (
    DEFAULT_DUCKDB_MAX_BYTES,
    GITHUB_FILE_LIMIT_BYTES,
    compact_duckdb_file,
)
from .exceptions import (
    NetworkError,
    NotificationError,
    ParseError,
    ReportError,
    SearchError,
    SourceError,
    StorageError,
)
from .health import (
    RunHealthVerdict,
    build_run_health,
    count_stored_items,
    evaluate_run_health,
    merge_run_health,
    write_run_health,
)
from .jsonutil import json_default, json_dumps
from .logger import configure_logging, get_logger
from .models import (
    Article,
    CategoryConfig,
    CrawlHealthAlert,
    EmailConfig,
    EmailSettings,
    EntityDefinition,
    NotificationConfig,
    RadarSettings,
    Source,
    StandardNotificationConfig,
    TelegramSettings,
    WebhookConfig,
)
from .nl_query import ParsedQuery, parse_query
from .notifier import (
    CompositeNotifier,
    EmailNotifier,
    NotificationPayload,
    Notifier,
    WebhookNotifier,
)
from .ontology import (
    EnumValueError,
    annotate_articles_with_ontology,
    backfill_duckdb_ontology,
    build_article_ontology_metadata,
    build_event_model_payload,
    build_summary_ontology_metadata,
    get_event_model_field_spec,
    load_runtime_contract,
)
from .raw_logger import RawLogger
from .reddit_collector import collect_reddit_source, collect_reddit_sources
from .search_index import SearchIndex, SearchResult
from .storage import RadarStorage
from .telegram_notifier import TelegramNotifier
from .url_extractor import (
    ExtractedContent,
    Html2TextExtractor,
    JinaExtractor,
    ReadabilityExtractor,
    TrafilaturaExtractor,
    URLExtractor,
    URLExtractorChain,
    extract_url_content,
    extract_url_content_safe,
)

__version__ = "0.4.1"

__all__ = [
    "DAILY_PARTITION_TIMEZONE",
    "DEFAULT_DUCKDB_MAX_BYTES",
    "GITHUB_FILE_LIMIT_BYTES",
    "AdaptiveThrottler",
    "Article",
    "CategoryConfig",
    "CompositeNotifier",
    "CrawlHealthAlert",
    "CrawlHealthRecord",
    "CrawlHealthStore",
    "EmailConfig",
    "EmailNotifier",
    "EmailSettings",
    "EntityDefinition",
    "EnumValueError",
    "ExtractedContent",
    "Html2TextExtractor",
    "JinaExtractor",
    "NetworkError",
    "NotificationConfig",
    "NotificationError",
    "NotificationPayload",
    "Notifier",
    "ParseError",
    "ParsedQuery",
    "RadarSettings",
    "RadarStorage",
    "RateLimiter",
    "RawLogger",
    "ReadabilityExtractor",
    "ReportError",
    "RunHealthVerdict",
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
    "TrafilaturaExtractor",
    "URLExtractor",
    "URLExtractorChain",
    "WebhookConfig",
    "WebhookNotifier",
    "__version__",
    "annotate_articles_with_ontology",
    "apply_entity_rules",
    "backfill_duckdb_ontology",
    "build_article_ontology_metadata",
    "build_event_model_payload",
    "build_run_health",
    "build_summary_ontology_metadata",
    "collect_reddit_source",
    "collect_reddit_sources",
    "collect_sources",
    "compact_duckdb_file",
    "configure_logging",
    "count_stored_items",
    "daily_partition_date",
    "evaluate_run_health",
    "extract_url_content",
    "extract_url_content_safe",
    "filter_sources",
    "get_event_model_field_spec",
    "get_logger",
    "json_default",
    "json_dumps",
    "load_category_config",
    "load_notification_config",
    "load_runtime_contract",
    "load_settings",
    "merge_run_health",
    "parse_feed_entry",
    "parse_html_published",
    "parse_published",
    "parse_query",
    "parse_unix_timestamp",
    "resolve_default_timezone",
    "to_utc_iso",
    "write_run_health",
]
