from __future__ import annotations

from .adaptive_throttle import AdaptiveThrottler, SourceThrottleState
from .analyzer import apply_entity_rules
from .config_loader import load_category_config, load_notification_config, load_settings
from .collector import RateLimiter, collect_sources
from .reddit_collector import collect_reddit_sources, collect_reddit_source
from .crawl_health import CrawlHealthRecord, CrawlHealthStore
from .exceptions import (
    NetworkError,
    NotificationError,
    ParseError,
    ReportError,
    SearchError,
    SourceError,
    StorageError,
)
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
from .nl_query import ParsedQuery, parse_query
from .notifier import (
    CompositeNotifier,
    EmailNotifier,
    NotificationPayload,
    Notifier,
    WebhookNotifier,
)
from .raw_logger import RawLogger
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

__version__ = "0.4.0"

__all__ = [
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
    "configure_logging",
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
    "get_logger",
    "Html2TextExtractor",
    "JinaExtractor",
    "load_category_config",
    "load_notification_config",
    "load_settings",
    "NetworkError",
    "Notifier",
    "NotificationConfig",
    "NotificationError",
    "NotificationPayload",
    "ParsedQuery",
    "parse_query",
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
    "TrafilaturaExtractor",
    "URLExtractor",
    "URLExtractorChain",
    "load_runtime_contract",
    "WebhookConfig",
    "WebhookNotifier",
]
