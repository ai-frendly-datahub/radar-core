from __future__ import annotations

from .analyzer import apply_entity_rules
from .config_loader import load_category_config, load_notification_config, load_settings
from .collector import RateLimiter, collect_sources
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
    EmailSettings,
    EntityDefinition,
    NotificationConfig,
    RadarSettings,
    Source,
    TelegramSettings,
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

__version__ = "0.2.0"

__all__ = [
    "__version__",
    "apply_entity_rules",
    "Article",
    "CategoryConfig",
    "CompositeNotifier",
    "configure_logging",
    "collect_sources",
    "EmailSettings",
    "EmailNotifier",
    "EntityDefinition",
    "get_logger",
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
    "ReportError",
    "SearchError",
    "SearchIndex",
    "SearchResult",
    "Source",
    "SourceError",
    "StorageError",
    "TelegramSettings",
    "WebhookNotifier",
]
