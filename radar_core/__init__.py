from __future__ import annotations

from .analyzer import apply_entity_rules
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
from .raw_logger import RawLogger
from .search_index import SearchIndex, SearchResult
from .storage import RadarStorage

__version__ = "0.2.0"

__all__ = [
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
]
