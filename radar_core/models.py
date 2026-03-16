from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class Source:
    name: str
    type: str
    url: str


@dataclass
class EntityDefinition:
    name: str
    display_name: str
    keywords: list[str]


@dataclass
class Article:
    title: str
    link: str
    summary: str
    published: datetime | None
    source: str
    category: str
    matched_entities: dict[str, list[str]] = field(default_factory=dict)
    collected_at: datetime | None = None


@dataclass
class CategoryConfig:
    category_name: str
    display_name: str
    sources: list[Source]
    entities: list[EntityDefinition]


@dataclass
class RadarSettings:
    database_path: Path
    report_dir: Path
    raw_data_dir: Path
    search_db_path: Path


@dataclass
class EmailSettings:
    smtp_host: str
    smtp_port: int
    username: str
    password: str
    from_address: str
    to_addresses: list[str]


@dataclass
class TelegramSettings:
    bot_token: str
    chat_id: str


@dataclass
class CrawlHealthAlert:
    source_name: str
    failure_count: int
    last_error: str | None
    disabled_at: datetime


@dataclass
class NotificationConfig:
    enabled: bool
    channels: list[str]
    email: EmailSettings | None = None
    webhook_url: str | None = None
    telegram: TelegramSettings | None = None
    rules: dict[str, object] = field(default_factory=dict)


@dataclass
class EmailConfig:
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    from_addr: str = ""
    to_addrs: list[str] = field(default_factory=list)


@dataclass
class WebhookConfig:
    enabled: bool = False
    url: str = ""
    method: str = "POST"
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class StandardNotificationConfig:
    enabled: bool = False
    channels: list[str] = field(default_factory=list)
    email: EmailConfig | None = None
    webhook: WebhookConfig | None = None
