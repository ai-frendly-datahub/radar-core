from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class Source:
    """데이터 소스 정의.

    Attributes:
        name: 소스 표시명
        type: 소스 타입 (rss, javascript, reddit, mcp, api)
        url: 소스 URL
        id: 고유 식별자 (예: media_techcrunch_us)
        enabled: 활성화 여부 (False면 수집 스킵)
        language: ISO 639-1 언어 코드 (예: ko, en, ja)
        country: ISO 3166-1 alpha-2 국가 코드 (예: KR, US, JP)
        region: 지역 계층 (예: Asia/East/Korea)
        trust_tier: 신뢰도 등급 (T1_authoritative, T2_expert, T3_professional, T4_community)
        weight: 스코어링 가중치 (0.5~3.0, 기본 1.0)
        content_type: 콘텐츠 유형 (news, review, statistics, education, community)
        collection_tier: 수집 복잡도 (C1_rss, C2_html_simple, C3_html_js, C4_api, C5_manual)
        producer_role: 생산자 역할 (expert_media, trade_media, government, research_inst, consumer_comm)
        info_purpose: 정보 목적 태그 목록
        notes: 관리자 메모
        config: 수집기별 추가 설정
    """

    name: str
    type: str
    url: str
    id: str = ""
    enabled: bool = True
    language: str = ""
    country: str = ""
    region: str = ""
    trust_tier: str = "T3_professional"
    weight: float = 1.0
    content_type: str = "news"
    collection_tier: str = "C1_rss"
    producer_role: str = ""
    info_purpose: list[str] = field(default_factory=list)
    notes: str = ""
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class EntityDefinition:
    """엔티티 정의.

    Attributes:
        name: 엔티티 식별자 (예: Vulnerability, DataBreach)
        display_name: 표시명 (예: 취약점, 데이터 유출)
        keywords: 매칭 키워드 목록
        boost_factor: 중요도 가중치 (0.5~3.0, 기본 1.0)
        case_sensitive: 대소문자 구분 여부
        partial_match: 부분 일치 허용 여부
    """

    name: str
    display_name: str
    keywords: list[str]
    boost_factor: float = 1.0
    case_sensitive: bool = False
    partial_match: bool = True


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
    ontology: dict[str, Any] = field(default_factory=dict)


@dataclass
class CategoryConfig:
    category_name: str
    display_name: str
    sources: list[Source]
    entities: list[EntityDefinition]


@dataclass
class GraphSettings:
    """데이터 라이프사이클 설정."""

    url_ttl_days: int = 30
    snapshot_keep_days: int = 30
    monthly_keep_months: int = 12


@dataclass
class ReportSettings:
    """리포트 생성 설정."""

    daily_max_items: int = 50
    sections: list[str] = field(default_factory=lambda: ["top_issues", "trending", "new_sources"])


@dataclass
class ResilienceSettings:
    """Circuit breaker 설정."""

    fail_max: int = 5
    reset_timeout_seconds: int = 60
    success_threshold: int = 2


@dataclass
class RadarSettings:
    """전역 Radar 설정.

    Attributes:
        database_path: DuckDB 데이터베이스 경로
        report_dir: HTML 리포트 출력 디렉토리
        raw_data_dir: Raw JSONL 데이터 디렉토리
        search_db_path: SQLite FTS5 검색 인덱스 경로
        mode: 운영 모드 (daily, incremental, manual)
        timezone: 스케줄링 타임존 (예: Asia/Seoul)
        graph: 데이터 라이프사이클 설정
        report: 리포트 생성 설정
        resilience: Circuit breaker 설정
    """

    database_path: Path
    report_dir: Path
    raw_data_dir: Path
    search_db_path: Path
    mode: str = "daily"
    timezone: str = "UTC"
    graph: GraphSettings = field(default_factory=GraphSettings)
    report: ReportSettings = field(default_factory=ReportSettings)
    resilience: ResilienceSettings = field(default_factory=ResilienceSettings)


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
