# RADAR-CORE

24개 Radar 레포가 공유하는 핵심 라이브러리 (v0.2.0).
HTML 리포트 생성, 데이터 모델, 스토리지, 분석기, 수집기를 제공합니다.

## PURPOSE

AI-Friendly DataHub의 모든 Radar 프로젝트에서 사용하는 공통 기능을 제공하는 공유 패키지입니다.
각 Radar 레포는 이 패키지를 의존성으로 설치하여 일관된 데이터 수집, 분석, 리포트 생성 기능을 사용합니다.

## CORE MODULES

- `radar_core/models.py` — Article, CategoryConfig, Source, EntityDefinition, RadarSettings 등 공유 데이터 모델
- `radar_core/storage.py` — RadarStorage (DuckDB 기반 CRUD + 보존 정책)
- `radar_core/analyzer.py` — apply_entity_rules (키워드 기반 엔티티 매칭, 한국어 지원)
- `radar_core/collector.py` — collect_sources (RSS/HTTP 수집, 서킷 브레이커, 적응형 스로틀)
- `radar_core/report_utils.py` — generate_report, generate_index_html (Jinja2 HTML 생성)
- `radar_core/templates/report.html` — 개별 리포트 템플릿 (Flatpickr + Chart.js + 플러그인 슬롯)
- `radar_core/templates/index.html` — 인덱스 페이지 (캘린더 네비게이션 + 트렌드 차트 + 검색)
- `radar_core/config_loader.py` — YAML 설정 로더 (load_settings, load_category_config, load_notification_config)
- `radar_core/search_index.py` — SQLite FTS5 전문 검색 (SearchIndex, SearchResult)
- `radar_core/notifier.py` — 알림 시스템 (EmailNotifier, WebhookNotifier, CompositeNotifier)
- `radar_core/telegram_notifier.py` — TelegramNotifier (텔레그램 알림)
- `radar_core/crawl_health.py` — CrawlHealthStore, CrawlHealthRecord (수집 상태 모니터링)
- `radar_core/adaptive_throttle.py` — AdaptiveThrottler, SourceThrottleState (적응형 스로틀링)
- `radar_core/nl_query.py` — parse_query, ParsedQuery (자연어 쿼리 파서)
- `radar_core/raw_logger.py` — RawLogger (JSONL 원시 데이터 로깅)
- `radar_core/logger.py` — configure_logging, get_logger (structlog 기반 로깅)
- `radar_core/exceptions.py` — NetworkError, ParseError, StorageError, ReportError, SearchError, NotificationError, SourceError

## PUBLIC API

```python
from radar_core import (
    # Version
    __version__,
    
    # Models
    Article,
    CategoryConfig,
    Source,
    EntityDefinition,
    RadarSettings,
    EmailSettings,
    TelegramSettings,
    NotificationConfig,
    CrawlHealthAlert,
    
    # Storage
    RadarStorage,
    
    # Collector
    collect_sources,
    RateLimiter,
    AdaptiveThrottler,
    SourceThrottleState,
    
    # Analyzer
    apply_entity_rules,
    
    # Config Loader
    load_settings,
    load_category_config,
    load_notification_config,
    
    # Search
    SearchIndex,
    SearchResult,
    
    # Natural Language Query
    parse_query,
    ParsedQuery,
    
    # Notifiers
    Notifier,
    EmailNotifier,
    WebhookNotifier,
    TelegramNotifier,
    CompositeNotifier,
    NotificationPayload,
    
    # Crawl Health
    CrawlHealthStore,
    CrawlHealthRecord,
    
    # Logging
    configure_logging,
    get_logger,
    RawLogger,
    
    # Exceptions
    NetworkError,
    ParseError,
    StorageError,
    ReportError,
    SearchError,
    NotificationError,
    SourceError,
)
```

## DEPENDENT REPOSITORIES

모든 24개 Radar 레포가 이 패키지에 의존합니다:
GameRadar, MovieRadar, MusicRadar, BookRadar, TechRadar, ScienceRadar, HealthRadar, FinanceRadar, CryptoRadar, EduRadar, JobRadar, RealEstateRadar, TravelRadar, FoodRadar, FashionRadar, SportsRadar, AutoRadar, PetRadar, HomeRadar, GardenRadar, WineRadar, PriceRadar, TrendRadar, WeatherRadar

변경 시 하위 호환성 유지가 필수입니다.

## MODIFICATION RULES

- `generate_report()` 함수 시그니처 변경 금지 (24개 레포 영향)
- `generate_index_html()` 함수 시그니처 변경 금지
- `RadarStorage` 스키마 변경 시 마이그레이션 스크립트 필요
- 새 기능 추가 시 `__init__.py`의 `__all__` 업데이트 필수
- 템플릿 변경 시 모든 레포의 리포트 출력에 영향
- 기존 API 제거 시 deprecation 경고 후 최소 1개 버전 유지

## PLUGIN SLOTS (ADVANCED TIER)

report.html에 Plotly 차트 삽입 가능:

```python
plugin_charts = [
    {
        "id": "my_chart",
        "title": "커스텀 차트 제목",
        "config_json": plotly_html
    }
]
generate_report(..., plugin_charts=plugin_charts)
```

현재 사용 레포:
- HomeRadar (choropleth 지역별 부동산 가격 분포)
- WineRadar (network_graph 와인 품종 관계도)
- PriceRadar (price_forecast 가격 예측 차트)
- TrendRadar (trend_heatmap 트렌드 히트맵)

## TESTING

```bash
cd radar-core && pytest tests/ -v
```

## RELEASE FLOW

1. `pyproject.toml`에서 버전 업데이트
2. `v0.2.1` 형식으로 git tag 생성
3. tag를 push
4. GitHub Actions가 자동으로 wheel/sdist 빌드 및 배포
