# 발행 시각 KST/UTC 보정 계획

감사에서 수집 항목 296건 중 160건의 저장 `published` 가 원문과 정확히 9시간 달랐다.
GameRadar RSS 원문과 대조한 결과, 원인은 타임존이 섞인 것이다. 이 문서는 이미 쌓인
DuckDB 와 jsonl 을 어떻게 찾고 어떤 조건에서만 고칠지 적는다. 스크립트는 기본값이
dry-run 이며, `--apply` 없이 데이터를 수정하지 않는다.

## 원인

`feedparser` 의 `published_parsed` / `updated_parsed` 는 UTC `time.struct_time` 이다.
radar-core `radar_core/collector.py` 의 `_extract_datetime` 이 이를 `time.mktime` 으로
읽었다. `time.mktime` 은 그 시계를 **프로세스 로컬 시각**으로 본다.

GameRadar 워크플로 (`.github/workflows/radar-crawler.yml`) 는 `TZ: Asia/Seoul` 이다.
UTC 자정(`00:00Z`) struct 를 KST 로 해석하면 POSIX 타임스탬프가 9시간 앞서고,
`datetime.fromtimestamp(..., tz=UTC)` 가 그 값을 UTC 로 저장한다. 저장된 발행 시각은
실제 UTC 보다 정확히 9시간 빠르다(벽시계는 9시간 과거).

같은 패턴이 각 레포에 복사된 collector 에도 있다. radar-core 만 고쳐서는 그 복사본이
바뀌지 않는다. 아래 "후속 수정이 필요한 레포" 를 본다.

함께 있던 문제:

- 오프셋이 없는 문자열을 UTC 로 붙였다. 한국 소스의 `2026-10-29 18:00:00` 은
  KST 18:00 인데 UTC 18:00 으로 남았다.
- DuckDB `TIMESTAMP` 에 넣기 전에 `tzinfo` 를 벗겨 오프셋이 사라졌다.
- `data/raw`, `data/snapshots`, `data/daily`, 날짜가 붙은 리포트 파일명은
  `datetime.now(UTC).date()` 를 썼다. 크롤러 `TZ` 는 Asia/Seoul 이라
  00:00–08:59 KST 에는 날짜 키가 하루 어긋날 수 있다.

## 앞으로의 규칙

파싱 결과는 항상 timezone-aware datetime 이다.

| 입력 | 해석 |
| --- | --- |
| `*_parsed` struct_time | UTC. `calendar.timegm` 만 사용한다. `time.mktime` 금지. |
| 오프셋이 있는 문자열 (`GMT`, `+0900`, `Z`, `-0400`) | 그 오프셋을 유지한다. |
| 오프셋이 없는 문자열 | 소스 `default_timezone`. 없으면 `.kr` 호스트는 `Asia/Seoul`, 그 외는 UTC. |
| API epoch (`created_utc` 등) | UTC. |

저장:

- DuckDB `articles.published`, `collected_at`, `fetched_at` 은 기존처럼
  `TIMESTAMP` 이고 **UTC 벽시계**(naive)다. 컬럼 타입을 바꾸면 36개 레포의
  비교 쿼리가 깨진다. 읽는 쪽에서는 UTC 로 다시 붙인다.
- JSON/jsonl/리포트의 발행 시각은 UTC ISO-8601 이며 오프셋이 있다.
  예: `2026-10-29T00:00:00+00:00`.
- 원문 문자열은 `published_raw` (TEXT, 마이그레이션 `v005_published_raw`) 에 둔다.
- 일별 파티션 키는 **수집 시각의 Asia/Seoul 달력 날짜**다. 기사 `published` 의
  날짜가 아니고 UTC 날짜도 아니다. `radar_core.dates.DAILY_PARTITION_TIMEZONE`
  과 `daily_partition_date()` 가 이 키의 정의다. `RadarSettings.timezone` 은
  스케줄 타임존으로 그대로 둔다.

소스 설정 예:

```yaml
- name: 인벤
  type: rss
  url: https://www.inven.co.kr/rss/news.xml
  # .kr 은 생략해도 Asia/Seoul. .com 한국 소스는 명시한다.
  default_timezone: Asia/Seoul
```

`config.default_timezone` 도 같은 의미다.

## 기존 행을 찾는 법

자동으로 고칠 수 있는 행과, 집계만 하는 행을 나눈다.

1. **`published_raw` 가 있는 행**  
   원문을 위 규칙으로 파싱한 UTC 순간과, 저장된 `published`(naive 는 UTC)를 비교한다.
   - 차이가 1초 미만이면 이미 맞다.
   - 차이가 정확히 +9시간 또는 -9시간이면 KST/UTC 혼선이다.
     오프셋이 있는 원문이면 소스와 무관하게 수정 대상이다.
     원문에 오프셋이 없으면 소스 기본 타임존이 `Asia/Seoul` 일 때만 수정 대상이다
     (`.kr` URL 또는 `default_timezone: Asia/Seoul`).
   - 그 외 차이(예: 4시간, 5시간)는 `unexpected` 로만 보고하고 고치지 않는다.
     DST 권역의 다른 오프셋은 사람이 확인한다.
2. **`published_raw` 가 없는 행**  
   과거 jsonl 의 `published` 는 이미 어긋난 ISO 인 경우가 많다. 원문이 없으면
   9시간을 더하거나 빼지 않는다. `.kr` / `Asia/Seoul` 소스는 `ambiguous` 로
   건수만 올린다. 고치려면 RSS 를 다시 받아 `published_raw` 를 채운 뒤
   같은 스크립트를 다시 돌린다.
3. **저장된 `published` 가 비어 있고 `published_raw` 가 파싱되는 행**  
   방향 추측이 아니라 원문으로 채운다. 수정 대상이다.

`time.mktime` 버그(오프셋 있는 RSS, 저장값이 9시간 과거)와, 타임존 없는 한국
문자열을 UTC 로 저장한 경우(저장 벽시계가 KST 와 같고 올바른 UTC 는 9시간 과거)는
부호가 반대다. 둘 다 차이가 정확히 9시간일 때만 원문 기준으로 덮어쓴다.
두 버그가 겹쳐 이미 올바른 UTC 가 된 행(차이 0)은 건드리지 않는다.

## 스크립트

```bash
# 집계만. DuckDB 는 read-only 로 연다. jsonl 을 쓰지 않는다.
python scripts/backfill_published_timezone.py --repos-root /path/to/checkouts

# 레포 하나
python scripts/backfill_published_timezone.py --repo /path/to/GameRadar

# 조건을 만족하는 행만 수정
python scripts/backfill_published_timezone.py --repo /path/to/GameRadar --apply
```

출력은 레포, 소스, 저장소 종류(`duckdb` / `jsonl`)별 건수다.
`total`, `already_correct`, `correctable`, `ambiguous`, `unexpected`.

대상 파일:

- `data/**/*.duckdb` 의 `articles`. `data/snapshots`, `data/daily` 은
  시점 복사본이라 제외한다.
- `data/**/*.jsonl`.

`--apply` 가 수정하는 것:

- DuckDB: `published` 를 올바른 UTC naive 로 `UPDATE`. `published_raw` 는 유지.
  `link` 가 있는 행만.
- jsonl: 해당 줄의 `published` 를 `+00:00` ISO 로 바꾼다. `published_raw` 는 유지.
  `link` 가 있는 줄만.

`--apply` 가 하지 않는 것:

- 네트워크 재수집
- `published_raw` 가 없는 행의 ±9시간 일괄 이동
- 9시간이 아닌 차이
- Asia/Seoul 이 아닌 소스의 타임존 없는 원문
- snapshot / daily 복사본

적용 전에 해당 DuckDB 와 jsonl 을 브랜치 밖으로 복사해 둔다.
한 번 적용한 뒤 다시 dry-run 하면 그 행은 `already_correct` 가 되어야 한다.
`unexpected` 가 남아 있으면 원문 RSS 와 `published_raw` 를 사람이 비교한다.

소스 타임존은 그 레포의 `config/**/*.yaml` `sources[].url`,
`default_timezone`, `config.default_timezone` 으로 정한다. 설정이 없으면 UTC 라서
`.kr` 이 아닌 한국 사이트는 `ambiguous` 로 남는다. 그 소스에
`default_timezone: Asia/Seoul` 을 넣은 뒤 dry-run 을 다시 본다.

## 스키마 마이그레이션

radar-core `RadarStorage` 가 열릴 때 `v005_published_raw` 가
`articles.published_raw TEXT` 를 추가한다. 기존 행의 값은 NULL 이다.
NULL 인 과거 행은 위 2번과 같이 자동 보정하지 않는다.
새로 수집되는 행부터 원문 문자열이 채워진다.

다운스트림 레포가 자체 `CREATE TABLE` 로 `articles` 를 만들면 같은 컬럼을
추가해야 `published_raw` 가 저장된다. radar-core `RadarStorage` 를 쓰면
마이그레이션이 붙는다.

## 후속 수정이 필요한 레포

radar-core 를 고쳐도, 수집을 `radar_core.collector` 가 아니라 **레포 안 collector**
로 하는 곳은 그대로 `time.mktime` 이거나 UTC 날짜 키를 쓴다. 워크플로의
`TZ: Asia/Seoul` 과 만나면 9시간 오차가 계속된다.

`time.mktime(published_parsed)` 를 아직 쓰는 복사본. 공유 파서
(`parse_feed_entry`, `calendar.timegm`)로 바꿔야 한다.

| 레포 | 위치 |
| --- | --- |
| GameRadar | `radar/collector.py` (`_extract_datetime`, struct 를 `time.mktime`) |
| PriceRadar | `priceradar/collector.py` |
| PaperRadar | `paperradar/collector.py` |
| TrendRadar | `trendradar/collector.py` |
| PolicyRadar | `policyradar/collector.py` |
| SubscriptionRadar | `subscriptionradar/collector.py` |
| RefundRadar | `refundradar/collector.py` |
| TrustRadar | `trustradar/collector.py` |
| QueueRadar | `queueradar/collector.py` |
| PropertyRadar | `propertyradar/collector.py` |
| PaperworkRadar | `paperworkradar/collector.py` |
| MovieRadar | `movieradar/collector.py` |
| JobRadar | `jobradar/collector.py` |
| MobilityRadar | `mobilityradar/collector.py` |
| HomeRadar | `homeradar/collector.py` (`collectors/rss_collector.py` 는 이미 `timegm`) |
| EventRadar | `eventradar/collector.py` |

struct 를 UTC 필드로 직접 읽거나 이미 `timegm` 을 쓰지만, 타임존 없는 한국 날짜와
일별 키가 아직 로컬 구현인 곳. 공유 `parse_feed_entry` / `daily_partition_date`
로 맞춘다.

| 레포 | 메모 |
| --- | --- |
| ArtRadar | `artradar/collector.py` 는 `timegm` |
| BlogRadar | `blogradar/collector.py` 는 `timegm` |
| BookRadar | `bookradar/collector.py` 는 `timegm` |
| BenefitRadar | `benefitradar/collector.py` 는 `timegm` |
| FoodRadar | `foodradar/collector.py` 는 `timegm` |
| WineRadar | `collectors/rss_collector.py` 는 `timegm`. `wineradar/collector.py` 는 radar-core 를 일부 사용 |
| GovRadar | struct 필드를 `tzinfo=UTC` 로 조립한 뒤 naive 로 벗긴다. `time.mktime` 은 아니다 |
| CommerceRadar | `commerceradar/collector.py` 가 struct 를 UTC 로 조립한다 |
| CryptoRadar | 이미 `default_timezone` 과 `timegm` 이 있다. 공유 파서와 이중 적용되지 않게 검토만 한다 |
| Radar-Template | `radar/collector.py` 는 문자열만 `parsedate_to_datetime` 하고 naive 를 UTC 로 둔다 |
| MCP Radar 일습 | `WeatherMCPRadar`, `TravelMCPRadar`, `SearchTrendMCPRadar`, `RealEstateMCPRadar`, `FinanceTaxMCPRadar`, `MiscMCPRadar`, `PublicDataMCPRadar`, `LegalGovMCPRadar`, `MapAddressMCPRadar`, `KoreanNLPMCPRadar`, `CollaborationMCPRadar`, `CommerceMCPRadar` 의 `radar/collector.py` 가 같은 문자열 경로다. `radar_core/collector.py` 재export 는 로컬 `radar.collector` 를 가리킨다 |

거의 모든 레포의 `date_storage.py` / raw 디렉터리가 `datetime.now(UTC).date()` 를
쓴다. 일별 키를 KST 로 맞출 때 `radar_core.dates.daily_partition_date` 를 쓰면 된다.
로컬 테스트를 `datetime.now(UTC).date()` 와 비교하는 곳도 같이 고쳐야 한다.

## 적용 순서

1. 이 radar-core 변경을 배포한다. 새 수집분부터 `published_raw` 와 올바른 UTC 가 쌓인다.
2. 위 표의 레포 collector 를 공유 파서로 바꾼다. 특히 `time.mktime` 목록.
3. 각 레포에서 dry-run 으로 건수를 확인한다.
4. `correctable` 만 `--apply` 로 반영한다. `ambiguous` 는 재수집 또는 소스별
   `default_timezone` 지정 후에 다시 dry-run 한다.
5. 리포트와 `data/raw` 날짜 디렉터리가 KST 기준인지 00:00–09:00 KST 구간에 한 번 확인한다.
