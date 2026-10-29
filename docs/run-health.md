# 이번 실행의 저장 건수 검사

수집 워크플로가 성공으로 끝나도, 소스가 모두 비었거나 항목이 전부 필터에서 빠지면 저장되는 행은 0일 수 있다. 요약 JSON의 `sources`는 최근 7일 누적이라 오늘 0건이 가려진다. `run_collection`은 이번 실행에서 가져온 건수이고, 품질 리포트 JSON의 `run_health`는 그중 실제로 저장된 건수까지 적는다.

## 수집 직후

저장 직전에 이미 있던 링크와, 이번에 저장소에 넘긴 링크만 센다. 가져온 건수를 저장 건수로 넣지 않는다.

```python
from pathlib import Path

from radar_core.health import build_run_health, count_stored_items, write_run_health

new_count, upserted_count = count_stored_items(
    written_links=written_links,
    existing_links=existing_links,
)
run_health = build_run_health(
    run_collection,
    items_stored_new=new_count,
    items_stored_upserted=upserted_count,
)
write_run_health(quality_report_path, run_health)
```

`run_collection`은 `generate_report(..., run_collection=...)`에 넘기는 것과 같은 dict다.

- `collected_counts`: 시도한 소스와 소스별 수집 건수
- `failed_sources` 또는 `errors`: fetch에 실패한 소스. `errors`는 `collect_sources`가 돌려주는 `"소스이름: 메시지"` 목록이어도 된다
- `collected_total`: 이번 실행에서 가져온 원본 건수

품질 리포트를 이 호출보다 나중에 다시 쓰면, 그 결과에 `merge_run_health(report, run_health)`로 같은 객체를 다시 붙인다. 오류 문자열은 `run_health`에 복사하지 않는다.

필드는 다음 여섯 개다.

- `sources_attempted`
- `sources_with_fetch_errors`
- `raw_items_fetched`
- `items_stored_new`
- `items_stored_upserted`
- `items_stored` (`items_stored_new` + `items_stored_upserted`)

품질 리포트 파일 이름은 저장소가 이미 쓰는 경로다. GovRadar는 `reports/<category>_quality.json`이다.

## 워크플로

수집 스텝 다음에 둔다. `if: always()`이므로 수집 스텝이 실패해도, 성공했지만 저장이 0건이어도 검사가 실행된다. 이 스텝은 시크릿이 필요 없다.

```yaml
      - name: Check this run stored rows
        if: always()
        run: python -m radar_core.health --report reports/govsupport_quality.json --min-stored 1
```

`--report` 값은 그 저장소의 품질 리포트 경로로 바꾼다. `--min-stored`를 생략하면 1이다.

- `items_stored`가 `--min-stored`보다 작으면 `::error::` 한 줄을 출력하고 종료 코드 1로 끝난다. 기본값에서는 저장 0건이 여기 해당한다.
- 시도한 소스가 모두 fetch 오류이면, 저장 건수가 기준 이상이어도 `::warning::`을 출력하고 종료 코드는 1이다.
- 둘 다이면 `::error::` 한 줄에 두 이유를 적는다.

실패를 알릴 때는 저장소에 이미 있는 `NOTIFICATION_WEBHOOK_URL`만 쓴다. 이 검사를 위해 시크릿을 새로 추가하지 않는다. 그 시크릿이 없는 저장소는 아래 스텝을 넣지 않는다.

```yaml
      - name: Notify on empty collection
        if: failure()
        env:
          NOTIFICATION_WEBHOOK_URL: ${{ secrets.NOTIFICATION_WEBHOOK_URL }}
        run: |
          if [ -z "${NOTIFICATION_WEBHOOK_URL:-}" ]; then
            echo "failure notify skipped; NOTIFICATION_WEBHOOK_URL is unset"
            exit 0
          fi
          curl -sS -X POST "${NOTIFICATION_WEBHOOK_URL}" \
            -H "Content-Type: application/json" \
            -d "{\"repo\":\"${{ github.repository }}\",\"workflow\":\"${{ github.workflow }}\",\"run_id\":\"${{ github.run_id }}\",\"run_url\":\"${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}\",\"status\":\"failure\"}" \
            || true
```
