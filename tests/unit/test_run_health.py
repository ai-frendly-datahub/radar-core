from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from radar_core.health import (
    build_run_health,
    count_stored_items,
    evaluate_run_health,
    main,
    merge_run_health,
    write_run_health,
)


def _collection(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "collected_total": 4,
        "collected_counts": {"alpha": 3, "beta": 0, "gamma": 1},
        "failed_sources": [{"source": "beta", "errors": ["timed out"]}],
        "last_success_at": {"alpha": "2026-10-29T00:00:00+00:00"},
    }
    payload.update(overrides)
    return payload


def _health(**overrides: int) -> dict[str, int]:
    payload = {
        "sources_attempted": 3,
        "sources_with_fetch_errors": 1,
        "raw_items_fetched": 4,
        "items_stored_new": 2,
        "items_stored_upserted": 1,
        "items_stored": 3,
    }
    payload.update(overrides)
    return payload


def test_build_run_health_uses_this_run_and_storage_counts() -> None:
    original = _collection()
    snapshot = json.loads(json.dumps(original))
    health = build_run_health(
        original,
        items_stored_new=2,
        items_stored_upserted=1,
    )

    assert health == _health()
    assert original == snapshot
    assert "timed out" not in json.dumps(health)


def test_build_run_health_does_not_treat_fetched_rows_as_stored() -> None:
    health = build_run_health(
        _collection(),
        items_stored_new=0,
        items_stored_upserted=0,
    )

    assert health["raw_items_fetched"] == 4
    assert health["items_stored"] == 0


def test_explicit_totals_override_run_collection() -> None:
    health = build_run_health(
        _collection(),
        sources_attempted=5,
        sources_with_fetch_errors=2,
        raw_items_fetched=9,
        items_stored_new=1,
        items_stored_upserted=0,
    )

    assert health["sources_attempted"] == 5
    assert health["sources_with_fetch_errors"] == 2
    assert health["raw_items_fetched"] == 9
    assert health["items_stored"] == 1


def test_collected_total_is_raw_even_when_source_counts_differ() -> None:
    health = build_run_health(
        _collection(
            collected_total=2,
            collected_counts={"alpha": 3},
            failed_sources=[],
        ),
        items_stored_new=0,
        items_stored_upserted=0,
    )

    assert health["raw_items_fetched"] == 2
    assert health["sources_attempted"] == 1
    assert health["sources_with_fetch_errors"] == 0
    assert health["items_stored"] == 0


def test_failed_source_missing_from_counts_is_still_attempted() -> None:
    health = build_run_health(
        {
            "collected_counts": {"alpha": 2},
            "failed_sources": [
                {"source": "beta", "errors": ["connection reset"]},
                {"source": "beta", "errors": ["connection reset again"]},
            ],
        },
        items_stored_new=1,
        items_stored_upserted=1,
    )

    assert health["sources_attempted"] == 2
    assert health["sources_with_fetch_errors"] == 1
    assert health["raw_items_fetched"] == 2
    assert health["items_stored"] == 2


def test_string_errors_match_the_longest_source_name() -> None:
    health = build_run_health(
        {
            "collected_counts": {"서울시": 0, "서울시 지원사업": 0},
            "errors": [
                "서울시 지원사업: SSLError: Authorization: Bearer test-token-value",
                "unattributed failure without a source prefix",
            ],
        },
        items_stored_new=0,
        items_stored_upserted=0,
    )

    assert health["sources_attempted"] == 2
    assert health["sources_with_fetch_errors"] == 1
    assert "test-token-value" not in json.dumps(health)
    assert "Bearer" not in json.dumps(health)


def test_empty_failure_text_does_not_count_as_a_fetch_error() -> None:
    health = build_run_health(
        {
            "collected_counts": {"alpha": 0},
            "failed_sources": [{"source": "alpha", "errors": ["", "   "]}],
        },
        items_stored_new=0,
        items_stored_upserted=0,
    )

    assert health["sources_with_fetch_errors"] == 0
    assert health["items_stored"] == 0


def test_failures_alone_do_not_invent_an_attempt_list() -> None:
    with pytest.raises(ValueError, match="sources_attempted"):
        build_run_health(
            {"failed_sources": [{"source": "alpha", "errors": ["timed out"]}]},
            raw_items_fetched=0,
            items_stored_new=0,
            items_stored_upserted=0,
        )


def test_missing_failure_list_is_not_assumed_to_be_zero() -> None:
    with pytest.raises(ValueError, match="sources_with_fetch_errors"):
        build_run_health(
            {"collected_counts": {"alpha": 1}, "collected_total": 1},
            items_stored_new=1,
            items_stored_upserted=0,
        )


def test_build_run_health_rejects_bools_and_negative_counts() -> None:
    with pytest.raises(ValueError, match="items_stored_new"):
        build_run_health(items_stored_new=True, items_stored_upserted=0)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="items_stored_upserted"):
        build_run_health(
            _collection(),
            items_stored_new=0,
            items_stored_upserted=-1,
        )


def test_count_stored_items_splits_new_and_upserted_links() -> None:
    new_count, upserted_count = count_stored_items(
        written_links=[
            "https://example.com/new",
            "https://example.com/old",
            "https://example.com/new",
        ],
        existing_links=["https://example.com/old", "https://example.com/untouched"],
    )

    assert (new_count, upserted_count) == (1, 1)


def test_count_stored_items_rejects_non_strings() -> None:
    with pytest.raises(TypeError, match="written_links"):
        count_stored_items(written_links=[None], existing_links=[])  # type: ignore[list-item]


def test_merge_run_health_keeps_existing_report_fields(tmp_path: Path) -> None:
    report = {
        "category": "govsupport",
        "summary": {"tracked_sources": 1, "fresh_sources": 0},
        "sources": [{"source": "알파", "errors": ["알파: timeout"]}],
        "errors": ["알파: timeout"],
    }
    health = build_run_health(
        {
            "collected_counts": {"알파": 0},
            "collected_total": 0,
            "failed_sources": [{"source": "알파", "errors": ["timeout"]}],
        },
        items_stored_new=0,
        items_stored_upserted=0,
    )
    merged = merge_run_health(report, {**health, "token": "test-token-value"})

    assert list(merged) == ["category", "summary", "run_health", "sources", "errors"]
    assert merged["summary"] == report["summary"]
    assert merged["sources"] == report["sources"]
    assert merged["errors"] == report["errors"]
    assert merged["run_health"] == health
    assert "token" not in json.dumps(merged["run_health"])
    assert "run_health" not in report

    path = tmp_path / "govsupport_quality.json"
    path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    write_run_health(path, health)
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written == merged
    assert "알파" in path.read_text(encoding="utf-8")

    write_run_health(path, _health())
    replaced = json.loads(path.read_text(encoding="utf-8"))
    assert replaced["run_health"] == _health()
    assert replaced["errors"] == ["알파: timeout"]


def test_write_run_health_requires_an_existing_report(tmp_path: Path) -> None:
    missing = tmp_path / "missing_quality.json"
    with pytest.raises(ValueError, match="not found"):
        write_run_health(missing, _health())
    assert not missing.exists()


def test_zero_stored_is_an_error_by_default() -> None:
    verdict = evaluate_run_health(
        _health(items_stored_new=0, items_stored_upserted=0, items_stored=0)
    )

    assert verdict.ok is False
    assert verdict.level == "error"
    assert verdict.reason == "stored 0 items (minimum 1)"
    assert verdict.annotation_line() == "::error::stored 0 items (minimum 1)"


def test_zero_stored_and_every_source_errored_share_one_error_line() -> None:
    verdict = evaluate_run_health(
        _health(
            sources_attempted=4,
            sources_with_fetch_errors=4,
            raw_items_fetched=0,
            items_stored_new=0,
            items_stored_upserted=0,
            items_stored=0,
        )
    )

    assert verdict.level == "error"
    assert verdict.reason == "stored 0 items (minimum 1); every source errored (4/4)"
    assert "\n" not in verdict.reason


def test_every_source_errored_warns_when_rows_were_stored() -> None:
    verdict = evaluate_run_health(
        _health(sources_attempted=2, sources_with_fetch_errors=2, items_stored=3),
        min_stored=1,
    )

    assert verdict.ok is False
    assert verdict.level == "warning"
    assert verdict.reason == "every source errored (2/2)"
    assert verdict.annotation_line() == "::warning::every source errored (2/2)"


def test_min_stored_zero_allows_an_empty_store_when_a_source_responded() -> None:
    verdict = evaluate_run_health(
        _health(
            sources_with_fetch_errors=0,
            items_stored_new=0,
            items_stored_upserted=0,
            items_stored=0,
        ),
        min_stored=0,
    )

    assert verdict.ok is True
    assert verdict.level == "ok"
    assert verdict.annotation_line() is None


def test_threshold_above_zero_stored_still_fails() -> None:
    verdict = evaluate_run_health(_health(items_stored=3), min_stored=4)

    assert verdict.ok is False
    assert verdict.reason == "stored 3 items (minimum 4)"


def test_inconsistent_stored_total_is_an_error() -> None:
    verdict = evaluate_run_health(_health(items_stored=9))

    assert verdict.ok is False
    assert verdict.level == "error"
    assert "items_stored 9 does not equal" in verdict.reason


def test_cli_zero_stored_prints_error_annotation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "quality.json"
    path.write_text(
        json.dumps(
            {
                "category": "example",
                "run_health": _health(
                    items_stored_new=0,
                    items_stored_upserted=0,
                    items_stored=0,
                ),
            }
        ),
        encoding="utf-8",
    )

    assert main(["--report", str(path)]) == 1
    output = capsys.readouterr().out.splitlines()
    assert output == [
        "stored 0 items (minimum 1)",
        "::error::stored 0 items (minimum 1)",
    ]


def test_cli_all_sources_errored_exits_non_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "quality.json"
    path.write_text(
        json.dumps(
            {"run_health": _health(sources_attempted=2, sources_with_fetch_errors=2)}
        ),
        encoding="utf-8",
    )

    assert main(["--report", str(path), "--min-stored", "1"]) == 1
    output = capsys.readouterr().out
    assert "every source errored (2/2)" in output
    assert "::warning::every source errored (2/2)" in output


def test_cli_passes_when_stored_meets_the_threshold(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "quality.json"
    path.write_text(json.dumps({"run_health": _health()}), encoding="utf-8")

    assert main(["--report", str(path), "--min-stored", "3"]) == 0
    output = capsys.readouterr().out
    assert output.strip() == "stored 3 items (minimum 3)"
    assert "::" not in output


def test_cli_fails_closed_without_run_health(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "quality.json"
    path.write_text(
        json.dumps({"category": "example", "summary": {}}), encoding="utf-8"
    )

    assert main(["--report", str(path)]) == 1
    output = capsys.readouterr().out
    assert f"quality report has no run_health object: {path}" in output
    assert "::error::" in output


def test_cli_rejects_invalid_json_and_missing_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    invalid = tmp_path / "broken.json"
    invalid.write_text("{", encoding="utf-8")
    assert main(["--report", str(invalid)]) == 1
    assert "not valid JSON" in capsys.readouterr().out

    missing = tmp_path / "absent.json"
    assert main(["--report", str(missing)]) == 1
    assert "not found" in capsys.readouterr().out


def test_cli_rejects_a_negative_threshold() -> None:
    with pytest.raises(SystemExit) as caught:
        main(["--report", "unused.json", "--min-stored", "-1"])
    assert caught.value.code == 2


def test_module_entry_point_exits_non_zero_for_zero_stored(tmp_path: Path) -> None:
    path = tmp_path / "quality.json"
    path.write_text(
        json.dumps(
            {
                "run_health": _health(
                    sources_attempted=1,
                    sources_with_fetch_errors=0,
                    raw_items_fetched=0,
                    items_stored_new=0,
                    items_stored_upserted=0,
                    items_stored=0,
                )
            }
        ),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "radar_core.health",
            "--report",
            str(path),
            "--min-stored",
            "1",
        ],
        cwd=Path(__file__).resolve().parents[2],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 1
    assert completed.stdout.splitlines() == [
        "stored 0 items (minimum 1)",
        "::error::stored 0 items (minimum 1)",
    ]
