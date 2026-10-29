"""Dry-run backfill must not write. --apply corrects only ±9h rows with raw text."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import duckdb
import yaml

from radar_core.timezone_backfill import (
    ALREADY_CORRECT,
    AMBIGUOUS,
    CORRECTABLE,
    UNEXPECTED,
    assess_published,
    format_report,
    scan_repositories,
)


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "GameRadar"
    (repo / "config" / "categories").mkdir(parents=True)
    (repo / "data" / "raw" / "2026-10-29").mkdir(parents=True)
    (repo / "config" / "categories" / "game.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "name": "인벤",
                        "type": "rss",
                        "url": "https://www.inven.co.kr/rss/news.xml",
                    },
                    {
                        "name": "IGN",
                        "type": "rss",
                        "url": "https://www.ign.com/rss",
                    },
                ]
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    db_path = repo / "data" / "radar.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE articles (
            link TEXT,
            source TEXT,
            published TIMESTAMP,
            published_raw TEXT
        )
        """)
    conn.execute(
        "INSERT INTO articles VALUES (?, ?, ?, ?)",
        [
            "https://example.com/gmt",
            "IGN",
            datetime(2026, 10, 28, 15, 0),
            "Thu, 29 Oct 2026 00:00:00 GMT",
        ],
    )
    conn.execute(
        "INSERT INTO articles VALUES (?, ?, ?, ?)",
        [
            "https://example.com/kr",
            "인벤",
            datetime(2026, 10, 29, 18, 0),
            "2026-10-29 18:00:00",
        ],
    )
    conn.execute(
        "INSERT INTO articles VALUES (?, ?, ?, ?)",
        [
            "https://example.com/ok",
            "IGN",
            datetime(2026, 10, 29, 0, 0),
            "Thu, 29 Oct 2026 00:00:00 GMT",
        ],
    )
    conn.execute(
        "INSERT INTO articles VALUES (?, ?, ?, ?)",
        ["https://example.com/noraw", "인벤", datetime(2026, 10, 29, 1, 0), None],
    )
    conn.close()

    jsonl = repo / "data" / "raw" / "2026-10-29" / "IGN.jsonl"
    jsonl.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "link": "https://example.com/jsonl",
                        "source": "IGN",
                        "published": "2026-10-28T15:00:00+00:00",
                        "published_raw": "Thu, 29 Oct 2026 00:00:00 GMT",
                    }
                ),
                json.dumps(
                    {
                        "link": "https://example.com/odd",
                        "source": "IGN",
                        "published": "2026-10-29T03:00:00+00:00",
                        "published_raw": "Thu, 29 Oct 2026 00:00:00 GMT",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return repo


def test_assess_nine_hour_and_zoneless_rules() -> None:
    shifted = assess_published(
        stored=datetime(2026, 10, 28, 15, 0),
        published_raw="Thu, 29 Oct 2026 00:00:00 GMT",
        default_timezone="UTC",
    )
    assert shifted.status == CORRECTABLE

    zoneless = assess_published(
        stored=datetime(2026, 10, 29, 18, 0),
        published_raw="2026-10-29 18:00:00",
        default_timezone="Asia/Seoul",
    )
    assert zoneless.status == CORRECTABLE

    same = assess_published(
        stored=datetime(2026, 10, 29, 0, 0),
        published_raw="Thu, 29 Oct 2026 00:00:00 GMT",
        default_timezone="UTC",
    )
    assert same.status == ALREADY_CORRECT

    missing = assess_published(
        stored=datetime(2026, 10, 29, 1, 0),
        published_raw=None,
        default_timezone="Asia/Seoul",
    )
    assert missing.status == AMBIGUOUS

    other = assess_published(
        stored=datetime(2026, 10, 29, 3, 0),
        published_raw="Thu, 29 Oct 2026 00:00:00 GMT",
        default_timezone="UTC",
    )
    assert other.status == UNEXPECTED


def test_dry_run_reports_counts_and_does_not_modify(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    db_path = repo / "data" / "radar.duckdb"
    jsonl_path = repo / "data" / "raw" / "2026-10-29" / "IGN.jsonl"
    before_db = db_path.read_bytes()
    before_jsonl = jsonl_path.read_text(encoding="utf-8")

    result = scan_repositories([repo], apply=False)
    report = format_report(result, apply=False)

    assert db_path.read_bytes() == before_db
    assert jsonl_path.read_text(encoding="utf-8") == before_jsonl
    assert "dry-run" in report
    assert "적용한 행: 0" in report
    by_source = {(row.source, row.store): row for row in result.counts}
    assert by_source[("IGN", "duckdb")].correctable == 1
    assert by_source[("IGN", "duckdb")].already_correct == 1
    assert by_source[("인벤", "duckdb")].correctable == 1
    assert by_source[("인벤", "duckdb")].ambiguous == 1
    assert by_source[("IGN", "jsonl")].correctable == 1
    assert by_source[("IGN", "jsonl")].unexpected == 1
    assert result.applied == 0


def test_apply_rewrites_only_correctable_rows(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    result = scan_repositories([repo], apply=True)
    assert result.applied == 3

    conn = duckdb.connect(str(repo / "data" / "radar.duckdb"), read_only=True)
    try:
        rows = {
            row[0]: row[1]
            for row in conn.execute("SELECT link, published FROM articles").fetchall()
        }
    finally:
        conn.close()
    assert rows["https://example.com/gmt"] == datetime(2026, 10, 29, 0, 0)
    assert rows["https://example.com/kr"] == datetime(2026, 10, 29, 9, 0)
    assert rows["https://example.com/ok"] == datetime(2026, 10, 29, 0, 0)
    assert rows["https://example.com/noraw"] == datetime(2026, 10, 29, 1, 0)

    lines = (
        (repo / "data" / "raw" / "2026-10-29" / "IGN.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    first = json.loads(lines[0])
    second = json.loads(lines[1])
    assert first["published"] == "2026-10-29T00:00:00+00:00"
    assert first["published_raw"] == "Thu, 29 Oct 2026 00:00:00 GMT"
    assert second["published"] == "2026-10-29T03:00:00+00:00"

    again = scan_repositories([repo], apply=True)
    assert again.applied == 0


def _cli_main():
    script = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "backfill_published_timezone.py"
    )
    spec = importlib.util.spec_from_file_location("backfill_published_timezone", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.main


def test_cli_defaults_to_dry_run(tmp_path: Path, capsys) -> None:
    repo = _repo(tmp_path)
    db_path = repo / "data" / "radar.duckdb"
    before = db_path.read_bytes()
    code = _cli_main()(["--repo", str(repo)])
    captured = capsys.readouterr()
    assert code == 0
    assert "dry-run" in captured.out
    assert db_path.read_bytes() == before
