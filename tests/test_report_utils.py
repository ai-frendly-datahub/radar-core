from __future__ import annotations

import json
import re
from datetime import datetime, timezone

from radar_core.models import Article, CategoryConfig
from radar_core.report_utils import (
    generate_index_html,
    generate_report,
    generate_summary_json,
)


def test_generate_summary_json(tmp_path) -> None:
    output_dir = tmp_path / "summaries"
    articles = [
        {
            "source": "YTN",
            "matched_entities": ["Nintendo", "PlayStation", "Nintendo"],
        },
        {
            "source": "MBC",
            "matched_entities": ["Nintendo", "Xbox"],
        },
        {
            "source": "YTN",
            "matched_entities": [],
        },
        {
            "source": "KBS",
        },
        {
            "source": "MBC",
            "matched_entities": ["PlayStation"],
        },
    ]

    result_path = generate_summary_json(
        category_name="game",
        articles=articles,
        stats={"article_count": 42, "source_count": 8, "matched_count": 35},
        output_dir=output_dir,
    )

    assert result_path.exists()
    assert re.fullmatch(r"game_\d{8}_summary\.json", result_path.name)

    payload = json.loads(result_path.read_text(encoding="utf-8"))
    assert payload["date"] == datetime.fromisoformat(payload["date"]).date().isoformat()
    assert (
        payload["generated_at"]
        == datetime.fromisoformat(payload["generated_at"]).isoformat()
    )
    assert datetime.fromisoformat(payload["generated_at"]).tzinfo is not None

    assert payload["category"] == "game"
    assert payload["article_count"] == 42
    assert payload["source_count"] == 8
    assert payload["matched_count"] == 35
    assert payload["sources"] == {"YTN": 2, "MBC": 2, "KBS": 1}
    assert payload["top_entities"] == [
        {"name": "Nintendo", "count": 3},
        {"name": "PlayStation", "count": 2},
        {"name": "Xbox", "count": 1},
    ]
    assert payload["top_entities"] == sorted(
        payload["top_entities"],
        key=lambda entity: entity["count"],
        reverse=True,
    )

    empty_path = generate_summary_json(
        category_name="game",
        articles=[],
        stats={},
        output_dir=output_dir,
    )
    empty_payload = json.loads(empty_path.read_text(encoding="utf-8"))
    assert empty_payload["article_count"] == 0
    assert empty_payload["source_count"] == 0
    assert empty_payload["matched_count"] == 0
    assert empty_payload["top_entities"] == []
    assert empty_payload["sources"] == {}

    no_entities_path = generate_summary_json(
        category_name="game",
        articles=[{"source": "YTN"}, {"source": "MBC", "matched_entities": []}],
        stats={},
        output_dir=output_dir,
    )
    no_entities_payload = json.loads(no_entities_path.read_text(encoding="utf-8"))
    assert no_entities_payload["top_entities"] == []
    assert no_entities_payload["matched_count"] == 0
    assert no_entities_payload["sources"] == {"YTN": 1, "MBC": 1}


def test_generate_report(tmp_path, monkeypatch) -> None:
    fixed_now = datetime(2024, 3, 15, 9, 30, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr("radar_core.report_utils.datetime", FixedDateTime)

    category = CategoryConfig(
        category_name="game",
        display_name="Game Radar",
        sources=[],
        entities=[],
    )
    articles = [
        Article(
            title="Zelda Launch",
            link="https://example.com/zelda",
            summary="Nintendo released a new Zelda trailer.",
            published=fixed_now,
            source="IGN",
            category="game",
            matched_entities={"Nintendo": ["nintendo", "zelda"]},
            collected_at=fixed_now,
        ),
        Article(
            title="Xbox Update",
            link="https://example.com/xbox",
            summary="Xbox announced spring update.",
            published=None,
            source="GameSpot",
            category="game",
            matched_entities={"Xbox": ["xbox"]},
            collected_at=fixed_now,
        ),
    ]

    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True)
    (report_dir / "game_20240314.html").write_text("prev", encoding="utf-8")
    (report_dir / "game_20240316.html").write_text("next", encoding="utf-8")

    output_path = report_dir / "game_report.html"
    result_path = generate_report(
        category=category,
        articles=articles,
        output_path=output_path,
        stats={"sources": 2, "collected": 2, "matched": 2, "window_days": 7},
        errors=["source timeout"],
        plugin_charts={
            "heatmap": {
                "id": "entity-heatmap",
                "title": "Entity Heatmap",
                "config_json": "{}",
            }
        },
    )

    assert result_path == output_path
    assert output_path.exists()

    dated_copy = report_dir / "game_20240315.html"
    assert dated_copy.exists()

    report_html = output_path.read_text(encoding="utf-8")
    assert "Game Radar" in report_html
    assert "Zelda Launch" in report_html
    assert "Entity Heatmap" in report_html
    assert 'href="game_20240314.html"' in report_html
    assert 'href="game_20240316.html"' in report_html
    assert "source timeout" in report_html
    assert "2024-03-15 09:30 UTC" in report_html
    assert '"title": "Zelda Launch"' in report_html

    summary_path = report_dir / "game_20240315_summary.json"
    assert summary_path.exists()
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["category"] == "game"
    assert summary_payload["article_count"] == 2
    assert summary_payload["source_count"] == 2
    assert summary_payload["matched_count"] == 2
    assert summary_payload["sources"] == {"IGN": 1, "GameSpot": 1}
    assert summary_payload["top_entities"] == [
        {"name": "Nintendo", "count": 2},
        {"name": "Xbox", "count": 1},
    ]


def test_generate_index_html(tmp_path) -> None:
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True)

    (report_dir / "game_20260315.html").write_text("game", encoding="utf-8")
    (report_dir / "policy_20260314.html").write_text("policy", encoding="utf-8")
    (report_dir / "latest_report.html").write_text("latest", encoding="utf-8")
    (report_dir / "index.html").write_text("old index", encoding="utf-8")

    advanced_dir = report_dir / "2026-03-16"
    advanced_dir.mkdir()
    (advanced_dir / "index.html").write_text("advanced", encoding="utf-8")

    (report_dir / "misc" / "index.html").parent.mkdir()
    (report_dir / "misc" / "index.html").write_text("ignored", encoding="utf-8")

    (report_dir / "game_20260315_summary.json").write_text(
        json.dumps(
            {
                "date": "2026-03-15",
                "category": "game",
                "article_count": 42,
                "source_count": 8,
                "matched_count": 35,
                "top_entities": [{"name": "Nintendo", "count": 12}],
            }
        ),
        encoding="utf-8",
    )
    (report_dir / "policy_20260314_summary.json").write_text(
        json.dumps(
            {
                "date": "2026-03-14",
                "category": "policy",
                "article_count": 30,
                "source_count": 6,
                "matched_count": 20,
                "top_entities": [{"name": "Tax", "count": 5}],
            }
        ),
        encoding="utf-8",
    )

    index_path = generate_index_html(report_dir=report_dir, radar_name="Unified Radar")

    assert index_path == report_dir / "index.html"
    assert index_path.exists()

    rendered = index_path.read_text(encoding="utf-8")
    assert "Unified Radar" in rendered
    assert "game_20260315.html" in rendered
    assert "policy_20260314.html" in rendered
    assert "2026-03-16/index.html" in rendered
    assert "latest_report.html" in rendered
    assert "misc/index.html" not in rendered
    assert "old index" not in rendered

    reports_match = re.search(r"reports:\s*(\[[\s\S]*?\])\s*,\s*summaries:", rendered)
    assert reports_match is not None
    reports = json.loads(reports_match.group(1))
    assert reports[0]["filename"] == "2026-03-16/index.html"
    assert reports[0]["date"] == "2026-03-16"
    assert reports[1]["filename"] == "game_20260315.html"
    assert reports[1]["date_label"] == "2026-03-15"
    assert reports[2]["filename"] == "policy_20260314.html"
    latest_entry = next(
        report for report in reports if report["filename"] == "latest_report.html"
    )
    assert latest_entry["date"] == ""
    assert latest_entry["date_label"] == ""

    summaries_match = re.search(
        r"summaries:\s*(\[[\s\S]*?\])\s*,\s*generatedAt:", rendered
    )
    assert summaries_match is not None
    summaries = json.loads(summaries_match.group(1))
    assert [item["date"] for item in summaries] == ["2026-03-15", "2026-03-14"]
    assert summaries[0]["category"] == "game"
    assert summaries[0]["article_count"] == 42
    assert summaries[0]["top_entities"] == [{"name": "Nintendo", "count": 12}]

    empty_dir = tmp_path / "empty"
    empty_index = generate_index_html(report_dir=empty_dir)
    empty_rendered = empty_index.read_text(encoding="utf-8")
    empty_summaries_match = re.search(
        r"summaries:\s*(\[[\s\S]*?\])\s*,\s*generatedAt:", empty_rendered
    )
    assert empty_summaries_match is not None
    assert json.loads(empty_summaries_match.group(1)) == []
