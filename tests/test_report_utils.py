from __future__ import annotations

import json
import re
from datetime import datetime, timezone

import pytest

from radar_core.models import Article, CategoryConfig
from radar_core.report_utils import (
    generate_index_html,
    generate_report,
    generate_summary_json,
)


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture()
def fixed_now():
    return datetime(2024, 3, 15, 9, 30, tzinfo=timezone.utc)


@pytest.fixture()
def sample_articles(fixed_now):
    return [
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


@pytest.fixture()
def sample_category():
    return CategoryConfig(
        category_name="game",
        display_name="Game Radar",
        sources=[],
        entities=[],
    )


@pytest.fixture()
def sample_stats():
    return {"sources": 2, "collected": 2, "matched": 2, "window_days": 7}


@pytest.fixture()
def patch_datetime(monkeypatch, fixed_now):
    """Monkeypatch datetime.now to return *fixed_now*."""

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr("radar_core.report_utils.datetime", FixedDateTime)


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


# ── New comprehensive tests ──────────────────────────────────────────────


def test_generate_summary_json_schema(tmp_path) -> None:
    articles = [
        {
            "source": "Reuters",
            "matched_entities": {"Apple": ["apple", "iphone"]},
        },
    ]
    result_path = generate_summary_json(
        category_name="tech",
        articles=articles,
        stats={"article_count": 1, "source_count": 1, "matched_count": 1},
        output_dir=tmp_path,
    )
    payload = json.loads(result_path.read_text(encoding="utf-8"))

    required_fields = {
        "date": str,
        "category": str,
        "article_count": int,
        "source_count": int,
        "matched_count": int,
        "top_entities": list,
        "sources": dict,
        "generated_at": str,
    }
    for field_name, expected_type in required_fields.items():
        assert field_name in payload, f"Missing field: {field_name}"
        assert isinstance(payload[field_name], expected_type), (
            f"{field_name} should be {expected_type.__name__}, got {type(payload[field_name]).__name__}"
        )


def test_generate_summary_json_top_entities_sorted(tmp_path) -> None:
    articles = [
        {"source": "A", "matched_entities": {"Z_rare": ["z"]}},
        {"source": "B", "matched_entities": {"A_common": ["a1", "a2", "a3"]}},
        {"source": "C", "matched_entities": {"M_mid": ["m1", "m2"]}},
        {"source": "D", "matched_entities": {"A_common": ["a4"]}},
    ]
    result_path = generate_summary_json(
        category_name="sort_test",
        articles=articles,
        stats={},
        output_dir=tmp_path,
    )
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    entities = payload["top_entities"]

    assert len(entities) == 3
    counts = [e["count"] for e in entities]
    assert counts == sorted(counts, reverse=True), "top_entities must be sorted DESC"
    assert entities[0]["name"] == "A_common"
    assert entities[0]["count"] == 4


def test_generate_summary_json_empty_articles(tmp_path) -> None:
    result_path = generate_summary_json(
        category_name="empty",
        articles=[],
        stats={},
        output_dir=tmp_path,
    )
    payload = json.loads(result_path.read_text(encoding="utf-8"))

    assert payload["article_count"] == 0
    assert payload["source_count"] == 0
    assert payload["matched_count"] == 0
    assert payload["top_entities"] == []
    assert payload["sources"] == {}
    assert payload["category"] == "empty"
    assert payload["date"]
    assert payload["generated_at"]


def test_generate_report_html_output(
    tmp_path, sample_category, sample_articles, sample_stats, patch_datetime
) -> None:
    output_path = tmp_path / "reports" / "game_report.html"
    generate_report(
        category=sample_category,
        articles=sample_articles,
        output_path=output_path,
        stats=sample_stats,
    )
    html = output_path.read_text(encoding="utf-8")

    expected_canvases = [
        "chartEntities",
        "chartTimeline",
        "chartSources",
        "chartFreshness",
        "chartEntityRate",
        "chartSourceHealth",
    ]
    for canvas_id in expected_canvases:
        assert f'id="{canvas_id}"' in html, f"Missing canvas: {canvas_id}"
    assert "canvas" in html


def test_generate_report_plugin_charts(
    tmp_path, sample_category, sample_articles, sample_stats, patch_datetime
) -> None:
    plugin_charts = {
        "heatmap": {
            "id": "entity-heatmap",
            "title": "Entity Heatmap",
            "config_json": '{"type": "heatmap"}',
        },
        "trend": {
            "id": "trend-line",
            "title": "Trend Analysis",
            "config_json": '{"type": "line"}',
        },
    }
    output_path = tmp_path / "reports" / "game_report.html"
    generate_report(
        category=sample_category,
        articles=sample_articles,
        output_path=output_path,
        stats=sample_stats,
        plugin_charts=plugin_charts,
    )
    html = output_path.read_text(encoding="utf-8")

    assert "Entity Heatmap" in html
    assert "Trend Analysis" in html
    assert 'id="plugin-entity-heatmap"' in html
    assert 'id="plugin-trend-line"' in html
    assert "plugin-charts" in html


def test_generate_report_prev_next(
    tmp_path, sample_category, sample_articles, sample_stats, patch_datetime
) -> None:
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True)
    output_path = report_dir / "game_report.html"

    generate_report(
        category=sample_category,
        articles=sample_articles,
        output_path=output_path,
        stats=sample_stats,
        prev_report="game_20240314.html",
        next_report="game_20240316.html",
    )
    html = output_path.read_text(encoding="utf-8")

    assert 'href="game_20240314.html"' in html
    assert 'href="game_20240316.html"' in html

    output_path2 = report_dir / "game_report_no_nav.html"
    generate_report(
        category=sample_category,
        articles=sample_articles,
        output_path=output_path2,
        stats=sample_stats,
    )
    html_no_nav = output_path2.read_text(encoding="utf-8")
    assert 'disabled aria-disabled="true"' in html_no_nav


def test_generate_report_dated_copy(
    tmp_path, sample_category, sample_articles, sample_stats, patch_datetime
) -> None:
    report_dir = tmp_path / "reports"
    output_path = report_dir / "game_report.html"

    generate_report(
        category=sample_category,
        articles=sample_articles,
        output_path=output_path,
        stats=sample_stats,
    )

    dated_copy = report_dir / "game_20240315.html"
    assert dated_copy.exists()
    assert re.fullmatch(r"game_\d{8}\.html", dated_copy.name)
    assert dated_copy.read_text(encoding="utf-8") == output_path.read_text(
        encoding="utf-8"
    )


def test_generate_index_html_with_summaries(tmp_path) -> None:
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True)

    (report_dir / "tech_20240315.html").write_text(
        "<html>tech</html>", encoding="utf-8"
    )
    (report_dir / "tech_20240315_summary.json").write_text(
        json.dumps(
            {
                "date": "2024-03-15",
                "category": "tech",
                "article_count": 50,
                "source_count": 10,
                "matched_count": 40,
                "top_entities": [{"name": "Apple", "count": 15}],
            }
        ),
        encoding="utf-8",
    )
    (report_dir / "policy_20240314.html").write_text(
        "<html>policy</html>", encoding="utf-8"
    )
    (report_dir / "policy_20240314_summary.json").write_text(
        json.dumps(
            {
                "date": "2024-03-14",
                "category": "policy",
                "article_count": 25,
                "source_count": 5,
                "matched_count": 18,
                "top_entities": [{"name": "Tax", "count": 8}],
            }
        ),
        encoding="utf-8",
    )

    index_path = generate_index_html(report_dir=report_dir, radar_name="Test Radar")
    rendered = index_path.read_text(encoding="utf-8")

    assert "Test Radar" in rendered
    assert "tech_20240315.html" in rendered
    assert "policy_20240314.html" in rendered

    summaries_match = re.search(
        r"summaries:\s*(\[[\s\S]*?\])\s*,\s*generatedAt:", rendered
    )
    assert summaries_match is not None
    summaries = json.loads(summaries_match.group(1))
    assert len(summaries) == 2
    assert summaries[0]["date"] == "2024-03-15"
    assert summaries[0]["article_count"] == 50
    assert summaries[1]["date"] == "2024-03-14"


def test_generate_index_html_no_summaries(tmp_path) -> None:
    report_dir = tmp_path / "empty_reports"

    index_path = generate_index_html(report_dir=report_dir, radar_name="Empty Radar")
    assert index_path.exists()

    rendered = index_path.read_text(encoding="utf-8")
    assert "Empty Radar" in rendered

    summaries_match = re.search(
        r"summaries:\s*(\[[\s\S]*?\])\s*,\s*generatedAt:", rendered
    )
    assert summaries_match is not None
    assert json.loads(summaries_match.group(1)) == []

    reports_match = re.search(r"reports:\s*(\[[\s\S]*?\])\s*,\s*summaries:", rendered)
    assert reports_match is not None
    assert json.loads(reports_match.group(1)) == []


def test_generate_index_html_mixed_date_patterns(tmp_path) -> None:
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True)

    (report_dir / "game_20240315.html").write_text("<html>a</html>", encoding="utf-8")

    advanced_dir = report_dir / "2024-03-16"
    advanced_dir.mkdir()
    (advanced_dir / "index.html").write_text("<html>b</html>", encoding="utf-8")

    index_path = generate_index_html(report_dir=report_dir)
    rendered = index_path.read_text(encoding="utf-8")

    assert "game_20240315.html" in rendered
    assert "2024-03-16/index.html" in rendered

    reports_match = re.search(r"reports:\s*(\[[\s\S]*?\])\s*,\s*summaries:", rendered)
    assert reports_match is not None
    reports = json.loads(reports_match.group(1))
    dates = [r["date"] for r in reports if r["date"]]
    assert "2024-03-16" in dates
    assert "2024-03-15" in dates
    assert dates == sorted(dates, reverse=True)
