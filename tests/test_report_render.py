"""Test script to verify report.html Jinja2 template renders correctly.

This script creates test data and renders the report template to verify:
1. All required sections are present
2. All 6 Chart.js canvas elements exist
3. Plugin slot exists with conditional rendering
4. Prev/Next navigation buttons exist
5. CSV export button exists
6. All Jinja2 variables are correctly interpolated
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader


def create_test_data() -> dict[str, object]:
    """Create test data for template rendering."""

    # Mock category
    class MockCategory:
        display_name = "Test Category"
        category_name = "test_category"

    # Mock article
    class MockArticle:
        title = "Test Article Title"
        link = "https://example.com/article/1"
        source = "TestSource"
        summary = "This is a test article summary for verification."
        published = datetime(2025, 3, 15, 10, 30, tzinfo=UTC)
        matched_entities = {
            "Entity1": ["keyword1"],
            "Entity2": ["keyword2", "keyword3"],
        }

    # Build test data
    articles = [MockArticle()]
    articles_json = [
        {
            "title": a.title,
            "link": a.link,
            "source": a.source,
            "published": a.published.isoformat() if a.published else None,
            "published_at": a.published.isoformat() if a.published else None,
            "summary": a.summary,
            "matched_entities": a.matched_entities or {},
            "collected_at": datetime.now(UTC).isoformat(),
        }
        for a in articles
    ]

    entity_counts: Counter[str] = Counter()
    entity_counts["Entity1"] = 5
    entity_counts["Entity2"] = 3
    entity_counts["Entity3"] = 8

    stats = {
        "sources": 3,
        "collected": 10,
        "matched": 7,
        "window_days": 7,
    }

    # Plugin charts (for Advanced Tier)
    plugin_charts = [
        {
            "id": "heatmap",
            "title": "Time Pattern Heatmap",
            "config_json": json.dumps({"type": "heatmap", "data": []}),
        }
    ]

    return {
        "category": MockCategory(),
        "articles": articles,
        "articles_json": articles_json,
        "generated_at": datetime.now(UTC),
        "stats": stats,
        "entity_counts": entity_counts,
        "errors": ["Test error message"],
        "plugin_charts": plugin_charts,
        "prev_report": "test_category_20250315.html",
        "next_report": None,  # Test disabled state
    }


def test_report_render() -> None:
    """Test that report.html template renders without errors."""
    template_dir = Path(__file__).parent.parent / "radar_core" / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=False)

    template = env.get_template("report.html")
    data = create_test_data()
    rendered = template.render(**data)

    # Verify essential sections exist
    assert "<title>Test Category - Report</title>" in rendered, "Title not rendered"
    assert "Test Category" in rendered, "Category name not in output"
    assert "Test Article Title" in rendered, "Article title not in output"
    assert "TestSource" in rendered, "Source name not in output"

    # Verify all 6 chart canvas IDs
    assert 'id="chartEntities"' in rendered, "chartEntities canvas missing"
    assert 'id="chartTimeline"' in rendered, "chartTimeline canvas missing"
    assert 'id="chartSources"' in rendered, "chartSources canvas missing"
    assert 'id="chartFreshness"' in rendered, "chartFreshness canvas missing"
    assert 'id="chartEntityRate"' in rendered, "chartEntityRate canvas missing"
    assert 'id="chartSourceHealth"' in rendered, "chartSourceHealth canvas missing"

    # Verify plugin charts slot
    assert 'id="plugin-charts"' in rendered, "plugin-charts container missing"
    assert 'id="plugin-heatmap"' in rendered, "plugin chart instance missing"
    assert "Time Pattern Heatmap" in rendered, "plugin chart title missing"

    # Verify prev/next navigation
    assert "Previous Day" in rendered, "Previous day button missing"
    assert "Next Day" in rendered, "Next day button missing"
    assert 'href="test_category_20250315.html"' in rendered, "Prev report link missing"
    assert 'aria-disabled="true"' in rendered, (
        "Disabled next button missing aria-disabled"
    )

    # Verify CSV export button
    assert 'id="export-csv"' in rendered, "Export CSV button missing"
    assert "Export CSV" in rendered, "Export CSV text missing"

    # Verify stats
    assert "3" in rendered, "Sources count missing"
    assert "10" in rendered, "Collected count missing"
    assert "7" in rendered, "Matched count missing"
    assert "7d" in rendered, "Window days missing"

    # Verify error section
    assert "Errors detected" in rendered, "Error section missing"
    assert "Test error message" in rendered, "Error message not in output"

    # Verify entity pills
    assert "entity-pill" in rendered, "Entity pills class missing"
    assert "Entity1" in rendered, "Entity1 not in pills"
    assert "Entity2" in rendered, "Entity2 not in pills"

    # Verify data scripts
    assert 'id="articles-data"' in rendered, "articles-data script missing"
    assert 'id="entities-data"' in rendered, "entities-data script missing"

    print("All report.html template tests passed!")
    print(f"Rendered output length: {len(rendered)} characters")


def test_report_render_without_plugin_charts() -> None:
    """Test that report.html renders correctly without plugin charts (Standard Tier)."""
    template_dir = Path(__file__).parent.parent / "radar_core" / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=False)

    template = env.get_template("report.html")
    data = create_test_data()
    data["plugin_charts"] = []  # Empty for Standard Tier
    rendered = template.render(**data)

    # Plugin container should exist but be empty
    assert 'id="plugin-charts"' in rendered, "plugin-charts container missing"
    # No plugin chart content should be present
    assert "plugin-heatmap" not in rendered, "Plugin chart should not be present"

    print("Standard Tier (no plugin charts) test passed!")


def test_report_render_without_navigation() -> None:
    """Test that report.html handles missing prev/next reports."""
    template_dir = Path(__file__).parent.parent / "radar_core" / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=False)

    template = env.get_template("report.html")
    data = create_test_data()
    data["prev_report"] = None
    data["next_report"] = None
    rendered = template.render(**data)

    # Both buttons should be disabled
    assert rendered.count('aria-disabled="true"') >= 2, (
        "Both nav buttons should be disabled"
    )

    print("Missing navigation test passed!")


if __name__ == "__main__":
    test_report_render()
    test_report_render_without_plugin_charts()
    test_report_render_without_navigation()
    print("\nAll tests completed successfully!")
