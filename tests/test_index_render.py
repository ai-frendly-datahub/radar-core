"""Test script to verify index.html Jinja2 template renders correctly.

This script renders the template with test data and validates that all
required sections are present.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape


def get_template_dir() -> Path:
    """Get the templates directory path."""
    return Path(__file__).parent.parent / "radar_core" / "templates"


def render_test_template() -> str:
    """Render the index.html template with test data.

    Returns:
        The rendered HTML string.
    """
    template_dir = get_template_dir()
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("index.html")

    # Test data matching generate_index_html() data contract
    test_data = {
        "radar_name": "TestRadar",
        "reports": [
            {
                "filename": "test_category_20240315.html",
                "display_name": "Test Category",
                "date": "2024-03-15",
            },
            {
                "filename": "test_category_20240314.html",
                "display_name": "Test Category",
                "date": "2024-03-14",
            },
            {
                "filename": "test_category_20240313.html",
                "display_name": "Test Category",
                "date": "2024-03-13",
            },
            {
                "filename": "latest_report.html",
                "display_name": "Latest Report",
                "date": "",  # No date for latest reports
            },
        ],
        "summaries_json": [
            {
                "date": "2024-03-15",
                "category": "test_category",
                "article_count": 42,
                "source_count": 8,
                "matched_count": 35,
                "top_entities": [
                    {"name": "Entity One", "count": 15},
                    {"name": "Entity Two", "count": 12},
                    {"name": "Entity Three", "count": 8},
                ],
                "sources": {"Source A": 20, "Source B": 22},
                "generated_at": "2024-03-15T12:00:00+00:00",
            },
            {
                "date": "2024-03-14",
                "category": "test_category",
                "article_count": 38,
                "source_count": 7,
                "matched_count": 30,
                "top_entities": [
                    {"name": "Entity One", "count": 10},
                    {"name": "Entity Four", "count": 9},
                ],
                "sources": {"Source A": 18, "Source C": 20},
                "generated_at": "2024-03-14T12:00:00+00:00",
            },
            {
                "date": "2024-03-13",
                "category": "test_category",
                "article_count": 25,
                "source_count": 5,
                "matched_count": 20,
                "top_entities": [
                    {"name": "Entity Five", "count": 7},
                ],
                "sources": {"Source B": 25},
                "generated_at": "2024-03-13T12:00:00+00:00",
            },
        ],
        "generated_at": datetime.now(timezone.utc),
    }

    return template.render(**test_data)


def validate_template_sections(html: str) -> list[str]:
    """Validate that all required sections are present in the rendered HTML.

    Args:
        html: The rendered HTML string.

    Returns:
        List of missing section identifiers (empty if all present).
    """
    required_sections = [
        # Header section - radar_name is rendered, check for the test value
        ("radar_name", "TestRadar"),
        # CDN dependencies
        ("flatpickr_css", "flatpickr@4.6.13/dist/flatpickr.min.css"),
        ("flatpickr_js", "flatpickr@4.6.13/dist/flatpickr.min.js"),
        ("chart_js", "chart.js@4.4.3/dist/chart.umd.min.js"),
        ("pretendard_font", "pretendard@1.3.9"),
        # Calendar widget
        ("calendar_container", 'id="calendar-container"'),
        # Trend chart
        ("trend_chart", 'id="trend-chart"'),
        # Search bar
        ("search_input", 'id="search-input"'),
        # Report list
        ("reports_grid", 'id="reports-grid"'),
        ("report_card", 'class="report-card"'),
        # TODO markers for Tasks 7 and 8
        ("todo_task7", "TODO: Task 7"),
        ("todo_task8", "TODO: Task 8"),
        # Report dates from test data
        ("report_date_1", "2024-03-15"),
        ("report_date_2", "2024-03-14"),
        ("report_latest", "Latest Report"),
    ]

    missing = []
    for section_id, marker in required_sections:
        if marker not in html:
            missing.append(section_id)

    return missing


def test_index_template_renders() -> None:
    """Test that index.html template renders without errors."""
    html = render_test_template()

    # Basic sanity checks
    assert len(html) > 1000, "Template output is too short"
    assert "<!doctype html>" in html.lower(), "Missing doctype"
    assert "</html>" in html.lower(), "Missing closing html tag"

    # Validate all required sections
    missing = validate_template_sections(html)
    assert not missing, f"Missing sections: {missing}"

    print("✓ Template renders successfully")
    print(f"✓ Output size: {len(html)} characters")
    print("✓ All required sections present")


def test_index_template_empty_reports() -> None:
    """Test that template handles empty reports list gracefully."""
    template_dir = get_template_dir()
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("index.html")

    html = template.render(
        radar_name="EmptyRadar",
        reports=[],
        summaries_json=[],
        generated_at=datetime.now(timezone.utc),
    )

    assert "No reports available" in html, "Missing empty state message"
    assert "0" in html, "Missing zero count for empty reports"
    print("✓ Empty reports case handled correctly")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing index.html Jinja2 template")
    print("=" * 60)

    test_index_template_renders()
    test_index_template_empty_reports()

    print("=" * 60)
    print("All tests passed!")
    print("=" * 60)

    # Save rendered output for inspection
    output_path = (
        Path(__file__).parent.parent.parent
        / ".sisyphus"
        / "evidence"
        / "task-3-index-template.txt"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    html = render_test_template()
    output_path.write_text(html, encoding="utf-8")
    print(f"\nRendered template saved to: {output_path}")
