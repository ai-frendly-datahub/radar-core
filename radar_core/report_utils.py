"""Report generation utilities for Radar data collection platform.

This module provides functions to generate HTML reports, index pages, and summary JSON
from collected articles and statistics.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from radar_core.models import Article, CategoryConfig


def generate_report(
    *,
    category: CategoryConfig,
    articles: Iterable[Article],
    output_path: Path,
    stats: dict[str, int],
    errors: list[str] | None = None,
    plugin_charts: dict[str, Any] | None = None,
) -> Path:
    """Generate an HTML report from collected articles.

    Renders a Jinja2 template with article data, statistics, and optional plugin charts.
    Creates both a timestamped version (e.g., category_20240315.html) and a main report file.

    Args:
        category: CategoryConfig object containing category metadata and configuration.
        articles: Iterable of Article objects to include in the report.
        output_path: Path where the main report HTML file will be written.
        stats: Dictionary of statistics (e.g., {"total_articles": 42, "new_articles": 10}).
        errors: Optional list of error messages encountered during collection/analysis.
        plugin_charts: Optional dictionary of pre-rendered chart HTML from plugins.

    Returns:
        Path to the generated report file.

    Raises:
        IOError: If the output directory cannot be created or file cannot be written.
    """
    ...


def generate_index_html(
    report_dir: Path,
    summaries_dir: Path | None = None,
) -> Path:
    """Generate an index.html listing all available report files.

    Scans the report directory for HTML files and creates an index page that lists
    both dated reports (e.g., category_20240315.html) and latest reports.
    Optionally includes links to summary JSON files if summaries_dir is provided.

    Args:
        report_dir: Directory containing report HTML files.
        summaries_dir: Optional directory containing summary JSON files to link from index.

    Returns:
        Path to the generated index.html file.

    Raises:
        IOError: If the report directory cannot be created or index file cannot be written.
    """
    ...


def generate_summary_json(
    category_name: str,
    articles: list[dict],
    stats: dict[str, int],
    output_dir: Path,
) -> Path:
    """Generate a JSON summary of articles and statistics.

    Creates a structured JSON file containing article metadata, entity counts,
    and collection statistics for programmatic access and archival.

    Args:
        category_name: Category name to include in the summary and output filename.
        articles: List of article dictionaries to summarize.
        stats: Dictionary of statistics to include in the summary.
        output_dir: Directory where the summary JSON file will be written.

    Returns:
        Path to the generated summary JSON file.

    Raises:
        IOError: If the output directory cannot be created or file cannot be written.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    source_counts: Counter[str] = Counter()
    entity_counts: Counter[str] = Counter()
    matched_count = 0

    for article in articles:
        source = article.get("source")
        if isinstance(source, str) and source:
            source_counts[source] += 1

        matched_entities = article.get("matched_entities")
        if isinstance(matched_entities, list):
            if matched_entities:
                matched_count += 1
            for entity_name in matched_entities:
                if isinstance(entity_name, str) and entity_name:
                    entity_counts[entity_name] += 1
        elif isinstance(matched_entities, dict):
            if matched_entities:
                matched_count += 1
            for entity_name, keywords in matched_entities.items():
                if not isinstance(entity_name, str) or not entity_name:
                    continue
                if isinstance(keywords, list):
                    entity_counts[entity_name] += len(keywords)
                else:
                    entity_counts[entity_name] += 1

    now = datetime.now(timezone.utc).astimezone()
    date_stamp = now.strftime("%Y%m%d")

    summary = {
        "date": now.date().isoformat(),
        "category": category_name,
        "article_count": int(stats.get("article_count", len(articles))),
        "source_count": int(stats.get("source_count", len(source_counts))),
        "matched_count": int(stats.get("matched_count", matched_count)),
        "top_entities": [
            {"name": name, "count": count}
            for name, count in entity_counts.most_common(20)
        ],
        "sources": dict(source_counts),
        "generated_at": now.isoformat(),
    }

    output_path = output_dir / f"{category_name}_{date_stamp}_summary.json"
    output_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path
