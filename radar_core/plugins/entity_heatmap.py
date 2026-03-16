"""Plotly entity heatmap plugin — top 15 entities × 14 days.

Universal plugin that works with any Radar repo. Extracts entities from
article titles and displays frequency over time as a heatmap.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any


_STOPWORDS = frozenset(
    {
        "이",
        "그",
        "저",
        "수",
        "것",
        "등",
        "및",
        "또",
        "더",
        "한",
        "할",
        "위",
        "위한",
        "통해",
        "대한",
        "관련",
        "오늘",
        "내일",
        "어제",
        "의",
        "를",
        "을",
        "에",
        "가",
        "은",
        "는",
        "로",
        "으로",
        "와",
        "과",
        "도",
        "만",
        "부터",
        "까지",
        "에서",
        "the",
        "a",
        "an",
        "is",
        "are",
        "was",
        "were",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "and",
        "or",
    }
)


def _extract_date(article: Any) -> datetime | None:
    """Extract date from article object, trying common attribute names."""
    for attr in ("collected_at", "published_at", "published", "date"):
        val = getattr(article, attr, None)
        if val is None:
            continue
        if isinstance(val, datetime):
            return val
        if isinstance(val, str) and val:
            for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(val[:19], fmt).replace(tzinfo=UTC)
                except (ValueError, IndexError):
                    continue
    return None


def _extract_entities_from_articles(articles: list[Any]) -> dict[str, dict[str, int]]:
    """Extract entity-date frequency matrix from articles.

    Returns {entity_name: {date_str: count}}.
    """
    entity_dates: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for article in articles:
        dt = _extract_date(article)
        if dt is None:
            continue
        date_str = dt.strftime("%Y-%m-%d")

        # Try top_entities attribute first (from summary data)
        top_entities = getattr(article, "top_entities", None)
        if top_entities and isinstance(top_entities, list):
            for ent in top_entities:
                name = ent.get("name", "") if isinstance(ent, dict) else str(ent)
                if name and len(name) >= 2:
                    entity_dates[name][date_str] += 1
            continue

        # Try matched_entities attribute
        matched = getattr(article, "matched_entities", None)
        if matched:
            if isinstance(matched, dict):
                for name in matched:
                    if name and len(name) >= 2:
                        entity_dates[name][date_str] += 1
                continue
            if isinstance(matched, list):
                for name in matched:
                    if isinstance(name, str) and name and len(name) >= 2:
                        entity_dates[name][date_str] += 1
                continue

        # Fallback: extract words from title
        title = getattr(article, "title", "") or ""
        if not title:
            continue
        words = title.split()
        for word in words:
            cleaned = word.strip(".,!?;:()[]\"'''")
            if len(cleaned) >= 2 and cleaned.lower() not in _STOPWORDS:
                entity_dates[cleaned][date_str] += 1

    return dict(entity_dates)


def get_chart_config(store: Any = None, articles: Any = None) -> dict | None:
    """Generate Plotly entity heatmap chart config for plugin slot.

    Args:
        store: Unused (kept for API compatibility).
        articles: List of article objects with title and date attributes.

    Returns:
        Plugin chart config dict with id, title, config_json, or None on failure.
    """
    try:
        if not articles:
            return None

        articles_list = list(articles)
        if not articles_list:
            return None

        entity_dates = _extract_entities_from_articles(articles_list)
        if not entity_dates:
            return None

        # Get top 15 entities by total frequency
        entity_totals = {
            name: sum(dates.values()) for name, dates in entity_dates.items()
        }
        top_entities = sorted(
            entity_totals, key=lambda n: entity_totals[n], reverse=True
        )[:15]

        if not top_entities:
            return None

        # Build date range: last 14 days
        today = datetime.now(tz=UTC).date()
        date_range = [(today - timedelta(days=13 - i)).isoformat() for i in range(14)]

        # Build matrix: rows = entities, columns = dates
        matrix = []
        for entity in top_entities:
            row = [entity_dates.get(entity, {}).get(d, 0) for d in date_range]
            matrix.append(row)

        max_val = max(max(row) for row in matrix) if matrix else 0
        if max_val == 0:
            return None

        import plotly.graph_objects as go
        import plotly.io as pio

        x_labels = [d[5:] for d in date_range]  # MM-DD format

        fig = go.Figure(
            data=go.Heatmap(
                z=matrix,
                x=x_labels,
                y=top_entities,
                colorscale="Blues",
                showscale=True,
                hovertemplate=(
                    "엔티티: %{y}<br>날짜: %{x}<br>빈도: %{z}건<extra></extra>"
                ),
            )
        )

        fig.update_layout(
            height=400,
            margin={"l": 120, "r": 20, "t": 24, "b": 40},
            paper_bgcolor="rgba(10,14,23,0)",
            plot_bgcolor="rgba(14,22,42,0.5)",
            font={"color": "#e9eefb"},
            xaxis={
                "title": "날짜",
                "color": "#e9eefb",
                "gridcolor": "rgba(233,238,251,0.1)",
            },
            yaxis={
                "title": "",
                "color": "#e9eefb",
                "gridcolor": "rgba(233,238,251,0.1)",
                "autorange": "reversed",
            },
        )

        config_json = pio.to_html(fig, full_html=False, include_plotlyjs="cdn")

        return {
            "id": "entity_heatmap",
            "title": "엔티티 히트맵 (Top 15 × 14일)",
            "config_json": config_json,
        }

    except Exception:
        return None
