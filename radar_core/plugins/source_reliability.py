"""Plotly source reliability plugin — horizontal bar chart of source success rates.

Universal plugin that works with any Radar repo. Queries crawl_health table
for source success/failure counts, or falls back to article source distribution.
"""

from __future__ import annotations

from typing import Any


def _query_crawl_health(store: Any) -> list[dict] | None:
    """Query crawl_health table for source success/failure data."""
    try:
        with store._connection() as conn:
            rows = conn.execute("""
                SELECT
                    source_id,
                    COALESCE(SUM(success_count), 0) AS successes,
                    COALESCE(SUM(failure_count), 0) AS failures
                FROM crawl_health
                GROUP BY source_id
                ORDER BY (COALESCE(SUM(success_count), 0) + COALESCE(SUM(failure_count), 0)) DESC
                LIMIT 20
            """).fetchall()

        if not rows:
            return None

        results = []
        for source_id, successes, failures in rows:
            total = successes + failures
            if total == 0:
                continue
            rate = (successes / total) * 100
            results.append(
                {
                    "source": str(source_id),
                    "success": int(successes),
                    "failure": int(failures),
                    "total": int(total),
                    "rate": round(rate, 1),
                }
            )
        return results if results else None

    except Exception:
        return None


def _query_article_sources(store: Any) -> list[dict] | None:
    """Fallback: query article source distribution."""
    try:
        with store._connection() as conn:
            # Try common table names
            for table in ("articles", "urls"):
                try:
                    rows = conn.execute(f"""
                        SELECT source, COUNT(*) AS cnt
                        FROM {table}
                        WHERE source IS NOT NULL AND source != ''
                        GROUP BY source
                        ORDER BY cnt DESC
                        LIMIT 15
                    """).fetchall()
                    if rows:
                        return [
                            {
                                "source": str(r[0]),
                                "success": int(r[1]),
                                "failure": 0,
                                "total": int(r[1]),
                                "rate": 100.0,
                            }
                            for r in rows
                        ]
                except Exception:
                    continue
        return None
    except Exception:
        return None


def get_chart_config(store: Any = None, articles: Any = None) -> dict | None:
    """Generate Plotly source reliability chart config for plugin slot.

    Args:
        store: RadarStorage or GraphStore instance with _connection() method.
        articles: Unused (kept for API compatibility).

    Returns:
        Plugin chart config dict with id, title, config_json, or None on failure.
    """
    try:
        if store is None:
            return None

        # Try crawl_health first, then fallback to article sources
        data = _query_crawl_health(store)
        if data is None:
            data = _query_article_sources(store)
        if not data:
            return None

        import plotly.graph_objects as go
        import plotly.io as pio

        sources = [d["source"] for d in data]
        rates = [d["rate"] for d in data]
        totals = [d["total"] for d in data]

        # Color by rate: green (>80%), yellow (50-80%), red (<50%)
        colors = []
        for rate in rates:
            if rate >= 80:
                colors.append("rgba(34, 197, 94, 0.8)")
            elif rate >= 50:
                colors.append("rgba(245, 158, 11, 0.8)")
            else:
                colors.append("rgba(239, 68, 68, 0.8)")

        hover_texts = [
            f"{d['source']}<br>"
            f"성공률: {d['rate']}%<br>"
            f"성공: {d['success']}건 / 실패: {d['failure']}건<br>"
            f"총: {d['total']}건"
            for d in data
        ]

        fig = go.Figure(
            data=go.Bar(
                x=rates,
                y=sources,
                orientation="h",
                marker_color=colors,
                text=[f"{r:.0f}%" for r in rates],
                textposition="auto",
                hovertext=hover_texts,
                hoverinfo="text",
            )
        )

        chart_height = max(300, len(sources) * 30)

        fig.update_layout(
            height=chart_height,
            margin={"l": 140, "r": 20, "t": 24, "b": 40},
            paper_bgcolor="rgba(10,14,23,0)",
            plot_bgcolor="rgba(14,22,42,0.5)",
            font={"color": "#e9eefb"},
            xaxis={
                "title": "성공률 (%)",
                "color": "#e9eefb",
                "gridcolor": "rgba(233,238,251,0.1)",
                "range": [0, 105],
            },
            yaxis={
                "title": "",
                "color": "#e9eefb",
                "gridcolor": "rgba(233,238,251,0.1)",
                "autorange": "reversed",
            },
            bargap=0.2,
        )

        config_json = pio.to_html(fig, full_html=False, include_plotlyjs="cdn")

        return {
            "id": "source_reliability",
            "title": "소스 신뢰도 (Source Reliability)",
            "config_json": config_json,
        }

    except Exception:
        return None
