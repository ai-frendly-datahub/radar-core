"""Report generation utilities for Radar data collection platform.

This module provides functions to generate HTML reports, index pages, and summary JSON
from collected articles and statistics.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from jinja2 import Environment, FileSystemLoader

from radar_core.jsonutil import json_default, json_dumps
from radar_core.models import Article, CategoryConfig

# Callers pass ``recent_articles`` (default 7 days). ``sources`` counts that
# rolling window, not the articles fetched in the current execution.
_DEFAULT_SUMMARY_WINDOW_DAYS = 7
_RUN_ERROR_LIMIT = 240
_SENSITIVE_HEADER = re.compile(
    r"(?i)\b("
    r"authorization|proxy-authorization|cookie|set-cookie|"
    r"x-api-key|api-key|x-auth-token|x-amz-security-token"
    r")\b"
    r"\s*[:=]\s*"
    r"(?:bearer\s+)?"
    r"(?:\"[^\"]*\"|'[^']*'|\S+(?:\s*;\s*\S+)*)"
)
_BEARER = re.compile(r"(?i)\bbearer\s+\S+")
_QUERY_SECRET = re.compile(
    r"(?i)((?:(?:service|crtfc|api)[_-]?key|(?:access|refresh|id)[_-]?token)=)"
    r"(?:\"[^\"]*\"|'[^']*'|[^&\s\"']+)"
)
_URL_USERINFO = re.compile(r"(://[^/\s:@]+:)([^@/\s]+)@")


def generate_report(
    *,
    category: CategoryConfig,
    articles: Iterable[Article],
    output_path: Path,
    stats: dict[str, int],
    errors: list[str] | None = None,
    plugin_charts: dict[str, Any] | None = None,
    extra_sections: Iterable[dict[str, Any]] | None = None,
    prev_report: str | None = None,
    next_report: str | None = None,
    ontology_metadata: Mapping[str, Any] | None = None,
    run_collection: Mapping[str, Any] | None = None,
) -> Path:
    """Generate an HTML report from collected articles.

    Renders a Jinja2 template with article data, statistics, and optional plugin charts.
    Creates both a timestamped version (e.g., category_20240315.html) and a main report file.

    ``run_collection`` is optional and is forwarded to :func:`generate_summary_json`.
    Existing callers that omit it keep the previous summary JSON.

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
    output_path.parent.mkdir(parents=True, exist_ok=True)

    articles_list = list(articles)

    articles_json: list[dict[str, Any]] = []
    entity_counts: Counter[str] = Counter()
    for article in articles_list:
        published_at = getattr(article, "published_at", None)
        if published_at is None:
            published_at = getattr(article, "published", None)

        collected_at = getattr(article, "collected_at", None)
        matched_entities_raw: Any = article.matched_entities or {}

        articles_json.append(
            {
                "title": article.title,
                "link": article.link,
                "source": article.source,
                "summary": article.summary,
                "published_at": (
                    published_at.isoformat()
                    if isinstance(published_at, datetime)
                    else None
                ),
                "collected_at": (
                    collected_at.isoformat()
                    if isinstance(collected_at, datetime)
                    else None
                ),
                "matched_entities": matched_entities_raw,
            }
        )

        # Cycle 13: opt-in propagation of per-article event_model_payload
        # stashed on Article.ontology by upstream radars. When the payload is
        # absent (default for legacy callers whose Article.ontology is
        # empty) articles_json sees no change — zero regression.
        ontology = getattr(article, "ontology", None)
        if isinstance(ontology, Mapping):
            payload = ontology.get("event_model_payload")
            if payload:
                articles_json[-1]["event_model_payload"] = payload

        # Copy into JSON-native values before the template's ``|tojson`` and
        # the summary writer see the row. PaperRadar keeps ``published`` as a
        # datetime inside ``event_model_payload``; the caller's ontology stays
        # untouched.
        articles_json[-1] = _json_safe(articles_json[-1])

        if isinstance(matched_entities_raw, dict):
            for entity_name, keywords in matched_entities_raw.items():
                if not isinstance(entity_name, str) or not entity_name:
                    continue
                if isinstance(keywords, list):
                    entity_counts[entity_name] += len(keywords)
                else:
                    entity_counts[entity_name] += 1
        elif isinstance(matched_entities_raw, list):
            for entity_name in matched_entities_raw:
                if isinstance(entity_name, str) and entity_name:
                    entity_counts[entity_name] += 1

    now = datetime.now(timezone.utc)
    date_stamp = now.strftime("%Y%m%d")

    dated_name = f"{category.category_name}_{date_stamp}.html"
    dated_path = output_path.parent / dated_name

    pattern = re.compile(rf"^{re.escape(category.category_name)}_(\d{{8}})\.html$")
    dated_reports = sorted(
        (match.group(1), path.name)
        for path in output_path.parent.glob("*.html")
        if (match := pattern.match(path.name))
    )
    if all(report_name != dated_name for _, report_name in dated_reports):
        dated_reports.append((date_stamp, dated_name))
        dated_reports.sort()

    scanned_prev: str | None = None
    scanned_next: str | None = None
    for idx, (report_date, report_name) in enumerate(dated_reports):
        if report_date != date_stamp:
            continue
        if idx > 0:
            scanned_prev = dated_reports[idx - 1][1]
        if idx < len(dated_reports) - 1:
            scanned_next = dated_reports[idx + 1][1]
        break

    normalized_plugin_charts = _normalize_plugin_charts(plugin_charts)
    normalized_extra_sections = _normalize_extra_sections(extra_sections)

    templates_dir = Path(__file__).parent / "templates"
    env = _report_environment(templates_dir)
    template = env.get_template("report.html")

    rendered = template.render(
        category=category,
        articles=articles_list,
        articles_json=articles_json,
        generated_at=now,
        stats=stats,
        entity_counts=entity_counts,
        errors=errors or [],
        plugin_charts=normalized_plugin_charts,
        extra_sections=normalized_extra_sections,
        prev_report=prev_report if prev_report is not None else scanned_prev,
        next_report=next_report if next_report is not None else scanned_next,
    )
    rendered = "\n".join(line.rstrip() for line in rendered.splitlines()) + "\n"

    output_path.write_text(rendered, encoding="utf-8")
    dated_path.write_text(rendered, encoding="utf-8")

    generate_summary_json(
        category.category_name,
        articles_json,
        stats,
        output_path.parent,
        ontology_metadata=ontology_metadata,
        run_collection=run_collection,
    )
    return output_path


def _normalize_plugin_charts(plugin_charts: object) -> list[dict[str, Any]]:
    if not plugin_charts:
        return []
    if isinstance(plugin_charts, dict):
        return [
            value for value in plugin_charts.values() if isinstance(value, dict) and value
        ]
    if isinstance(plugin_charts, list):
        return [value for value in plugin_charts if isinstance(value, dict) and value]
    return []


def _normalize_extra_sections(extra_sections: object) -> list[dict[str, Any]]:
    if not isinstance(extra_sections, list):
        return []

    normalized: list[dict[str, Any]] = []
    for raw_section in extra_sections:
        if not isinstance(raw_section, Mapping):
            continue

        section_id = str(raw_section.get("id") or "").strip()
        title = str(raw_section.get("title") or "").strip()
        body_html = str(raw_section.get("body_html") or "").strip()
        if not section_id or not title or not body_html:
            continue

        badges_raw = raw_section.get("badges")
        badges: list[str] = []
        if isinstance(badges_raw, list):
            badges = [str(value).strip() for value in badges_raw if str(value).strip()]

        normalized.append(
            {
                "id": section_id,
                "title": title,
                "panel_title": str(raw_section.get("panel_title") or title).strip()
                or title,
                "subtitle": str(raw_section.get("subtitle") or "").strip(),
                "nav_label": str(raw_section.get("nav_label") or title).strip() or title,
                "aria_label": str(raw_section.get("aria_label") or title).strip()
                or title,
                "badges": badges,
                "body_html": body_html,
            }
        )
    return normalized


def generate_index_html(
    report_dir: Path,
    radar_name: str = "Radar",
) -> Path:
    """Generate an index.html listing all available report files.

    Scans the report directory for summary JSON and HTML files, then renders
    templates/index.html with report metadata and trend data.

    Args:
        report_dir: Directory containing report and summary files.
        radar_name: Display name for the index page header.

    Returns:
        Path to the generated index.html file.

    Raises:
        IOError: If the report directory cannot be created or index file cannot be written.
    """
    report_dir.mkdir(parents=True, exist_ok=True)

    standard_pattern = re.compile(r"^(.+)_(\d{8})\.html$")
    advanced_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2})$")

    summaries: list[dict[str, Any]] = []
    for summary_path in report_dir.glob("*_summary.json"):
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        summary = {
            "date": payload.get("date", ""),
            "category": payload.get("category", ""),
            "article_count": int(payload.get("article_count", 0) or 0),
            "source_count": int(payload.get("source_count", 0) or 0),
            "matched_count": int(payload.get("matched_count", 0) or 0),
            "top_entities": payload.get("top_entities", []),
        }
        summaries.append(summary)

    summaries.sort(key=lambda item: str(item.get("date") or ""), reverse=True)

    report_entries: list[tuple[str, str, dict[str, str]]] = []

    for html_path in report_dir.glob("*.html"):
        if html_path.name == "index.html":
            continue

        filename = html_path.name
        match = standard_pattern.match(filename)
        if match:
            category_name, raw_date = match.groups()
            date_label = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            report_entries.append(
                (
                    date_label,
                    filename,
                    {
                        "filename": filename,
                        "display_name": f"{category_name} - {date_label}",
                        "date": date_label,
                        "date_label": date_label,
                    },
                )
            )
            continue

        report_entries.append(
            (
                "",
                filename,
                {
                    "filename": filename,
                    "display_name": html_path.stem.replace("_", " ").title(),
                    "date": "",
                    "date_label": "",
                },
            )
        )

    for advanced_index in report_dir.glob("*/index.html"):
        relative_path = advanced_index.relative_to(report_dir)
        relative_filename = relative_path.as_posix()
        parent_name = advanced_index.parent.name
        if not advanced_pattern.match(parent_name):
            continue

        report_entries.append(
            (
                parent_name,
                relative_filename,
                {
                    "filename": relative_filename,
                    "display_name": parent_name,
                    "date": parent_name,
                    "date_label": parent_name,
                },
            )
        )

    report_entries.sort(key=lambda item: (item[0], item[1]), reverse=True)
    reports = [entry[2] for entry in report_entries]

    templates_dir = Path(__file__).parent / "templates"
    env = _report_environment(templates_dir)
    template = env.get_template("index.html")

    rendered = template.render(
        radar_name=radar_name,
        reports=reports,
        summaries_json=json_dumps(summaries),
        generated_at=datetime.now(timezone.utc),
    )
    rendered = "\n".join(line.rstrip() for line in rendered.splitlines()) + "\n"

    index_path = report_dir / "index.html"
    index_path.write_text(rendered, encoding="utf-8")
    return index_path


def generate_summary_json(
    category_name: str,
    articles: list[dict[str, Any]],
    stats: dict[str, int],
    output_dir: Path,
    ontology_metadata: Mapping[str, Any] | None = None,
    run_collection: Mapping[str, Any] | None = None,
) -> Path:
    """Generate a JSON summary of articles and statistics.

    Creates a structured JSON file containing article metadata, entity counts,
    and collection statistics for programmatic access and archival.

    ``articles`` is the rolling window the caller loaded, usually
    ``RadarStorage.recent_articles`` (default 7 days). ``sources``,
    ``article_count``, ``source_count``, and ``matched_count`` are that
    cumulative window. They are not the number of articles fetched in the
    current execution. A source that failed today can still show a positive
    ``sources`` count when older rows remain inside the window.

    Pass ``run_collection`` to record this execution under the ``run`` key:
    per-source fetch counts (including zeros), failed sources with error text
    capped at 240 characters and stripped of secrets and header values, and
    the last successful store time per source. The same call also adds
    ``sources_window``, which names ``sources`` as the rolling-window total.
    Omit ``run_collection`` (or pass ``None``) and the JSON keys and values
    stay exactly as they were before this argument existed.

    ``stats`` remains ``dict[str, int]`` and cannot carry nested run data.
    ``stats["window_days"]`` only affects the ``sources_window`` label.

    Args:
        category_name: Category name to include in the summary and output filename.
        articles: Rolling-window article dictionaries. Radar runners pass the
            recent 7-day rows used for the report, not this run's raw fetch.
        stats: Integer statistics. ``window_days`` labels the rolling window
            when ``run_collection`` is set. It is not written on its own.
        output_dir: Directory where the summary JSON file will be written.
        ontology_metadata: Optional ontology block. Omitted when empty.
        run_collection: Optional this-run record. Recognized keys are
            ``collected_counts``, ``collected_total``, ``failed_sources`` or
            ``errors``, and ``last_success_at``. Unknown keys are ignored.

    Returns:
        Path to the generated summary JSON file.

    Raises:
        IOError: If the output directory cannot be created or file cannot be written.
        TypeError: If ``run_collection`` is not a mapping.
    """
    if run_collection is not None and not isinstance(run_collection, Mapping):
        raise TypeError("run_collection must be a mapping or None")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Counts the articles argument (the rolling window), not this execution.
    rolling_window_source_counts: Counter[str] = Counter()
    entity_counts: Counter[str] = Counter()
    matched_count = 0

    for article in articles:
        source = article.get("source")
        if isinstance(source, str) and source:
            rolling_window_source_counts[source] += 1

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
        "source_count": int(
            stats.get("source_count", len(rolling_window_source_counts))
        ),
        "matched_count": int(stats.get("matched_count", matched_count)),
        "top_entities": [
            {"name": name, "count": count}
            for name, count in entity_counts.most_common(20)
        ],
        "sources": dict(rolling_window_source_counts),
        "generated_at": now.isoformat(),
    }
    # Opt-in preservation of per-article `event_model_payload` for ontology-aware
    # downstream consumers (e.g. dashboard event-model panel). Callers that do not
    # attach this key see no change in output schema (zero regression).
    preserved_articles = [
        {"event_model_payload": article["event_model_payload"]}
        for article in articles
        if isinstance(article, Mapping) and article.get("event_model_payload")
    ]
    if preserved_articles:
        summary["articles"] = preserved_articles
    normalized_ontology = _normalize_summary_metadata(ontology_metadata)
    if normalized_ontology:
        summary["ontology"] = normalized_ontology
    if run_collection is not None:
        # Names the existing ``sources`` map as the rolling window without
        # changing those counts. Absent unless the caller opted in.
        summary["sources_window"] = _sources_window_metadata(stats)
        summary["run"] = _normalize_run_collection(run_collection)

    output_path = output_dir / f"{category_name}_{date_stamp}_summary.json"
    output_path.write_text(
        json_dumps(summary, indent=2),
        encoding="utf-8",
    )
    return output_path


def _report_environment(templates_dir: Path) -> Environment:
    """Jinja environment whose ``|tojson`` filter uses :func:`json_default`.

    ``Environment.policies`` shallow-copies Jinja's defaults, and the shared
    ``json.dumps_kwargs`` dict must be replaced rather than mutated.
    """
    env = Environment(loader=FileSystemLoader(str(templates_dir)), autoescape=False)
    dumps_kwargs = dict(env.policies.get("json.dumps_kwargs") or {})
    dumps_kwargs["default"] = json_default
    env.policies["json.dumps_kwargs"] = dumps_kwargs
    return env


def _json_safe(value: Any) -> Any:
    """Deep-copy ``value`` into types ``json.dumps`` accepts.

    ``datetime`` and ``date`` become ISO 8601 strings. ``Decimal`` becomes a
    string. ``set`` and ``frozenset`` become lists. Nested containers are
    copied so the input object is left unchanged.
    """
    if isinstance(value, Mapping):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return _json_safe(json_default(value))


def _normalize_summary_metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(metadata, Mapping):
        return {}

    normalized: dict[str, Any] = {}
    for key, value in metadata.items():
        normalized_key = str(key).strip()
        if not normalized_key:
            continue
        normalized[normalized_key] = _normalize_summary_metadata_value(value)
    return normalized


def _sources_window_metadata(stats: Mapping[str, Any]) -> dict[str, Any]:
    """Describe ``sources`` as the rolling window, defaulting to 7 days."""
    raw_days = stats.get("window_days", _DEFAULT_SUMMARY_WINDOW_DAYS)
    if isinstance(raw_days, bool) or not isinstance(raw_days, int) or raw_days < 1:
        days = _DEFAULT_SUMMARY_WINDOW_DAYS
    else:
        days = raw_days
    return {
        "days": days,
        "scope": "rolling_window_cumulative",
        "counts_field": "sources",
        "description": (
            "sources, article_count, source_count, matched_count는 "
            f"최근 {days}일 누적 기사 수입니다. "
            "이번 실행의 소스별 수집 건수는 run.collected_counts입니다."
        ),
    }


def _normalize_run_collection(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Copy this-run counts into the summary ``run`` object.

    Error strings are capped and stripped here so a caller can pass the raw
    ``collect_sources`` messages without writing secrets into the summary.
    Unknown keys on ``raw`` are dropped.
    """
    counts = _normalize_collected_counts(raw.get("collected_counts"))
    known_sources = set(counts)
    last_success = _normalize_last_success(raw.get("last_success_at"))
    known_sources.update(last_success)
    failed = _normalize_failed_sources(raw, known_sources)
    for item in failed:
        name = item["source"]
        if isinstance(name, str) and name and name not in counts:
            counts[name] = 0
    total = _non_negative_int(raw.get("collected_total"))
    if total is None:
        total = sum(counts.values())
    return {
        "collected_total": total,
        "collected_counts": counts,
        "failed_sources": failed,
        "last_success_at": last_success,
    }


def _normalize_collected_counts(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    counts: dict[str, int] = {}
    for key, raw_count in value.items():
        name = str(key).strip()
        parsed = _non_negative_int(raw_count)
        if not name or parsed is None:
            continue
        counts[name] = parsed
    return counts


def _normalize_failed_sources(
    raw: Mapping[str, Any],
    known_sources: set[str],
) -> list[dict[str, Any]]:
    payload = raw.get("failed_sources", raw.get("errors"))
    grouped: dict[str, list[str]] = {}
    order: list[str] = []

    def add(source: str, messages: list[object]) -> None:
        name = source.strip()
        if name not in grouped:
            grouped[name] = []
            order.append(name)
        for message in messages:
            detail = _sanitize_error_detail(message)
            if detail:
                grouped[name].append(detail)

    if isinstance(payload, Mapping):
        for key, value in payload.items():
            add(str(key), _coerce_messages(value))
    elif isinstance(payload, (list, tuple)):
        for item in payload:
            if isinstance(item, str):
                source, message = _split_source_error(item, known_sources)
                add(source, [message if source else item])
            elif isinstance(item, Mapping):
                source = str(item.get("source") or item.get("name") or "")
                messages = _error_messages(item)
                if source.strip():
                    add(source, messages)
                else:
                    for raw_message in messages:
                        text = (
                            raw_message
                            if isinstance(raw_message, str)
                            else str(raw_message)
                        )
                        split_source, split_message = _split_source_error(
                            text, known_sources
                        )
                        add(split_source, [split_message if split_source else text])
            elif item is not None:
                add("", [item])
    return [
        {"source": name, "errors": grouped[name]}
        for name in order
        if name or grouped[name]
    ]


def _error_messages(item: Mapping[str, Any]) -> list[object]:
    if "errors" in item:
        return _coerce_messages(item.get("errors"))
    if "error" in item:
        return _coerce_messages(item.get("error"))
    if "message" in item:
        return _coerce_messages(item.get("message"))
    return []


def _coerce_messages(value: object) -> list[object]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [item for item in value if item is not None]
    if value is None:
        return []
    return [value]


def _split_source_error(text: str, known_sources: set[str]) -> tuple[str, str]:
    matches = [name for name in known_sources if text.startswith(f"{name}:")]
    if matches:
        name = max(matches, key=len)
        return name, text[len(name) + 1 :].lstrip()
    if ": " in text:
        name, _, message = text.partition(": ")
        name = name.strip()
        if name:
            return name, message.strip()
    return "", text.strip()


def _normalize_last_success(value: object) -> dict[str, str | None]:
    if not isinstance(value, Mapping):
        return {}
    normalized: dict[str, str | None] = {}
    for key, raw_value in value.items():
        name = str(key).strip()
        if not name:
            continue
        normalized[name] = _normalize_timestamp(raw_value)
    return normalized


def _normalize_timestamp(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        stamped = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return stamped.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        candidate = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    return None


def _non_negative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value >= 0 else 0


def _sanitize_error_detail(value: object, *, limit: int = _RUN_ERROR_LIMIT) -> str:
    """Drop header values and obvious secrets, then cap the stored error text."""
    text = value if isinstance(value, str) else str(value)
    text = _SENSITIVE_HEADER.sub(lambda match: f"{match.group(1)}=[redacted]", text)
    text = _BEARER.sub("Bearer [redacted]", text)
    text = _QUERY_SECRET.sub(r"\1[redacted]", text)
    text = _URL_USERINFO.sub(r"\1[redacted]@", text)
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    if limit < 4:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def _normalize_summary_metadata_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key).strip(): _normalize_summary_metadata_value(item)
            for key, item in value.items()
            if str(key).strip()
        }
    if isinstance(value, (list, tuple, set)):
        return [_normalize_summary_metadata_value(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
