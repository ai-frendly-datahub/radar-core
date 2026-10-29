"""Fail collection runs that finish green while storing nothing.

``run_collection`` on the summary JSON is this execution's fetch record.
The quality report is the JSON each Radar already writes (for example
``reports/<category>_quality.json``). This module adds a ``run_health``
object to that report and a checker that reads it.

The totals are counts the caller measured. Fetched items are not treated as
stored items, and error text is not copied into the report.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

RUN_HEALTH_KEYS = (
    "sources_attempted",
    "sources_with_fetch_errors",
    "raw_items_fetched",
    "items_stored_new",
    "items_stored_upserted",
    "items_stored",
)


@dataclass(frozen=True)
class RunHealthVerdict:
    """Result of comparing a ``run_health`` object with the stored-row gate."""

    ok: bool
    level: str
    reason: str

    def annotation_line(self) -> str | None:
        """GitHub Actions workflow command, or ``None`` when the run passes."""
        if self.ok:
            return None
        return f"::{self.level}::{self.reason}"


def count_stored_items(
    *,
    written_links: Iterable[str],
    existing_links: Iterable[str],
) -> tuple[int, int]:
    """Return ``(new, upserted)`` for links written in this run.

    A written link that is already in ``existing_links`` is an upsert. Any
    other written link is new. Duplicate written links count once, matching
    one stored row per link. Call this with the links that existed before
    upsert and the links actually passed to storage.
    """
    existing = {_require_link(link, "existing_links") for link in existing_links}
    seen: set[str] = set()
    new = 0
    upserted = 0
    for link in written_links:
        key = _require_link(link, "written_links")
        if key in seen:
            continue
        seen.add(key)
        if key in existing:
            upserted += 1
        else:
            new += 1
    return new, upserted


def build_run_health(
    run_collection: Mapping[str, Any] | None = None,
    *,
    sources_attempted: int | None = None,
    sources_with_fetch_errors: int | None = None,
    raw_items_fetched: int | None = None,
    items_stored_new: int,
    items_stored_upserted: int,
) -> dict[str, int]:
    """Build the ``run_health`` object for one collection run.

    Source and fetch totals come from ``run_collection`` when the caller does
    not pass them. Recognized keys match the summary ``run`` record:
    ``collected_counts``, ``collected_total``, and ``failed_sources`` or
    ``errors``. Storage totals are only the ``items_stored_new`` and
    ``items_stored_upserted`` arguments. ``items_stored`` is their sum.

    Missing fetch totals stay missing. They are not filled with zero, and
    ``collected_total`` is never copied into ``items_stored``.
    """
    if run_collection is not None and not isinstance(run_collection, Mapping):
        raise TypeError("run_collection must be a mapping or None")

    stored_new = _require_count(items_stored_new, "items_stored_new")
    stored_upserted = _require_count(items_stored_upserted, "items_stored_upserted")

    derived_attempted: int | None = None
    derived_errors: int | None = None
    derived_raw: int | None = None
    if run_collection is not None:
        counts, attempted_names = _collected_counts(run_collection)
        failed_names = _failed_source_names(run_collection, attempted_names)
        # collected_counts is the attempt list. Failed names missing from it
        # were still attempted. Failures alone are not an attempt list: that
        # would mark every source errored when successful sources were omitted.
        if "collected_counts" in run_collection:
            derived_attempted = len(attempted_names | failed_names)
        if _has_failure_list(run_collection):
            derived_errors = len(failed_names)
        if "collected_total" in run_collection:
            derived_raw = _require_count(
                run_collection.get("collected_total"),
                "collected_total",
            )
        elif "collected_counts" in run_collection:
            derived_raw = sum(counts.values())

    attempted = (
        sources_attempted if sources_attempted is not None else derived_attempted
    )
    errored = (
        sources_with_fetch_errors
        if sources_with_fetch_errors is not None
        else derived_errors
    )
    raw = raw_items_fetched if raw_items_fetched is not None else derived_raw
    if attempted is None:
        raise ValueError(
            "sources_attempted is required when run_collection does not name sources"
        )
    if errored is None:
        raise ValueError(
            "sources_with_fetch_errors is required when run_collection "
            "has no failed_sources or errors"
        )
    if raw is None:
        raise ValueError(
            "raw_items_fetched is required when run_collection has no "
            "collected_total or collected_counts"
        )

    attempted_count = _require_count(attempted, "sources_attempted")
    error_count = _require_count(errored, "sources_with_fetch_errors")
    raw_count = _require_count(raw, "raw_items_fetched")
    if error_count > attempted_count:
        raise ValueError(
            f"sources_with_fetch_errors {error_count} exceeds "
            f"sources_attempted {attempted_count}"
        )
    return {
        "sources_attempted": attempted_count,
        "sources_with_fetch_errors": error_count,
        "raw_items_fetched": raw_count,
        "items_stored_new": stored_new,
        "items_stored_upserted": stored_upserted,
        "items_stored": stored_new + stored_upserted,
    }


def merge_run_health(
    report: Mapping[str, Any],
    run_health: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a copy of a quality report with ``run_health`` set.

    Existing keys and their values are kept. The input report is not mutated.
    ``run_health`` is placed immediately after ``summary`` when that key
    exists. Unknown keys inside ``run_health`` are dropped.
    """
    if not isinstance(report, Mapping):
        raise TypeError("quality report must be a mapping")
    normalized = normalize_run_health(run_health)
    copied = dict(report)
    copied.pop("run_health", None)
    items = list(copied.items())
    keys = [key for key, _value in items]
    if "summary" in keys:
        index = keys.index("summary") + 1
    elif "errors" in keys:
        index = keys.index("errors")
    else:
        index = len(items)
    items.insert(index, ("run_health", normalized))
    return dict(items)


def write_run_health(report_path: Path, run_health: Mapping[str, Any]) -> Path:
    """Insert ``run_health`` into an existing quality report JSON file."""
    path = Path(report_path)
    report = _read_report(path)
    merged = merge_run_health(report, run_health)
    path.write_text(
        json.dumps(merged, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def normalize_run_health(run_health: Mapping[str, Any]) -> dict[str, int]:
    """Copy the six ``run_health`` counts and check that stored rows add up."""
    if not isinstance(run_health, Mapping):
        raise TypeError("run_health must be a mapping")
    missing = [key for key in RUN_HEALTH_KEYS if key not in run_health]
    if missing:
        raise ValueError("run_health is missing " + ", ".join(missing))
    values = {key: _require_count(run_health[key], key) for key in RUN_HEALTH_KEYS}
    stored = values["items_stored"]
    parts = values["items_stored_new"] + values["items_stored_upserted"]
    if stored != parts:
        raise ValueError(
            f"items_stored {stored} does not equal "
            f"items_stored_new {values['items_stored_new']} + "
            f"items_stored_upserted {values['items_stored_upserted']}"
        )
    if values["sources_with_fetch_errors"] > values["sources_attempted"]:
        raise ValueError(
            "sources_with_fetch_errors "
            f"{values['sources_with_fetch_errors']} exceeds "
            f"sources_attempted {values['sources_attempted']}"
        )
    return values


def evaluate_run_health(
    run_health: Mapping[str, Any],
    *,
    min_stored: int = 1,
) -> RunHealthVerdict:
    """Decide whether this run stored enough rows and any source responded.

    ``min_stored`` defaults to 1, so zero stored rows is an error. A run where
    every attempted source has a fetch error still fails when the stored count
    meets the threshold. That case is a warning annotation with a non-zero
    exit. When both are true, the annotation is an error and the reason
    states both.
    """
    minimum = _require_count(min_stored, "min_stored")
    try:
        totals = normalize_run_health(run_health)
    except (TypeError, ValueError) as exc:
        return RunHealthVerdict(ok=False, level="error", reason=_one_line(str(exc)))

    stored = totals["items_stored"]
    attempted = totals["sources_attempted"]
    errored = totals["sources_with_fetch_errors"]
    below_minimum = stored < minimum
    every_source_errored = attempted > 0 and errored == attempted
    stored_clause = f"stored {stored} items (minimum {minimum})"
    errored_clause = f"every source errored ({errored}/{attempted})"

    if below_minimum and every_source_errored:
        reason = f"{stored_clause}; {errored_clause}"
        return RunHealthVerdict(ok=False, level="error", reason=reason)
    if below_minimum:
        return RunHealthVerdict(ok=False, level="error", reason=stored_clause)
    if every_source_errored:
        return RunHealthVerdict(ok=False, level="warning", reason=errored_clause)
    return RunHealthVerdict(ok=True, level="ok", reason=stored_clause)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry for ``python -m radar_core.health``."""
    parser = argparse.ArgumentParser(
        prog="python -m radar_core.health",
        description=(
            "Exit non-zero when a quality report's run_health stored too few "
            "rows or every attempted source had a fetch error."
        ),
    )
    parser.add_argument(
        "--report",
        required=True,
        type=Path,
        help="Path to the quality report JSON that contains run_health.",
    )
    parser.add_argument(
        "--min-stored",
        type=_non_negative_arg,
        default=1,
        help="Fail when items_stored is below this value (default: 1).",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        report = _read_report(args.report)
    except (OSError, ValueError, TypeError) as exc:
        verdict = RunHealthVerdict(ok=False, level="error", reason=_one_line(str(exc)))
        _emit(verdict)
        return 1

    payload = report.get("run_health")
    if not isinstance(payload, Mapping):
        verdict = RunHealthVerdict(
            ok=False,
            level="error",
            reason=f"quality report has no run_health object: {args.report}",
        )
        _emit(verdict)
        return 1

    verdict = evaluate_run_health(payload, min_stored=args.min_stored)
    _emit(verdict)
    return 0 if verdict.ok else 1


def _emit(verdict: RunHealthVerdict) -> None:
    print(verdict.reason)
    annotation = verdict.annotation_line()
    if annotation is not None:
        print(annotation)


def _read_report(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ValueError(f"quality report not found: {path}") from exc
    except OSError as exc:
        raise ValueError(f"could not read quality report: {path}") from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"quality report is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise TypeError(f"quality report must be a JSON object: {path}")
    return payload


def _collected_counts(
    run_collection: Mapping[str, Any],
) -> tuple[dict[str, int], set[str]]:
    if "collected_counts" not in run_collection:
        return {}, set()
    raw_counts = run_collection.get("collected_counts")
    if not isinstance(raw_counts, Mapping):
        raise TypeError("collected_counts must be a mapping")
    counts: dict[str, int] = {}
    for key, raw_count in raw_counts.items():
        name = str(key).strip()
        if not name:
            raise ValueError("collected_counts contains an empty source name")
        counts[name] = _require_count(raw_count, f"collected_counts[{name}]")
    return counts, set(counts)


def _failed_source_names(
    run_collection: Mapping[str, Any],
    known_sources: set[str],
) -> set[str]:
    if "failed_sources" in run_collection:
        payload = run_collection.get("failed_sources")
        label = "failed_sources"
    elif "errors" in run_collection:
        payload = run_collection.get("errors")
        label = "errors"
    else:
        return set()
    if payload is None:
        raise TypeError(f"{label} must be a list or mapping")

    names: set[str] = set()
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            name = str(key).strip()
            if name and _message_texts(value):
                names.add(name)
        return names
    if not isinstance(payload, (list, tuple)):
        raise TypeError(f"{label} must be a list or mapping")

    for item in payload:
        if isinstance(item, str):
            name = _source_name(item, known_sources)
            if name and _message_after_source(item, name):
                names.add(name)
            continue
        if isinstance(item, Mapping):
            name = str(item.get("source") or item.get("name") or "").strip()
            messages = _error_messages(item)
            if name and messages:
                names.add(name)
            continue
        if item is not None:
            raise TypeError(f"{label} entries must be strings or objects")
    return names


def _error_messages(item: Mapping[str, Any]) -> list[str]:
    if "errors" in item:
        return _message_texts(item.get("errors"))
    if "error" in item:
        return _message_texts(item.get("error"))
    if "message" in item:
        return _message_texts(item.get("message"))
    return []


def _message_texts(value: object) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple)):
        texts: list[str] = []
        for item in value:
            texts.extend(_message_texts(item))
        return texts
    return []


def _source_name(text: str, known_sources: set[str]) -> str:
    matches = [name for name in known_sources if text.startswith(f"{name}:")]
    if matches:
        return max(matches, key=len)
    if ": " in text:
        name, _, _message = text.partition(": ")
        return name.strip()
    return ""


def _message_after_source(text: str, name: str) -> str:
    prefix = f"{name}:"
    if text.startswith(prefix):
        return text[len(prefix) :].strip()
    return text.strip()


def _has_failure_list(run_collection: Mapping[str, Any]) -> bool:
    return "failed_sources" in run_collection or "errors" in run_collection


def _require_link(link: object, label: str) -> str:
    if not isinstance(link, str):
        raise TypeError(f"{label} entries must be strings")
    return link


def _require_count(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _non_negative_arg(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def _one_line(text: str) -> str:
    return " ".join(text.split())


if __name__ == "__main__":
    raise SystemExit(main())
