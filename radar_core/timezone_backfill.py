"""Detect and correct published instants that mix KST and UTC.

The historical collector bug stored feedparser's UTC ``*_parsed`` struct
through ``time.mktime``. On ``TZ=Asia/Seoul`` that instant is nine hours
early. Zone-less Korean strings were also stored as UTC wall time.

This module only rewrites a row when ``published_raw`` parses and the
stored UTC instant is empty or exactly nine hours away. It does not shift
rows that have no original string. Callers must pass ``apply=True``;
the default reports counts and leaves files untouched.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import duckdb
import yaml

from .dates import (
    has_explicit_offset,
    parse_published,
    resolve_default_timezone,
    to_utc_iso,
)

NINE_HOURS = timedelta(hours=9)
ALREADY_CORRECT = "already_correct"
CORRECTABLE = "correctable"
AMBIGUOUS = "ambiguous"
UNEXPECTED = "unexpected"


@dataclass(frozen=True)
class PublishedAssessment:
    status: str
    reason: str
    corrected_utc: datetime | None = None


@dataclass
class SourceCount:
    repo: str
    source: str
    store: str = ""
    total: int = 0
    already_correct: int = 0
    correctable: int = 0
    ambiguous: int = 0
    unexpected: int = 0

    def add(self, status: str) -> None:
        self.total += 1
        if status == ALREADY_CORRECT:
            self.already_correct += 1
        elif status == CORRECTABLE:
            self.correctable += 1
        elif status == AMBIGUOUS:
            self.ambiguous += 1
        else:
            self.unexpected += 1


@dataclass
class ScanResult:
    counts: list[SourceCount] = field(default_factory=list)
    applied: int = 0
    errors: list[str] = field(default_factory=list)


def assess_published(
    *,
    stored: object,
    published_raw: object,
    default_timezone: str,
) -> PublishedAssessment:
    """Classify one stored published value against its original raw string.

    Apply conditions (all required for :attr:`CORRECTABLE`):

    * ``published_raw`` is a non-empty string that parses.
    * The stored instant is missing, or it differs by exactly nine hours.
    * A zone-less raw string is corrected only when ``default_timezone`` is
      ``Asia/Seoul``. An explicit offset is corrected regardless of source.
    """
    raw = published_raw.strip() if isinstance(published_raw, str) else ""
    if not raw:
        if default_timezone == "Asia/Seoul":
            reason = "no published_raw; Korean source is not rewritten without the original string"
        else:
            reason = "no published_raw"
        return PublishedAssessment(AMBIGUOUS, reason)

    try:
        corrected = parse_published(raw, default_timezone=default_timezone)
    except ValueError as exc:
        return PublishedAssessment(UNEXPECTED, f"invalid timezone: {exc}")
    if corrected is None:
        return PublishedAssessment(UNEXPECTED, "published_raw did not parse")

    stored_utc = _stored_instant(stored)
    if stored_utc is None:
        return PublishedAssessment(
            CORRECTABLE,
            "stored published is empty",
            corrected.astimezone(UTC),
        )

    delta = _whole_seconds(corrected) - _whole_seconds(stored_utc)
    if delta == timedelta(0):
        return PublishedAssessment(ALREADY_CORRECT, "instant matches published_raw")
    if delta not in {NINE_HOURS, -NINE_HOURS}:
        return PublishedAssessment(
            UNEXPECTED,
            f"delta is {delta}, not exactly ±9 hours",
        )
    if not has_explicit_offset(raw) and default_timezone != "Asia/Seoul":
        return PublishedAssessment(
            UNEXPECTED,
            "zone-less raw is only rewritten for Asia/Seoul sources",
        )
    direction = "ahead" if delta > timedelta(0) else "behind"
    return PublishedAssessment(
        CORRECTABLE,
        f"stored instant is 9 hours {direction} of published_raw",
        corrected.astimezone(UTC),
    )


def scan_repositories(
    repos: Iterable[Path],
    *,
    apply: bool = False,
) -> ScanResult:
    """Scan each repo's DuckDB and jsonl. Write only when ``apply`` is true."""
    result = ScanResult()
    for repo in repos:
        path = repo.expanduser().resolve()
        if not path.is_dir():
            result.errors.append(f"{path} is not a directory")
            continue
        timezones = load_source_timezones(path)
        repo_name = path.name
        for db_path in _duckdb_files(path):
            try:
                applied = _scan_duckdb(
                    db_path,
                    repo=repo_name,
                    timezones=timezones,
                    apply=apply,
                    counts=result.counts,
                )
            except (OSError, duckdb.Error, yaml.YAMLError) as exc:
                result.errors.append(f"{db_path}: {exc}")
                continue
            result.applied += applied
        for jsonl_path in _jsonl_files(path):
            try:
                applied = _scan_jsonl(
                    jsonl_path,
                    repo=repo_name,
                    timezones=timezones,
                    apply=apply,
                    counts=result.counts,
                )
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                result.errors.append(f"{jsonl_path}: {exc}")
                continue
            result.applied += applied
    result.counts.sort(key=lambda item: (item.repo, item.store, item.source))
    return result


def load_source_timezones(repo: Path) -> dict[str, str]:
    """Map source display names to the zone used for zone-less dates."""
    mapping: dict[str, str] = {}
    config_dir = repo / "config"
    if not config_dir.is_dir():
        return mapping
    for path in sorted(config_dir.rglob("*.yaml")) + sorted(config_dir.rglob("*.yml")):
        try:
            loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            continue
        if not isinstance(loaded, dict):
            continue
        sources = loaded.get("sources")
        if not isinstance(sources, list):
            continue
        for entry in sources:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if not isinstance(name, str) or not name.strip():
                continue
            url = entry.get("url") if isinstance(entry.get("url"), str) else ""
            explicit = entry.get("default_timezone")
            explicit_text = explicit.strip() if isinstance(explicit, str) else ""
            config = (
                entry.get("config") if isinstance(entry.get("config"), dict) else {}
            )
            try:
                mapping[name.strip()] = resolve_default_timezone(
                    source_url=url,
                    default_timezone=explicit_text,
                    config=cast(dict[str, Any], config),
                )
            except ValueError:
                mapping[name.strip()] = "UTC"
    return mapping


def format_report(result: ScanResult, *, apply: bool) -> str:
    """Korean operator summary. Counts are per repository and source."""
    mode = "apply" if apply else "dry-run"
    lines = [
        f"모드: {mode}",
        "dry-run 은 파일을 수정하지 않습니다. 수정은 --apply 일 때만 수행합니다.",
        "",
        "repo\tsource\tstore\ttotal\talready_correct\tcorrectable\tambiguous\tunexpected",
    ]
    totals = SourceCount(repo="TOTAL", source="", store="")
    for row in result.counts:
        lines.append(
            "\t".join(
                [
                    row.repo,
                    row.source,
                    row.store,
                    str(row.total),
                    str(row.already_correct),
                    str(row.correctable),
                    str(row.ambiguous),
                    str(row.unexpected),
                ]
            )
        )
        totals.total += row.total
        totals.already_correct += row.already_correct
        totals.correctable += row.correctable
        totals.ambiguous += row.ambiguous
        totals.unexpected += row.unexpected
    lines.append(
        "\t".join(
            [
                totals.repo,
                totals.source,
                totals.store,
                str(totals.total),
                str(totals.already_correct),
                str(totals.correctable),
                str(totals.ambiguous),
                str(totals.unexpected),
            ]
        )
    )
    lines.append("")
    if apply:
        lines.append(f"적용한 행: {result.applied}")
    else:
        lines.append("적용한 행: 0 (--apply 없음)")
    if result.errors:
        lines.append("오류:")
        lines.extend(f"- {error}" for error in result.errors)
    lines.append("")
    return "\n".join(lines)


def _stored_instant(stored: object) -> datetime | None:
    if stored is None:
        return None
    if isinstance(stored, datetime):
        if stored.tzinfo is None or stored.tzinfo.utcoffset(stored) is None:
            return stored.replace(tzinfo=UTC)
        return stored.astimezone(UTC)
    if isinstance(stored, str):
        text = stored.strip()
        if not text or text.lower() == "null":
            return None
        parsed = parse_published(text, default_timezone="UTC")
        if parsed is None:
            return None
        return parsed.astimezone(UTC)
    return None


def _whole_seconds(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(microsecond=0)


def _bucket(
    counts: list[SourceCount], repo: str, source: str, store: str
) -> SourceCount:
    for row in counts:
        if row.repo == repo and row.source == source and row.store == store:
            return row
    row = SourceCount(repo=repo, source=source, store=store)
    counts.append(row)
    return row


def _timezone_for(timezones: dict[str, str], source: str) -> str:
    return timezones.get(source, "UTC")


def _duckdb_files(repo: Path) -> list[Path]:
    data = repo / "data"
    if not data.is_dir():
        return []
    found: list[Path] = []
    for path in sorted(data.rglob("*.duckdb")):
        if "snapshots" in path.parts or "daily" in path.parts:
            continue
        found.append(path)
    return found


def _jsonl_files(repo: Path) -> list[Path]:
    data = repo / "data"
    if not data.is_dir():
        return []
    return sorted(data.rglob("*.jsonl"))


def _scan_duckdb(
    path: Path,
    *,
    repo: str,
    timezones: dict[str, str],
    apply: bool,
    counts: list[SourceCount],
) -> int:
    conn = duckdb.connect(str(path), read_only=not apply)
    applied = 0
    try:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        if "articles" not in tables:
            return 0
        columns = {
            str(row[0])
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'main' AND table_name = 'articles'"
            ).fetchall()
        }
        if (
            "published" not in columns
            or "source" not in columns
            or "link" not in columns
        ):
            return 0
        select_cols = ["link", "source", "published"]
        if "published_raw" in columns:
            select_cols.append("published_raw")
        rows = conn.execute(f"SELECT {', '.join(select_cols)} FROM articles").fetchall()
        updates: list[tuple[datetime, str]] = []
        for row in rows:
            link = str(row[0] or "")
            source = str(row[1] or "")
            stored = row[2]
            raw = row[3] if len(row) > 3 else None
            zone = _timezone_for(timezones, source)
            assessment = assess_published(
                stored=stored,
                published_raw=raw,
                default_timezone=zone,
            )
            _bucket(counts, repo, source or "(unknown)", "duckdb").add(
                assessment.status
            )
            if (
                apply
                and assessment.status == CORRECTABLE
                and assessment.corrected_utc is not None
                and link
            ):
                updates.append(
                    (
                        assessment.corrected_utc.astimezone(UTC).replace(tzinfo=None),
                        link,
                    )
                )
        if apply and updates:
            conn.executemany(
                "UPDATE articles SET published = ? WHERE link = ?",
                updates,
            )
            applied = len(updates)
    finally:
        conn.close()
    return applied


def _scan_jsonl(
    path: Path,
    *,
    repo: str,
    timezones: dict[str, str],
    apply: bool,
    counts: list[SourceCount],
) -> int:
    original = path.read_text(encoding="utf-8")
    rewritten: list[str] = []
    applied = 0
    changed = False
    for line in original.splitlines():
        if not line.strip():
            rewritten.append(line)
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            rewritten.append(line)
            _bucket(counts, repo, "(unparsed-jsonl)", "jsonl").add(UNEXPECTED)
            continue
        if not isinstance(record, dict):
            rewritten.append(line)
            continue
        source = record.get("source")
        source_name = source.strip() if isinstance(source, str) else ""
        zone = _timezone_for(timezones, source_name)
        assessment = assess_published(
            stored=record.get("published"),
            published_raw=record.get("published_raw"),
            default_timezone=zone,
        )
        _bucket(counts, repo, source_name or "(unknown)", "jsonl").add(
            assessment.status
        )
        if (
            apply
            and assessment.status == CORRECTABLE
            and assessment.corrected_utc is not None
            and record.get("link")
        ):
            record["published"] = to_utc_iso(assessment.corrected_utc)
            changed = True
            applied += 1
            rewritten.append(json.dumps(record, ensure_ascii=False))
        else:
            rewritten.append(line)
    if apply and changed:
        text = "\n".join(rewritten)
        if original.endswith("\n") or text:
            text += "\n"
        path.write_text(text, encoding="utf-8")
    return applied
