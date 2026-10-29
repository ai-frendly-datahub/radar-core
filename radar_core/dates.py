"""Timezone-aware published dates and the KST daily partition key.

feedparser's ``published_parsed`` / ``updated_parsed`` values are UTC
``time.struct_time`` values. Interpreting them with ``time.mktime`` treats
that UTC clock as local time. Crawlers set ``TZ=Asia/Seoul``, so the stored
instant is exactly nine hours early and then labeled UTC.

Daily partition key
-------------------
``data/raw/<YYYY-MM-DD>/``, ``data/snapshots/<YYYY-MM-DD>/``,
``data/daily/``, and dated report filenames (``<category>_YYYYMMDD``) use
the calendar date in Asia/Seoul (KST, UTC+09:00, no daylight-saving time).

The key is the KST date of the *collection* instant, when the file is
written. It is not the UTC date and not the article's published date.
Between 00:00 and 08:59 KST the UTC date is still the previous day; those
hours still use the KST date. ``RadarSettings.timezone`` remains the
schedule zone and does not change this key.
"""

from __future__ import annotations

import calendar
import re
import time
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, NamedTuple
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

KST = ZoneInfo("Asia/Seoul")
DAILY_PARTITION_TIMEZONE = "Asia/Seoul"
KOREAN_DEFAULT_TIMEZONE = "Asia/Seoul"

_RAW_STRING_KEYS: tuple[str, ...] = (
    "published",
    "updated",
    "pubDate",
    "date",
    "published_at",
    "publishedAt",
    "created_at",
    "created",
)
_STRUCT_KEYS: tuple[str, ...] = (
    "published_parsed",
    "updated_parsed",
    "created_parsed",
)
_UNIX_KEYS: tuple[str, ...] = ("created_utc", "timestamp")

# RFC 2822 abbreviations. CST follows the RFC (-0600), not China Standard Time.
_ABBREV_HOURS: dict[str, int] = {
    "KST": 9,
    "JST": 9,
    "UTC": 0,
    "GMT": 0,
    "UT": 0,
    "EST": -5,
    "EDT": -4,
    "CST": -6,
    "CDT": -5,
    "MST": -7,
    "MDT": -6,
    "PST": -8,
    "PDT": -7,
}
_TRAILING_ABBREV = re.compile(
    r"^(?P<body>.*\d)(?:\s+)(?P<abbr>KST|JST|UTC|GMT|UT|EST|EDT|CST|CDT|MST|MDT|PST|PDT)\s*$",
    re.IGNORECASE,
)
_EXPLICIT_OFFSET = re.compile(r"(?:Z|[+-]\d{2}:?\d{2})\s*$", re.IGNORECASE)
_LABEL_PREFIX = re.compile(
    r"^(?:게시일|발행일|작성일|등록일|published|date)\s*[:：]?\s*",
    re.IGNORECASE,
)
_KOREAN_DATE = re.compile(
    r"(?P<y>\d{4})\s*년\s*(?P<m>\d{1,2})\s*월\s*(?P<d>\d{1,2})\s*일"
    r"(?:\s*(?P<ampm>오전|오후)?\s*(?P<h>\d{1,2})\s*시"
    r"(?:\s*(?P<min>\d{1,2})\s*분)?)?\s*$"
)
_NUMERIC_DATE = re.compile(
    r"(?P<y>\d{4})\s*[./-]\s*(?P<m>\d{1,2})\s*[./-]\s*(?P<d>\d{1,2})"
    r"(?:[ T](?P<h>\d{1,2}):(?P<min>\d{2})(?::(?P<s>\d{2}))?)?\s*$"
)
_TIME_DATETIME = re.compile(
    r"<time\b[^>]*\bdatetime\s*=\s*[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)
_META_PUBLISHED = re.compile(
    r"<meta\b[^>]*(?:property|name|itemprop)\s*=\s*[\"']"
    r"(?:article:published_time|og:published_time|datePublished)"
    r"[\"'][^>]*\bcontent\s*=\s*[\"']([^\"']+)[\"']"
    r"|<meta\b[^>]*\bcontent\s*=\s*[\"']([^\"']+)[\"'][^>]*"
    r"(?:property|name|itemprop)\s*=\s*[\"']"
    r"(?:article:published_time|og:published_time|datePublished)[\"']",
    re.IGNORECASE,
)
_JSONLD_PUBLISHED = re.compile(
    r'"datePublished"\s*:\s*"([^"]+)"',
    re.IGNORECASE,
)


class _Abbrev(NamedTuple):
    body: str
    hours: int


def daily_partition_date(moment: datetime | None = None) -> date:
    """Return the Asia/Seoul calendar date used as the daily data key.

    See the module docstring. Naive ``moment`` values are UTC wall time,
    matching the storage convention, not the process-local zone.
    """
    current = moment if moment is not None else datetime.now(UTC)
    if current.tzinfo is None or current.tzinfo.utcoffset(current) is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(KST).date()


def to_utc_iso(value: datetime) -> str:
    """UTC ISO-8601 with an explicit numeric offset (``+00:00``).

    Naive values are UTC wall time. Aware values keep their instant and are
    converted to UTC. The original offset belongs in ``published_raw``.
    """
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        aware = value.replace(tzinfo=UTC)
    else:
        aware = value.astimezone(UTC)
    return aware.isoformat()


def resolve_default_timezone(
    *,
    source_url: str = "",
    default_timezone: str = "",
    config: Mapping[str, Any] | None = None,
) -> str:
    """Resolve the zone for a source's zone-less published strings.

    Explicit ``default_timezone`` (field or ``config.default_timezone``)
    wins. Otherwise a ``.kr`` host uses Asia/Seoul. Every other source
    uses UTC, so a missing zone is never read as the machine's local zone.
    """
    explicit = default_timezone.strip()
    if not explicit and config is not None:
        configured = config.get("default_timezone")
        if isinstance(configured, str):
            explicit = configured.strip()
    if explicit:
        try:
            ZoneInfo(explicit)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"Unknown default_timezone {explicit!r}") from exc
        return explicit

    host = (urlparse(source_url).hostname or "").lower().rstrip(".")
    if host.endswith(".kr"):
        return KOREAN_DEFAULT_TIMEZONE
    return "UTC"


def has_explicit_offset(value: str) -> bool:
    """True when ``value`` carries a numeric offset, ``Z``, or a zone abbreviation."""
    text = value.strip()
    if _EXPLICIT_OFFSET.search(text):
        return True
    return _trailing_abbrev(text) is not None


def struct_time_to_utc(value: time.struct_time) -> datetime:
    """Convert a feedparser ``*_parsed`` struct to an aware UTC datetime.

    ``calendar.timegm`` treats the fields as UTC. ``time.mktime`` would
    treat them as local time and shift KST crawlers by nine hours.
    """
    return datetime.fromtimestamp(calendar.timegm(value), tz=UTC)


def parse_unix_timestamp(timestamp: float | None) -> datetime | None:
    """API epoch fields such as Reddit ``created_utc`` are UTC seconds."""
    if timestamp is None or isinstance(timestamp, bool):
        return None
    try:
        return datetime.fromtimestamp(float(timestamp), tz=UTC)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def parse_published(
    value: object,
    *,
    default_timezone: str = "UTC",
) -> datetime | None:
    """Parse one published value into a timezone-aware datetime.

    ``time.struct_time`` is UTC (feedparser). Strings that already have an
    offset keep that offset. Zone-less strings use ``default_timezone``.
    Integers and floats are UTC epoch seconds.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        return _attach_zone(value, default_timezone)
    if isinstance(value, date):
        return datetime(
            value.year,
            value.month,
            value.day,
            tzinfo=_zone(default_timezone),
        )
    if isinstance(value, time.struct_time):
        return struct_time_to_utc(value)
    if isinstance(value, (int, float)):
        return parse_unix_timestamp(value)
    if isinstance(value, bytes):
        value = value.decode("utf-8", "replace")
    if isinstance(value, str):
        return _parse_datetime_string(value, default_timezone=default_timezone)
    return None


def parse_feed_entry(
    entry: Mapping[str, Any],
    *,
    default_timezone: str = "UTC",
) -> tuple[datetime | None, str | None]:
    """Parse a feed or API record.

    A raw published string is preferred over ``*_parsed``. feedparser copies
    a zone-less clock into ``published_parsed`` as if it were UTC, which is
    wrong for Korean sources. ``*_parsed`` is the fallback and is always UTC
    via :func:`struct_time_to_utc`. The second item is the original raw
    string when one was present.
    """
    raw = _first_raw_string(entry)
    if raw:
        parsed = parse_published(raw, default_timezone=default_timezone)
        if parsed is not None:
            return parsed, raw

    for key in _STRUCT_KEYS:
        candidate = entry.get(key)
        if isinstance(candidate, time.struct_time):
            return struct_time_to_utc(candidate), raw

    for key in _UNIX_KEYS:
        candidate = entry.get(key)
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            parsed = parse_unix_timestamp(candidate)
            if parsed is not None:
                return parsed, raw
    return None, raw


def parse_html_published(
    html: str,
    *,
    default_timezone: str = "UTC",
) -> tuple[datetime | None, str | None]:
    """Read a published instant from HTML ``<time>``, meta, or JSON-LD.

    Zone-less attribute values use ``default_timezone``. The raw attribute
    text is returned unchanged so storage can keep ``published_raw``.
    """
    if not html or not html.strip():
        return None, None
    for pattern in (_TIME_DATETIME, _META_PUBLISHED, _JSONLD_PUBLISHED):
        for match in pattern.finditer(html):
            raw = next((group for group in match.groups() if group), None)
            if not raw or not raw.strip():
                continue
            parsed = parse_published(raw.strip(), default_timezone=default_timezone)
            if parsed is not None:
                return parsed, raw.strip()
    return None, None


def _first_raw_string(entry: Mapping[str, Any]) -> str | None:
    for key in _RAW_STRING_KEYS:
        raw = entry.get(key)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return None


def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown default_timezone {name!r}") from exc


def _attach_zone(value: datetime, default_timezone: str) -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        return value.replace(tzinfo=_zone(default_timezone))
    return value


def _trailing_abbrev(text: str) -> _Abbrev | None:
    match = _TRAILING_ABBREV.match(text.strip())
    if match is None:
        return None
    hours = _ABBREV_HOURS.get(match.group("abbr").upper())
    if hours is None:
        return None
    return _Abbrev(match.group("body").strip(), hours)


def _parse_datetime_string(text: str, *, default_timezone: str) -> datetime | None:
    stripped = text.strip()
    if not stripped:
        return None
    abbrev = _trailing_abbrev(stripped)
    parsed = _parse_known_formats(stripped)
    if parsed is not None:
        return _finalize_parsed(parsed, abbrev, default_timezone)
    if abbrev is not None:
        parsed = _parse_known_formats(abbrev.body)
        if parsed is not None:
            return _finalize_parsed(parsed, abbrev, default_timezone)

    labeled = _LABEL_PREFIX.sub("", stripped).strip()
    for candidate in (stripped, labeled):
        parsed = _parse_korean(candidate)
        if parsed is None:
            parsed = _parse_numeric(candidate)
        if parsed is not None:
            return _finalize_parsed(parsed, abbrev, default_timezone)

    parsed = _parse_with_dateutil(stripped)
    if parsed is not None:
        return _finalize_parsed(parsed, abbrev, default_timezone)
    return None


def _finalize_parsed(
    parsed: datetime,
    abbrev: _Abbrev | None,
    default_timezone: str,
) -> datetime:
    if parsed.tzinfo is not None and parsed.tzinfo.utcoffset(parsed) is not None:
        return parsed
    if abbrev is not None:
        return parsed.replace(tzinfo=timezone(timedelta(hours=abbrev.hours)))
    return parsed.replace(tzinfo=_zone(default_timezone))


def _parse_known_formats(text: str) -> datetime | None:
    iso_text = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        parsed = datetime.fromisoformat(iso_text)
    except ValueError:
        parsed = None
    if isinstance(parsed, datetime):
        return parsed
    try:
        parsed_rfc = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if isinstance(parsed_rfc, datetime):
        return parsed_rfc
    return None


def _parse_korean(text: str) -> datetime | None:
    match = _KOREAN_DATE.fullmatch(text.strip())
    if match is None:
        return None
    hour = int(match.group("h") or 0)
    ampm = match.group("ampm")
    if ampm == "오후" and hour < 12:
        hour += 12
    elif ampm == "오전" and hour == 12:
        hour = 0
    return _build_datetime(
        int(match.group("y")),
        int(match.group("m")),
        int(match.group("d")),
        hour,
        int(match.group("min") or 0),
        0,
    )


def _parse_numeric(text: str) -> datetime | None:
    match = _NUMERIC_DATE.fullmatch(text.strip())
    if match is None:
        return None
    return _build_datetime(
        int(match.group("y")),
        int(match.group("m")),
        int(match.group("d")),
        int(match.group("h") or 0),
        int(match.group("min") or 0),
        int(match.group("s") or 0),
    )


def _build_datetime(
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> datetime | None:
    try:
        return datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None


def _parse_with_dateutil(text: str) -> datetime | None:
    """Optional fallback for odd HTML dates when python-dateutil is installed."""
    if not re.search(r"\d{4}", text):
        return None
    try:
        from dateutil import parser as dateutil_parser
    except ImportError:
        return None
    try:
        parsed = dateutil_parser.parse(text, fuzzy=False)
    except (ValueError, OverflowError, TypeError):
        return None
    if isinstance(parsed, datetime):
        return parsed
    return None
