"""Article lineage helpers.

`Article` and `RadarStorage` already carry optional ``run_id``,
``collector_version`` and ``fetch_status`` columns. These helpers make it
easy for individual radars to populate them consistently so quality
investigations can later say "regression started in run X / version Y".

Usage:

    >>> run_id = make_run_id("blogradar")
    >>> version = get_radar_core_version()

Both helpers are pure and safe to call in workflow scripts.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from importlib import metadata


def make_run_id(prefix: str = "radar") -> str:
    """Return ``<prefix>-<YYYYmmddTHHMMSSZ>-<short_uuid>``.

    Honors the ``GITHUB_RUN_ID`` env var when present so workflow runs share
    the same identifier across collector / analyzer / reporter stages.
    """
    gh = os.environ.get("GITHUB_RUN_ID")
    if gh:
        return f"{prefix}-gh-{gh}"
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    short = uuid.uuid4().hex[:8]
    return f"{prefix}-{stamp}-{short}"


def get_radar_core_version() -> str:
    """Return the installed radar-core version (best-effort, empty on error)."""
    try:
        return metadata.version("radar-core")
    except metadata.PackageNotFoundError:
        return ""


__all__ = ["make_run_id", "get_radar_core_version"]
