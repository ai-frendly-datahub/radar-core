from __future__ import annotations

import re

import pytest

from radar_core.lineage import get_radar_core_version, make_run_id


def test_make_run_id_default_format() -> None:
    rid = make_run_id()
    assert re.match(r"^radar-\d{8}T\d{6}Z-[0-9a-f]{8}$", rid), rid


def test_make_run_id_with_prefix() -> None:
    rid = make_run_id("blogradar")
    assert rid.startswith("blogradar-")


def test_make_run_id_honors_github_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_RUN_ID", "12345")
    rid = make_run_id("priceradar")
    assert rid == "priceradar-gh-12345"


def test_get_radar_core_version_returns_string() -> None:
    v = get_radar_core_version()
    assert isinstance(v, str)
    # 0.2.0 was just tagged today, so we should see something non-empty.
    assert v != ""
