from __future__ import annotations

import pytest

from radar_core.url_utils import canonical_url


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("https://example.com/a?utm_source=x&id=1", "https://example.com/a?id=1"),
        ("HTTP://Example.COM:80/path/", "http://example.com/path"),
        ("https://example.com/a#section", "https://example.com/a"),
        ("https://example.com/a?b=2&a=1", "https://example.com/a?a=1&b=2"),
        (
            "https://example.com/a?gclid=xyz&fbclid=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        ("  https://example.com/  ", "https://example.com/"),
        ("https://example.com/", "https://example.com/"),
        ("https://example.com:443/path", "https://example.com/path"),
        ("", ""),
        (None, ""),
    ],
)
def test_canonical_url(raw, expected) -> None:
    assert canonical_url(raw) == expected


def test_canonical_url_strips_all_known_tracking_params() -> None:
    url = (
        "https://e.com/p?id=1&utm_source=x&utm_medium=y&utm_campaign=z"
        "&gclid=g&fbclid=f&mc_cid=m&_ga=ga&ref=r&keep=stay"
    )
    assert canonical_url(url) == "https://e.com/p?id=1&keep=stay"


def test_canonical_url_preserves_userinfo_and_non_default_port() -> None:
    url = "https://user:pw@HOST.example.com:8443/path/?b=2&a=1"
    assert canonical_url(url) == "https://user:pw@host.example.com:8443/path?a=1&b=2"
