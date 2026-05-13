"""URL canonicalization helpers.

`canonical_url` normalizes a URL string for use as a deduplication key:
- lower-case scheme + host
- strip whitespace
- drop default ports (80/443)
- drop fragment (#…)
- strip common tracking parameters (utm_*, gclid, fbclid, msclkid,
  yclid, mc_cid, mc_eid, ref, ref_src, ref_url, igshid, _ga, _gl,
  campaign_*)
- collapse trailing slash on the path (except root)
- sort the remaining query parameters so order doesn't break equality
"""

from __future__ import annotations

from urllib.parse import (
    parse_qsl,
    quote,
    unquote,
    urlencode,
    urlsplit,
    urlunsplit,
)

# Parameters that are advertising / analytics noise and should be stripped.
_TRACKING_PARAMS: frozenset[str] = frozenset(
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "utm_id",
        "utm_name",
        "utm_brand",
        "utm_social",
        "utm_social-type",
        "utm_referrer",
        "gclid",
        "gbraid",
        "wbraid",
        "fbclid",
        "msclkid",
        "yclid",
        "twclid",
        "ttclid",
        "mc_cid",
        "mc_eid",
        "igshid",
        "_ga",
        "_gl",
        "ref",
        "ref_src",
        "ref_url",
        "campaign",
        "campaign_id",
        "campaign_name",
        "trk",
        "trkInfo",
    }
)

_DEFAULT_PORTS = {"http": 80, "https": 443}


def canonical_url(url: str | None) -> str:
    """Return a canonical form of ``url`` suitable for deduplication.

    Returns the empty string for empty / whitespace-only input. Returns the
    original string (best-effort lower-cased + trimmed) if the URL is too
    malformed to parse.
    """
    if not url:
        return ""

    raw = url.strip()
    if not raw:
        return ""

    try:
        parts = urlsplit(raw)
    except ValueError:
        return raw.lower()

    scheme = parts.scheme.lower()
    netloc = parts.netloc

    # Split userinfo / host:port
    userinfo = ""
    host = netloc
    if "@" in netloc:
        userinfo, host = netloc.split("@", 1)
    if ":" in host and not host.startswith("["):
        host, _, port = host.rpartition(":")
        try:
            port_int = int(port)
            if scheme in _DEFAULT_PORTS and port_int == _DEFAULT_PORTS[scheme]:
                port = ""
        except ValueError:
            port = port
        host = host.lower()
        if port:
            host = f"{host}:{port}"
    else:
        host = host.lower()

    new_netloc = f"{userinfo}@{host}" if userinfo else host

    # Normalize path
    path = parts.path or ""
    if path:
        try:
            path = quote(unquote(path), safe="/:@!$&'()*+,;=-._~%")
        except Exception:
            pass
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
    else:
        path = ""

    # Strip tracking parameters, sort the rest
    keep: list[tuple[str, str]] = []
    for key, value in parse_qsl(parts.query, keep_blank_values=False):
        if key in _TRACKING_PARAMS:
            continue
        keep.append((key, value))
    keep.sort()
    query = urlencode(keep, doseq=True)

    # Drop fragment
    fragment = ""

    return urlunsplit((scheme, new_netloc, path, query, fragment))


__all__ = ["canonical_url"]
