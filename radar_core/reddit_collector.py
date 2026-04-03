"""Reddit JSON API collector for radar-core.

Reddit has blocked RSS feeds (403 Forbidden) but the JSON API endpoint
still works. This collector uses the .json endpoint with proper rate
limiting, browser-like headers, and fallback strategies.

Usage:
    In config YAML, use type: reddit instead of type: rss for Reddit sources:

    - name: r/RealEstate
      type: reddit
      url: https://www.reddit.com/r/RealEstate/
      config:
        sort: new  # hot, new, top, rising (default: new)
        limit: 25  # max items to fetch (default: 25, max: 100)

Features:
    - Browser-like headers to avoid bot detection
    - Human-like timing with random jitter
    - Fallback to old.reddit.com on rate limiting
    - Automatic retry with exponential backoff
    - User-Agent rotation

References:
    - https://dev.to/agenthustler/how-to-scrape-reddit-in-2026-3-methods-that-still-work-402b
    - https://til.simonwillison.net/reddit/scraping-reddit-json
    - https://painonsocial.com/blog/reddit-api-rate-limits-workaround
"""

from __future__ import annotations

import html
import random
import re
import time
import threading
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .crawl_health import CrawlHealthStore
from .exceptions import NetworkError, ParseError, SourceError
from .models import Article, Source

# Reddit rate limits: 100 requests per 5 minutes = 1 request per 3 seconds
# We use 3 seconds base + jitter for safety
REDDIT_MIN_INTERVAL = 3.0
REDDIT_JITTER = 1.5  # Random jitter ±1.5 seconds
REDDIT_TIMEOUT = 30

# Rotate between different User-Agent strings per Reddit API guidelines
# Format: [platform]:[app_id]:[version] (by [username])
REDDIT_USER_AGENTS = [
    "python:radar-core:v1.0 (by /u/RadarDataCollector) Data aggregation",
    "python:datahub-collector:v2.0 (by /u/DataHubBot) Research purposes",
    "python:news-aggregator:v1.5 (by /u/NewsAggregator) Content collection",
]

# Browser-like headers for better compatibility
REDDIT_BROWSER_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
}

_reddit_rate_limiter_lock = threading.Lock()
_reddit_last_request: float = 0.0
_reddit_ua_index: int = 0


def _get_next_user_agent() -> str:
    """Rotate through User-Agent strings."""
    global _reddit_ua_index
    with _reddit_rate_limiter_lock:
        ua = REDDIT_USER_AGENTS[_reddit_ua_index % len(REDDIT_USER_AGENTS)]
        _reddit_ua_index += 1
        return ua


def _reddit_rate_limit() -> None:
    """Global rate limiter with human-like jitter."""
    global _reddit_last_request
    with _reddit_rate_limiter_lock:
        now = time.monotonic()
        elapsed = now - _reddit_last_request
        # Add random jitter to appear more human-like
        target_interval = REDDIT_MIN_INTERVAL + random.uniform(
            -REDDIT_JITTER, REDDIT_JITTER
        )
        target_interval = max(1.5, target_interval)  # Never less than 1.5s
        if elapsed < target_interval:
            time.sleep(target_interval - elapsed)
        _reddit_last_request = time.monotonic()


def _create_reddit_session(user_agent: str | None = None) -> requests.Session:
    """Create a requests session with browser-like configuration.

    Args:
        user_agent: Optional specific User-Agent. If None, rotates automatically.
    """
    session = requests.Session()

    # Start with browser-like headers
    headers = REDDIT_BROWSER_HEADERS.copy()
    headers["User-Agent"] = user_agent or _get_next_user_agent()
    session.headers.update(headers)

    # Retry strategy with longer backoff for rate limits
    retry_strategy = Retry(
        total=3,
        backoff_factor=3,  # 3s, 6s, 12s
        status_forcelist=[500, 502, 503, 504],  # Don't auto-retry 429
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def _normalize_reddit_url(url: str, use_old_reddit: bool = False) -> str:
    """Normalize Reddit URL to JSON endpoint format.

    Converts various Reddit URL formats to the JSON API endpoint:
    - https://www.reddit.com/r/RealEstate/ -> https://www.reddit.com/r/RealEstate/new.json
    - https://www.reddit.com/r/RealEstate/.rss -> https://www.reddit.com/r/RealEstate/new.json
    - https://reddit.com/r/RealEstate -> https://www.reddit.com/r/RealEstate/new.json

    Args:
        url: Reddit URL to normalize
        use_old_reddit: If True, use old.reddit.com (less strict rate limiting)
    """
    # Remove trailing .rss if present
    url = re.sub(r"\.rss/?$", "", url)

    # Normalize domain
    url = url.replace("://reddit.com", "://www.reddit.com")
    url = url.replace("://old.reddit.com", "://www.reddit.com")

    # Switch to old.reddit.com if requested (lighter weight, sometimes less strict)
    if use_old_reddit:
        url = url.replace("://www.reddit.com", "://old.reddit.com")

    # Ensure trailing slash
    if not url.endswith("/"):
        url += "/"

    return url


def _build_reddit_json_url(base_url: str, sort: str = "new", limit: int = 25) -> str:
    """Build the JSON API URL with query parameters.

    Args:
        base_url: Normalized subreddit URL
        sort: Sort order (hot, new, top, rising)
        limit: Number of items to fetch (max 100)

    Returns:
        Complete JSON API URL with parameters
    """
    # Append sort method and .json
    json_url = f"{base_url}{sort}.json"

    # Add query parameters
    params = {
        "limit": min(limit, 100),
        "raw_json": 1,  # Get unescaped JSON
    }

    return f"{json_url}?{urlencode(params)}"


def _parse_reddit_timestamp(timestamp: float | int | None) -> datetime | None:
    """Parse Reddit Unix timestamp to datetime."""
    if timestamp is None:
        return None
    try:
        return datetime.fromtimestamp(float(timestamp), tz=UTC)
    except (ValueError, TypeError, OSError):
        return None


def _extract_reddit_text(data: dict[str, Any]) -> str:
    """Extract text content from Reddit post data.

    Prefers selftext for text posts, falls back to title for link posts.
    """
    selftext = data.get("selftext", "")
    if selftext and selftext != "[removed]" and selftext != "[deleted]":
        # Clean up markdown formatting slightly
        text = selftext.strip()
        # Limit to reasonable length
        if len(text) > 2000:
            text = text[:2000] + "..."
        return text

    # For link posts, include the URL
    url = data.get("url", "")
    if url and not url.startswith("https://www.reddit.com"):
        return f"Link: {url}"

    return ""


def _parse_reddit_response(
    response_data: dict[str, Any],
    source_name: str,
    category: str,
    limit: int = 25,
) -> list[Article]:
    """Parse Reddit JSON response into Article objects.

    Args:
        response_data: Parsed JSON response from Reddit
        source_name: Name of the source for attribution
        category: Category for the articles
        limit: Maximum number of articles to return

    Returns:
        List of Article objects
    """
    articles: list[Article] = []

    # Reddit JSON structure: {"kind": "Listing", "data": {"children": [...]}}
    if response_data.get("kind") != "Listing":
        return articles

    children = response_data.get("data", {}).get("children", [])

    for child in children[:limit]:
        if child.get("kind") != "t3":  # t3 = link/post
            continue

        data = child.get("data", {})

        # Skip removed/deleted posts
        if data.get("removed_by_category") or data.get("removed"):
            continue

        title = data.get("title", "")
        if not title or title == "[deleted]":
            continue

        # Build permalink URL
        permalink = data.get("permalink", "")
        if permalink:
            link = f"https://www.reddit.com{permalink}"
        else:
            post_id = data.get("id", "")
            subreddit = data.get("subreddit", "")
            if post_id and subreddit:
                link = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}/"
            else:
                continue

        # Extract content
        summary = _extract_reddit_text(data)

        # Get timestamp
        published = _parse_reddit_timestamp(data.get("created_utc"))

        # Build metadata
        author = data.get("author", "[deleted]")
        score = data.get("score", 0)
        num_comments = data.get("num_comments", 0)
        subreddit = data.get("subreddit", "")

        # Format summary with metadata
        meta_info = (
            f"[r/{subreddit}] by u/{author} | {score} points | {num_comments} comments"
        )
        if summary:
            full_summary = f"{meta_info}\n\n{summary}"
        else:
            full_summary = meta_info

        articles.append(
            Article(
                title=html.unescape(title.strip()),
                link=link,
                summary=html.unescape(full_summary.strip()),
                published=published,
                source=source_name,
                category=category,
            )
        )

    return articles


def _fetch_reddit_json(
    url: str,
    session: requests.Session,
    timeout: int,
    source_name: str,
) -> dict[str, Any]:
    """Fetch JSON from Reddit with fallback strategies.

    Tries www.reddit.com first, falls back to old.reddit.com on 429.
    """
    response = session.get(url, timeout=timeout)

    # Handle rate limiting with fallback
    if response.status_code == 429:
        # Try old.reddit.com as fallback (sometimes less strict)
        if "www.reddit.com" in url:
            old_url = url.replace("www.reddit.com", "old.reddit.com")
            # Wait a bit before retry
            time.sleep(2 + random.uniform(0, 1))
            response = session.get(old_url, timeout=timeout)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "60")
                try:
                    wait_time = min(int(retry_after), 120)  # Cap at 2 minutes
                except ValueError:
                    wait_time = 60
                raise SourceError(
                    source_name,
                    f"Reddit rate limit exceeded. Retry after {wait_time} seconds",
                )
        else:
            retry_after = response.headers.get("Retry-After", "60")
            raise SourceError(
                source_name,
                f"Reddit rate limit exceeded. Retry after {retry_after} seconds",
            )

    # Handle other errors
    if response.status_code == 403:
        raise SourceError(
            source_name,
            "Reddit returned 403 Forbidden. The subreddit may be private or banned.",
        )

    if response.status_code == 404:
        raise SourceError(
            source_name, "Reddit returned 404. The subreddit may not exist."
        )

    response.raise_for_status()

    # Parse JSON
    try:
        return response.json()
    except ValueError as exc:
        raise ParseError(
            f"Failed to parse Reddit JSON from {source_name}: {exc}"
        ) from exc


def collect_reddit_source(
    source: Source,
    *,
    category: str,
    limit: int = 25,
    timeout: int = REDDIT_TIMEOUT,
    session: requests.Session | None = None,
) -> list[Article]:
    """Collect articles from a single Reddit source.

    Uses browser-like headers and fallback strategies to avoid rate limiting.

    Args:
        source: Reddit source configuration
        category: Category for the articles
        limit: Maximum number of articles to fetch
        timeout: Request timeout in seconds
        session: Optional requests session to reuse

    Returns:
        List of Article objects

    Raises:
        NetworkError: On network/connection errors
        ParseError: On JSON parsing errors
        SourceError: On Reddit API errors
    """
    # Rate limit with jitter
    _reddit_rate_limit()

    # Parse config
    config = source.config if hasattr(source, "config") and source.config else {}
    sort = config.get("sort", "new")
    fetch_limit = config.get("limit", limit)

    # Build URL
    base_url = _normalize_reddit_url(source.url)
    json_url = _build_reddit_json_url(base_url, sort=sort, limit=fetch_limit)

    # Create or use session
    close_session = False
    if session is None:
        session = _create_reddit_session()
        close_session = True

    try:
        data = _fetch_reddit_json(json_url, session, timeout, source.name)

        # Parse articles
        articles = _parse_reddit_response(
            data, source.name, category, limit=fetch_limit
        )

        return articles

    except requests.exceptions.Timeout as exc:
        raise NetworkError(f"Timeout fetching {source.name}: {exc}") from exc
    except requests.exceptions.ConnectionError as exc:
        raise NetworkError(f"Connection error fetching {source.name}: {exc}") from exc
    except requests.exceptions.RequestException as exc:
        raise SourceError(source.name, f"Request failed: {exc}", exc) from exc
    finally:
        if close_session:
            session.close()


def collect_reddit_sources(
    sources: list[Source],
    *,
    category: str,
    limit: int = 25,
    timeout: int = REDDIT_TIMEOUT,
    health_db_path: str | None = None,
) -> tuple[list[Article], list[str]]:
    """Collect articles from multiple Reddit sources.

    Args:
        sources: List of Reddit source configurations
        category: Category for the articles
        limit: Maximum number of articles per source
        timeout: Request timeout in seconds
        health_db_path: Optional path to health tracking database

    Returns:
        Tuple of (articles, errors)
    """
    articles: list[Article] = []
    errors: list[str] = []

    session = _create_reddit_session()
    health_store = None
    if health_db_path:
        health_store = CrawlHealthStore(health_db_path)

    try:
        for source in sources:
            # Check if source is disabled
            if health_store and health_store.is_disabled(source.name):
                errors.append(
                    f"{source.name}: Source disabled (crawl health threshold reached)"
                )
                continue

            try:
                source_articles = collect_reddit_source(
                    source,
                    category=category,
                    limit=limit,
                    timeout=timeout,
                    session=session,
                )
                articles.extend(source_articles)

                # Record success
                if health_store:
                    health_store.record_success(source.name, REDDIT_MIN_INTERVAL)

            except (NetworkError, ParseError, SourceError) as exc:
                errors.append(str(exc))
                if health_store:
                    health_store.record_failure(
                        source.name, str(exc), REDDIT_MIN_INTERVAL
                    )
            except Exception as exc:
                error_msg = (
                    f"{source.name}: Unexpected error - {type(exc).__name__}: {exc}"
                )
                errors.append(error_msg)
                if health_store:
                    health_store.record_failure(
                        source.name, str(exc), REDDIT_MIN_INTERVAL
                    )
    finally:
        session.close()
        if health_store:
            health_store.close()

    return articles, errors
