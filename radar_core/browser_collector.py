from __future__ import annotations

import html
import os
import random
import re
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from pybreaker import CircuitBreakerError

from .adaptive_throttle import AdaptiveThrottler
from .crawl_health import CrawlHealthStore
from .exceptions import ParseError, SourceError
from .models import Article, Source
from .resilience import get_circuit_breaker_manager

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None


_DEFAULT_HEALTH_DB_PATH = "data/radar_data.duckdb"
_COOKIE_SELECTORS: tuple[str, ...] = (
    'button:has-text("Accept")',
    'button:has-text("I agree")',
    'button:has-text("동의")',
    'button:has-text("모두 동의")',
    'button:has-text("확인")',
    "#accept-cookies",
    "#cookie-accept",
    ".cookie-accept",
)


def collect_browser_sources(
    sources: Sequence[Source | Mapping[str, Any]],
    category: str,
    *,
    timeout: int = 15_000,
    health_db_path: str | None = None,
) -> tuple[list[Article], list[str]]:
    collector = BrowserCollector(
        health_db_path=health_db_path
        or os.environ.get("RADAR_CRAWL_HEALTH_DB_PATH", _DEFAULT_HEALTH_DB_PATH),
        default_timeout=timeout,
    )
    return collector.collect_browser_sources(sources=sources, category=category)


class BrowserCollector:
    def __init__(self, *, health_db_path: str, default_timeout: int = 15_000):
        self._health_store = CrawlHealthStore(health_db_path)
        self._default_timeout = max(1_000, default_timeout)
        self._throttler = AdaptiveThrottler(min_delay=2.0, max_delay=30.0)
        self._manager = get_circuit_breaker_manager()

    def collect_browser_sources(
        self,
        sources: Sequence[Source | Mapping[str, Any]],
        category: str,
    ) -> tuple[list[Article], list[str]]:
        if sync_playwright is None:
            raise ImportError("Install playwright: pip install 'radar-core[browser]'")

        articles: list[Article] = []
        errors: list[str] = []

        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    headless=True,
                    args=["--headless", "--disable-dev-shm-usage", "--no-sandbox"],
                )
                context = browser.new_context()

                try:
                    for source in sources:
                        source_name = _source_string(source, "name", "unknown")
                        if self._health_store.is_disabled(source_name):
                            errors.append(
                                f"{source_name}: Source disabled (crawl health threshold reached)"
                            )
                            continue

                        try:
                            breaker = self._manager.get_breaker(source_name)
                            source_articles = breaker.call(
                                self._collect_source,
                                source,
                                category,
                                context,
                            )
                            articles.extend(source_articles)
                            self._throttler.record_success(source_name)
                            self._health_store.record_success(
                                source_name,
                                self._throttler.get_current_delay(source_name),
                            )
                        except CircuitBreakerError:
                            errors.append(
                                f"{source_name}: Circuit breaker open (source unavailable)"
                            )
                        except SourceError as exc:
                            errors.append(str(exc))
                            self._record_failure(source_name, exc)
                        except ParseError as exc:
                            errors.append(f"{source_name}: {exc}")
                            self._record_failure(source_name, exc)
                        except Exception as exc:
                            errors.append(
                                f"{source_name}: Unexpected error - {type(exc).__name__}: {exc}"
                            )
                            self._record_failure(source_name, exc)

                        time.sleep(random.uniform(2.0, 5.0))
                finally:
                    context.close()
                    browser.close()
        finally:
            self._health_store.close()

        return articles, errors

    def _record_failure(self, source_name: str, exc: Exception) -> None:
        self._throttler.record_failure(source_name)
        self._health_store.record_failure(
            source_name,
            str(exc),
            self._throttler.get_current_delay(source_name),
        )

    def _collect_source(
        self,
        source: Source | Mapping[str, Any],
        category: str,
        context: Any,
    ) -> list[Article]:
        source_name = _source_string(source, "name", "unknown")
        source_type = _source_string(source, "type", "browser").lower()
        source_url = _source_string(source, "url", "")
        if not source_url:
            raise SourceError(source_name, "Missing source URL")

        if source_type not in {"browser", "web", "html", "js"}:
            raise SourceError(source_name, f"Unsupported source type '{source_type}'")

        config = _source_config(source)
        timeout_ms = _source_int(config, "timeout", self._default_timeout)
        wait_for_selector = _source_optional_string(config, "wait_for")
        content_selector = _source_optional_string(config, "content_selector")
        title_selector = _source_optional_string(config, "title_selector")
        link_selector = _source_optional_string(config, "link_selector")

        page = context.new_page()
        page.set_default_timeout(timeout_ms)

        try:
            _ = page.goto(source_url, wait_until="domcontentloaded", timeout=timeout_ms)
            _dismiss_cookie_banner(page)

            if wait_for_selector:
                page.wait_for_selector(wait_for_selector, timeout=timeout_ms)

            extraction_page = _resolve_naver_frame(page)
            encoding = _detect_page_encoding(extraction_page)
            page_html = extraction_page.content()
            summary = _extract_summary(
                extraction_page, content_selector, page_html, encoding
            )
            title = _extract_title(extraction_page, title_selector)

            items = _extract_articles_from_links(
                extraction_page=extraction_page,
                source_name=source_name,
                category=category,
                fallback_title=title,
                fallback_summary=summary,
                fallback_link=source_url,
                link_selector=link_selector,
            )
            if items:
                return items

            return [
                Article(
                    title=title,
                    link=source_url,
                    summary=summary,
                    published=datetime.now(UTC),
                    source=source_name,
                    category=category,
                )
            ]
        except SourceError:
            raise
        except Exception as exc:
            raise ParseError(
                f"Failed to collect browser content from {source_name}: {exc}"
            ) from exc
        finally:
            page.close()


def _source_config(source: Source | Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(source, Mapping):
        raw = source.get("config")
    else:
        raw = getattr(source, "config", None)

    if isinstance(raw, Mapping):
        return raw
    return {}


def _source_string(source: Source | Mapping[str, Any], key: str, default: str) -> str:
    value: object
    if isinstance(source, Mapping):
        value = source.get(key, default)
    else:
        value = getattr(source, key, default)
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or default
    return default


def _source_optional_string(config: Mapping[str, Any], key: str) -> str | None:
    value = config.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _source_int(config: Mapping[str, Any], key: str, default: int) -> int:
    value = config.get(key)
    if isinstance(value, int):
        return max(1_000, value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return max(1_000, int(stripped))
    return max(1_000, default)


def _dismiss_cookie_banner(page: Any) -> None:
    for selector in _COOKIE_SELECTORS:
        try:
            locator = page.locator(selector)
            if locator.count() == 0:
                continue
            first = locator.first
            if first.is_visible(timeout=250):
                first.click(timeout=500)
                return
        except Exception:
            continue


def _resolve_naver_frame(page: Any) -> Any:
    host = ""
    try:
        host = page.url.lower()
    except Exception:
        host = ""

    if "naver.com" not in host:
        return page

    for frame in page.frames:
        frame_name = (frame.name or "").lower()
        frame_url = (frame.url or "").lower()
        if "mainframe" in frame_name:
            return frame
        if any(
            token in frame_url for token in ("blog.naver", "post.naver", "land.naver")
        ):
            return frame
    return page


def _detect_page_encoding(page: Any) -> str:
    detected = "utf-8"

    try:
        character_set = page.evaluate("() => document.characterSet || ''")
        if isinstance(character_set, str) and character_set.strip():
            detected = character_set.strip().lower()
    except Exception:
        pass

    if detected in {"euc-kr", "ks_c_5601-1987", "cp949"}:
        return "euc-kr"

    try:
        html_text = page.content().lower()
    except Exception:
        return detected

    if re.search(r"charset\s*=\s*['\"]?(euc-kr|ks_c_5601-1987|cp949)", html_text):
        return "euc-kr"
    return detected


def _extract_title(page: Any, selector: str | None) -> str:
    if selector:
        try:
            title = page.locator(selector).first.inner_text(timeout=1_000).strip()
            if title:
                return html.unescape(title)
        except Exception:
            pass

    try:
        page_title = page.title().strip()
        if page_title:
            return html.unescape(page_title)
    except Exception:
        pass
    return "(no title)"


def _extract_summary(
    page: Any,
    selector: str | None,
    page_html: str,
    encoding: str,
) -> str:
    if selector:
        try:
            text = page.locator(selector).first.inner_text(timeout=1_500).strip()
            if text:
                return html.unescape(text)
        except Exception:
            pass

    try:
        body_text = page.inner_text("body", timeout=1_500).strip()
    except Exception:
        body_text = ""

    if body_text:
        return html.unescape(body_text[:4_000])

    if encoding == "euc-kr":
        repaired = _decode_euc_kr_fallback(page_html)
        if repaired:
            return html.unescape(repaired)

    cleaned = re.sub(r"<[^>]+>", " ", page_html)
    return html.unescape(re.sub(r"\s+", " ", cleaned).strip())[:4_000]


def _decode_euc_kr_fallback(raw_html: str) -> str:
    if not raw_html:
        return ""

    try:
        repaired = raw_html.encode("latin-1", errors="ignore").decode("euc-kr")
        return re.sub(r"\s+", " ", repaired).strip()[:4_000]
    except Exception:
        return ""


def _extract_articles_from_links(
    *,
    extraction_page: Any,
    source_name: str,
    category: str,
    fallback_title: str,
    fallback_summary: str,
    fallback_link: str,
    link_selector: str | None,
) -> list[Article]:
    if not link_selector:
        return []

    try:
        links: list[dict[str, str]] = extraction_page.eval_on_selector_all(
            link_selector,
            """
            (nodes) => nodes
                .map((node) => {
                    const href = node.getAttribute('href') || '';
                    const text = (node.textContent || '').trim();
                    return { href, text };
                })
                .filter((item) => item.href)
            """,
        )
    except Exception:
        return []

    items: list[Article] = []
    for entry in links[:30]:
        href = entry.get("href", "").strip()
        text = entry.get("text", "").strip()
        if not href:
            continue

        items.append(
            Article(
                title=html.unescape(text or fallback_title),
                link=href,
                summary=fallback_summary,
                published=datetime.now(UTC),
                source=source_name,
                category=category,
            )
        )

    if not items and fallback_link:
        return [
            Article(
                title=fallback_title,
                link=fallback_link,
                summary=fallback_summary,
                published=datetime.now(UTC),
                source=source_name,
                category=category,
            )
        ]
    return items
