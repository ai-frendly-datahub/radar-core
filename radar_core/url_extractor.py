"""URL-to-Markdown extraction with fallback chain.

This module provides content extraction from URLs when RSS feeds fail,
using a chain of extractors with automatic fallback.

Fallback Chain:
1. Jina.AI Reader API (r.jina.ai) - Best quality, cloud-based
2. Trafilatura - Excellent local extraction
3. html2text - Basic fallback (always available)
"""

from __future__ import annotations

import html
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import requests

from .exceptions import NetworkError

if TYPE_CHECKING:
    from typing import Callable

# Jina.AI Reader API configuration
JINA_READER_BASE_URL = "https://r.jina.ai/"
JINA_TIMEOUT = 30  # seconds
JINA_RATE_LIMIT_DELAY = 0.5  # seconds between requests


@dataclass
class ExtractedContent:
    """Result of URL content extraction."""

    title: str
    content: str  # Markdown content
    url: str
    extractor_used: str
    extraction_time: float = 0.0
    metadata: dict = field(default_factory=dict)


class URLExtractor(ABC):
    """Abstract base class for URL extractors."""

    name: str = "base"

    @abstractmethod
    def extract(self, url: str, timeout: int = 30) -> ExtractedContent | None:
        """Extract content from URL.

        Args:
            url: URL to extract content from
            timeout: Request timeout in seconds

        Returns:
            ExtractedContent if successful, None if extraction failed
        """


class JinaExtractor(URLExtractor):
    """Extract content using Jina.AI Reader API (r.jina.ai)."""

    name = "jina"

    def __init__(
        self,
        base_url: str = JINA_READER_BASE_URL,
        rate_limit_delay: float = JINA_RATE_LIMIT_DELAY,
    ):
        self.base_url = base_url.rstrip("/") + "/"
        self.rate_limit_delay = rate_limit_delay
        self._last_request_time: float = 0.0

    def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limits."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.monotonic()

    def extract(self, url: str, timeout: int = JINA_TIMEOUT) -> ExtractedContent | None:
        """Extract content using Jina.AI Reader API.

        The API converts any URL to clean, LLM-friendly markdown.

        Args:
            url: URL to extract content from
            timeout: Request timeout in seconds

        Returns:
            ExtractedContent if successful, None on failure
        """
        start_time = time.monotonic()
        self._wait_for_rate_limit()

        jina_url = f"{self.base_url}{url}"
        headers = {
            "Accept": "text/markdown",
            "User-Agent": "RadarCore/1.0 (URL Extractor)",
        }

        try:
            response = requests.get(jina_url, headers=headers, timeout=timeout)
            response.raise_for_status()

            content = response.text
            extraction_time = time.monotonic() - start_time

            # Parse title from markdown (first # heading)
            title = self._extract_title(content, url)

            return ExtractedContent(
                title=title,
                content=content,
                url=url,
                extractor_used=self.name,
                extraction_time=extraction_time,
                metadata={
                    "jina_url": jina_url,
                    "content_length": len(content),
                },
            )
        except requests.exceptions.Timeout:
            return None
        except requests.exceptions.RequestException:
            return None
        except Exception:
            return None

    def _extract_title(self, content: str, fallback_url: str) -> str:
        """Extract title from markdown content."""
        # Look for first # heading
        match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
        if match:
            return match.group(1).strip()

        # Look for Title: metadata
        match = re.search(r"^Title:\s*(.+)$", content, re.MULTILINE)
        if match:
            return match.group(1).strip()

        return fallback_url


class TrafilaturaExtractor(URLExtractor):
    """Extract content using Trafilatura library.

    Trafilatura is a Python library for web scraping and text extraction
    with excellent accuracy for article content.
    """

    name = "trafilatura"

    def __init__(self):
        self._trafilatura = None

    def _get_trafilatura(self):
        """Lazy import of trafilatura."""
        if self._trafilatura is None:
            try:
                import trafilatura

                self._trafilatura = trafilatura
            except ImportError:
                raise ImportError(
                    "trafilatura is required for TrafilaturaExtractor. "
                    "Install with: pip install 'radar-core[urlextract]'"
                )
        return self._trafilatura

    def extract(self, url: str, timeout: int = 30) -> ExtractedContent | None:
        """Extract content using Trafilatura.

        Args:
            url: URL to extract content from
            timeout: Request timeout in seconds

        Returns:
            ExtractedContent if successful, None on failure
        """
        start_time = time.monotonic()

        try:
            trafilatura = self._get_trafilatura()

            # Fetch the URL
            downloaded = trafilatura.fetch_url(url)
            if not downloaded:
                return None

            # Extract content as markdown
            content = trafilatura.extract(
                downloaded,
                output_format="markdown",
                include_links=True,
                include_images=True,
                include_tables=True,
            )

            if not content:
                return None

            # Extract metadata for title
            metadata = trafilatura.extract_metadata(downloaded)
            title = metadata.title if metadata and metadata.title else url

            extraction_time = time.monotonic() - start_time

            return ExtractedContent(
                title=title,
                content=content,
                url=url,
                extractor_used=self.name,
                extraction_time=extraction_time,
                metadata={
                    "author": metadata.author if metadata else None,
                    "date": str(metadata.date) if metadata and metadata.date else None,
                    "sitename": metadata.sitename if metadata else None,
                },
            )
        except ImportError:
            return None
        except Exception:
            return None


class Html2TextExtractor(URLExtractor):
    """Extract content using html2text library.

    This is a basic fallback that converts HTML to markdown.
    Always available as it has minimal dependencies.
    """

    name = "html2text"

    def __init__(self, verify_ssl: bool = True):
        """Initialize Html2Text extractor.

        Args:
            verify_ssl: Whether to verify SSL certificates (default True)
        """
        self.verify_ssl = verify_ssl

    def extract(self, url: str, timeout: int = 30) -> ExtractedContent | None:
        """Extract content using html2text.

        Args:
            url: URL to extract content from
            timeout: Request timeout in seconds

        Returns:
            ExtractedContent if successful, None on failure
        """
        start_time = time.monotonic()

        try:
            # Fetch the URL
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }
            response = requests.get(
                url, headers=headers, timeout=timeout, verify=self.verify_ssl
            )
            response.raise_for_status()

            # Convert HTML to markdown
            content = self._convert_html(response.text)
            extraction_time = time.monotonic() - start_time

            # Try to extract title from HTML
            title = self._extract_title_from_html(response.text, url)

            return ExtractedContent(
                title=title,
                content=content,
                url=url,
                extractor_used=self.name,
                extraction_time=extraction_time,
                metadata={
                    "content_length": len(content),
                },
            )
        except Exception:
            return None

    def _convert_html(self, raw_html: str) -> str:
        """Convert HTML to markdown-like text without requiring optional deps."""
        try:
            import html2text

            h = html2text.HTML2Text()
            h.ignore_links = False
            h.ignore_images = False
            h.body_width = 0  # No line wrapping
            return h.handle(raw_html)
        except ImportError:
            return self._fallback_text_from_html(raw_html)

    def _fallback_text_from_html(self, raw_html: str) -> str:
        """Best-effort HTML to text conversion using the standard library only."""
        cleaned = re.sub(
            r"<(script|style)[^>]*>.*?</\1>",
            "",
            raw_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        cleaned = re.sub(
            r"</?(p|div|section|article|li|ul|ol|h[1-6]|br|tr|td|th)[^>]*>",
            "\n",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(r"<[^>]+>", "", cleaned)
        cleaned = html.unescape(cleaned)
        cleaned = re.sub(r"\r\n?", "\n", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    def _extract_title_from_html(self, html: str, fallback: str) -> str:
        """Extract title from HTML <title> tag."""
        match = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return fallback


class ReadabilityExtractor(URLExtractor):
    """Extract content using readability-lxml library.

    Uses Mozilla's Readability algorithm to extract main article content.
    """

    name = "readability"

    def __init__(self, verify_ssl: bool = True):
        """Initialize Readability extractor.

        Args:
            verify_ssl: Whether to verify SSL certificates (default True)
        """
        self.verify_ssl = verify_ssl

    def extract(self, url: str, timeout: int = 30) -> ExtractedContent | None:
        """Extract content using readability-lxml.

        Args:
            url: URL to extract content from
            timeout: Request timeout in seconds

        Returns:
            ExtractedContent if successful, None on failure
        """
        start_time = time.monotonic()

        try:
            from readability import Document
            import html2text

            # Fetch the URL
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }
            response = requests.get(
                url, headers=headers, timeout=timeout, verify=self.verify_ssl
            )
            response.raise_for_status()

            # Use readability to extract main content
            doc = Document(response.text)
            title = doc.title()
            html_content = doc.summary()

            # Convert cleaned HTML to markdown
            h = html2text.HTML2Text()
            h.ignore_links = False
            h.ignore_images = False
            h.body_width = 0

            content = h.handle(html_content)
            extraction_time = time.monotonic() - start_time

            return ExtractedContent(
                title=title if title else url,
                content=content,
                url=url,
                extractor_used=self.name,
                extraction_time=extraction_time,
                metadata={
                    "content_length": len(content),
                },
            )
        except ImportError:
            return None
        except Exception:
            return None


class URLExtractorChain:
    """Chain of extractors with automatic fallback.

    Tries each extractor in order until one succeeds.

    Default chain:
    1. Jina.AI Reader API (best quality)
    2. Trafilatura (excellent local extraction)
    3. Readability + html2text (article extraction)
    4. html2text (basic fallback)
    """

    def __init__(
        self,
        extractors: list[URLExtractor] | None = None,
        on_fallback: Callable[[str, str, str], None] | None = None,
    ):
        """Initialize extractor chain.

        Args:
            extractors: List of extractors to try in order.
                       If None, uses default chain.
            on_fallback: Optional callback when falling back to next extractor.
                        Called with (url, failed_extractor_name, next_extractor_name)
        """
        if extractors is None:
            extractors = self._create_default_chain()
        self.extractors = extractors
        self.on_fallback = on_fallback

    def _create_default_chain(self) -> list[URLExtractor]:
        """Create default extractor chain."""
        chain: list[URLExtractor] = []

        # 1. Jina.AI Reader (always available - API-based)
        chain.append(JinaExtractor())

        # 2. Trafilatura (if installed)
        try:
            import trafilatura  # noqa: F401

            chain.append(TrafilaturaExtractor())
        except ImportError:
            pass

        # 3. Readability (if installed)
        try:
            from readability import Document  # noqa: F401

            chain.append(ReadabilityExtractor())
        except ImportError:
            pass

        # 4. html2text (if installed)
        try:
            import html2text  # noqa: F401

            chain.append(Html2TextExtractor())
        except ImportError:
            pass

        return chain

    def extract(self, url: str, timeout: int = 30) -> ExtractedContent:
        """Extract content from URL using fallback chain.

        Args:
            url: URL to extract content from
            timeout: Request timeout in seconds

        Returns:
            ExtractedContent from first successful extractor

        Raises:
            NetworkError: If all extractors fail
        """
        last_error: Exception | None = None

        for i, extractor in enumerate(self.extractors):
            try:
                result = extractor.extract(url, timeout=timeout)
                if result is not None:
                    return result

                # Extractor returned None, try next
                if self.on_fallback and i < len(self.extractors) - 1:
                    next_extractor = self.extractors[i + 1]
                    self.on_fallback(url, extractor.name, next_extractor.name)

            except Exception as e:
                last_error = e
                if self.on_fallback and i < len(self.extractors) - 1:
                    next_extractor = self.extractors[i + 1]
                    self.on_fallback(url, extractor.name, next_extractor.name)
                continue

        # All extractors failed
        raise NetworkError(
            f"All extractors failed for {url}. "
            f"Tried: {[e.name for e in self.extractors]}"
        )


def extract_url_content(
    url: str,
    timeout: int = 30,
    extractors: list[URLExtractor] | None = None,
) -> ExtractedContent:
    """Convenience function to extract content from URL.

    Uses default extractor chain with fallback.

    Args:
        url: URL to extract content from
        timeout: Request timeout in seconds
        extractors: Optional custom list of extractors

    Returns:
        ExtractedContent from first successful extractor

    Raises:
        NetworkError: If all extractors fail
    """
    chain = URLExtractorChain(extractors=extractors)
    return chain.extract(url, timeout=timeout)


def extract_url_content_safe(
    url: str,
    timeout: int = 30,
    extractors: list[URLExtractor] | None = None,
) -> ExtractedContent | None:
    """Safe version of extract_url_content that returns None on failure.

    Args:
        url: URL to extract content from
        timeout: Request timeout in seconds
        extractors: Optional custom list of extractors

    Returns:
        ExtractedContent if successful, None on failure
    """
    try:
        return extract_url_content(url, timeout=timeout, extractors=extractors)
    except Exception:
        return None
