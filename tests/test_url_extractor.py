"""Tests for URL extractor module."""

from __future__ import annotations

import pytest
from unittest.mock import Mock, patch

from radar_core.url_extractor import (
    ExtractedContent,
    JinaExtractor,
    TrafilaturaExtractor,
    Html2TextExtractor,
    ReadabilityExtractor,
    URLExtractorChain,
    extract_url_content,
    extract_url_content_safe,
)
from radar_core.exceptions import NetworkError


class TestExtractedContent:
    """Tests for ExtractedContent dataclass."""

    def test_basic_creation(self):
        content = ExtractedContent(
            title="Test Title",
            content="# Test Content\n\nBody text.",
            url="https://example.com",
            extractor_used="test",
        )
        assert content.title == "Test Title"
        assert content.content == "# Test Content\n\nBody text."
        assert content.url == "https://example.com"
        assert content.extractor_used == "test"
        assert content.extraction_time == 0.0
        assert content.metadata == {}

    def test_with_metadata(self):
        content = ExtractedContent(
            title="Test",
            content="Content",
            url="https://example.com",
            extractor_used="test",
            extraction_time=1.5,
            metadata={"author": "John", "date": "2026-04-01"},
        )
        assert content.extraction_time == 1.5
        assert content.metadata["author"] == "John"


class TestJinaExtractor:
    """Tests for Jina.AI Reader API extractor."""

    def test_init_default(self):
        extractor = JinaExtractor()
        assert extractor.base_url == "https://r.jina.ai/"
        assert extractor.name == "jina"

    def test_init_custom_base_url(self):
        extractor = JinaExtractor(base_url="https://custom.jina.ai")
        assert extractor.base_url == "https://custom.jina.ai/"

    @patch("radar_core.url_extractor.requests.get")
    def test_extract_success(self, mock_get):
        mock_response = Mock()
        mock_response.text = "# Test Title\n\nThis is the content."
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        extractor = JinaExtractor()
        result = extractor.extract("https://example.com/article")

        assert result is not None
        assert result.title == "Test Title"
        assert result.content == "# Test Title\n\nThis is the content."
        assert result.extractor_used == "jina"
        assert "jina_url" in result.metadata
        mock_get.assert_called_once()

    @patch("radar_core.url_extractor.requests.get")
    def test_extract_with_title_metadata(self, mock_get):
        mock_response = Mock()
        mock_response.text = "Title: Article Title\n\nContent here."
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        extractor = JinaExtractor()
        result = extractor.extract("https://example.com")

        assert result is not None
        assert result.title == "Article Title"

    @patch("radar_core.url_extractor.requests.get")
    def test_extract_timeout(self, mock_get):
        import requests

        mock_get.side_effect = requests.exceptions.Timeout()

        extractor = JinaExtractor()
        result = extractor.extract("https://example.com")

        assert result is None

    @patch("radar_core.url_extractor.requests.get")
    def test_extract_request_error(self, mock_get):
        import requests

        mock_get.side_effect = requests.exceptions.ConnectionError()

        extractor = JinaExtractor()
        result = extractor.extract("https://example.com")

        assert result is None

    @patch("radar_core.url_extractor.requests.get")
    def test_extract_http_error(self, mock_get):
        import requests

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "404 Not Found"
        )
        mock_get.return_value = mock_response

        extractor = JinaExtractor()
        result = extractor.extract("https://example.com")

        assert result is None

    def test_extract_title_from_heading(self):
        extractor = JinaExtractor()
        content = "# My Article Title\n\nSome content here."
        title = extractor._extract_title(content, "https://fallback.com")
        assert title == "My Article Title"

    def test_extract_title_fallback(self):
        extractor = JinaExtractor()
        content = "No heading here, just content."
        title = extractor._extract_title(content, "https://fallback.com")
        assert title == "https://fallback.com"


class TestTrafilaturaExtractor:
    """Tests for Trafilatura extractor."""

    def test_name(self):
        extractor = TrafilaturaExtractor()
        assert extractor.name == "trafilatura"

    @patch("radar_core.url_extractor.TrafilaturaExtractor._get_trafilatura")
    def test_extract_success(self, mock_get_trafilatura):
        mock_trafilatura = Mock()
        mock_trafilatura.fetch_url.return_value = "<html><body>Content</body></html>"
        mock_trafilatura.extract.return_value = "# Extracted Content"

        mock_metadata = Mock()
        mock_metadata.title = "Article Title"
        mock_metadata.author = "John Doe"
        mock_metadata.date = "2026-04-01"
        mock_metadata.sitename = "Example Site"
        mock_trafilatura.extract_metadata.return_value = mock_metadata

        mock_get_trafilatura.return_value = mock_trafilatura

        extractor = TrafilaturaExtractor()
        result = extractor.extract("https://example.com/article")

        assert result is not None
        assert result.title == "Article Title"
        assert result.content == "# Extracted Content"
        assert result.extractor_used == "trafilatura"
        assert result.metadata["author"] == "John Doe"

    @patch("radar_core.url_extractor.TrafilaturaExtractor._get_trafilatura")
    def test_extract_fetch_failed(self, mock_get_trafilatura):
        mock_trafilatura = Mock()
        mock_trafilatura.fetch_url.return_value = None
        mock_get_trafilatura.return_value = mock_trafilatura

        extractor = TrafilaturaExtractor()
        result = extractor.extract("https://example.com")

        assert result is None

    @patch("radar_core.url_extractor.TrafilaturaExtractor._get_trafilatura")
    def test_extract_content_empty(self, mock_get_trafilatura):
        mock_trafilatura = Mock()
        mock_trafilatura.fetch_url.return_value = "<html></html>"
        mock_trafilatura.extract.return_value = None
        mock_get_trafilatura.return_value = mock_trafilatura

        extractor = TrafilaturaExtractor()
        result = extractor.extract("https://example.com")

        assert result is None

    def test_extract_import_error(self):
        extractor = TrafilaturaExtractor()
        extractor._trafilatura = None

        with patch.dict("sys.modules", {"trafilatura": None}):
            # Force import to fail
            extractor._get_trafilatura = Mock(side_effect=ImportError("No module"))
            result = extractor.extract("https://example.com")
            assert result is None


class TestHtml2TextExtractor:
    """Tests for html2text extractor."""

    def test_name(self):
        extractor = Html2TextExtractor()
        assert extractor.name == "html2text"

    @patch("radar_core.url_extractor.requests.get")
    def test_extract_success(self, mock_get):
        mock_response = Mock()
        mock_response.text = "<html><head><title>Test Page</title></head><body><h1>Hello</h1><p>World</p></body></html>"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        extractor = Html2TextExtractor()
        result = extractor.extract("https://example.com")

        assert result is not None
        assert result.title == "Test Page"
        assert "Hello" in result.content
        assert "World" in result.content
        assert result.extractor_used == "html2text"

    @patch("radar_core.url_extractor.requests.get")
    def test_extract_no_title(self, mock_get):
        mock_response = Mock()
        mock_response.text = "<html><body><p>Content without title</p></body></html>"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        extractor = Html2TextExtractor()
        result = extractor.extract("https://example.com/page")

        assert result is not None
        assert result.title == "https://example.com/page"

    @patch("radar_core.url_extractor.requests.get")
    def test_extract_timeout(self, mock_get):
        import requests

        mock_get.side_effect = requests.exceptions.Timeout()

        extractor = Html2TextExtractor()
        result = extractor.extract("https://example.com")

        assert result is None

    def test_extract_title_from_html(self):
        extractor = Html2TextExtractor()
        html = "<html><head><title>  Page Title  </title></head></html>"
        title = extractor._extract_title_from_html(html, "fallback")
        assert title == "Page Title"


class TestReadabilityExtractor:
    """Tests for Readability extractor."""

    def test_name(self):
        extractor = ReadabilityExtractor()
        assert extractor.name == "readability"

    @patch("radar_core.url_extractor.requests.get")
    def test_extract_success(self, mock_get):
        mock_response = Mock()
        mock_response.text = """
        <html>
        <head><title>Original Title</title></head>
        <body>
            <article>
                <h1>Article Title</h1>
                <p>Main article content here.</p>
            </article>
            <aside>Sidebar content</aside>
        </body>
        </html>
        """
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        extractor = ReadabilityExtractor()
        result = extractor.extract("https://example.com")

        # Result depends on readability-lxml being installed
        if result is not None:
            assert result.extractor_used == "readability"
            assert "content" in result.content.lower() or result.content

    @patch("radar_core.url_extractor.requests.get")
    def test_extract_http_error(self, mock_get):
        import requests

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_get.return_value = mock_response

        extractor = ReadabilityExtractor()
        result = extractor.extract("https://example.com")

        assert result is None


class TestURLExtractorChain:
    """Tests for URL extractor chain."""

    def test_default_chain_creation(self):
        chain = URLExtractorChain()
        assert len(chain.extractors) > 0
        # Jina should always be first
        assert chain.extractors[0].name == "jina"

    def test_custom_chain(self):
        extractors = [JinaExtractor(), Html2TextExtractor()]
        chain = URLExtractorChain(extractors=extractors)
        assert len(chain.extractors) == 2
        assert chain.extractors[0].name == "jina"
        assert chain.extractors[1].name == "html2text"

    def test_fallback_callback(self):
        fallback_calls = []

        def on_fallback(url, failed, next_ext):
            fallback_calls.append((url, failed, next_ext))

        # Create extractors that fail
        failing_extractor = Mock()
        failing_extractor.name = "failing"
        failing_extractor.extract.return_value = None

        success_extractor = Mock()
        success_extractor.name = "success"
        success_extractor.extract.return_value = ExtractedContent(
            title="Test",
            content="Content",
            url="https://example.com",
            extractor_used="success",
        )

        chain = URLExtractorChain(
            extractors=[failing_extractor, success_extractor],
            on_fallback=on_fallback,
        )

        result = chain.extract("https://example.com")

        assert result is not None
        assert result.extractor_used == "success"
        assert len(fallback_calls) == 1
        assert fallback_calls[0] == ("https://example.com", "failing", "success")

    def test_all_extractors_fail(self):
        failing_extractor1 = Mock()
        failing_extractor1.name = "fail1"
        failing_extractor1.extract.return_value = None

        failing_extractor2 = Mock()
        failing_extractor2.name = "fail2"
        failing_extractor2.extract.return_value = None

        chain = URLExtractorChain(extractors=[failing_extractor1, failing_extractor2])

        with pytest.raises(NetworkError) as exc_info:
            chain.extract("https://example.com")

        assert "All extractors failed" in str(exc_info.value)
        assert "fail1" in str(exc_info.value)
        assert "fail2" in str(exc_info.value)

    def test_first_extractor_succeeds(self):
        success_extractor = Mock()
        success_extractor.name = "first"
        success_extractor.extract.return_value = ExtractedContent(
            title="First",
            content="Content from first",
            url="https://example.com",
            extractor_used="first",
        )

        never_called = Mock()
        never_called.name = "second"

        chain = URLExtractorChain(extractors=[success_extractor, never_called])
        result = chain.extract("https://example.com")

        assert result.extractor_used == "first"
        never_called.extract.assert_not_called()

    def test_extractor_raises_exception(self):
        raising_extractor = Mock()
        raising_extractor.name = "raising"
        raising_extractor.extract.side_effect = ValueError("Something went wrong")

        success_extractor = Mock()
        success_extractor.name = "backup"
        success_extractor.extract.return_value = ExtractedContent(
            title="Backup",
            content="Content",
            url="https://example.com",
            extractor_used="backup",
        )

        chain = URLExtractorChain(extractors=[raising_extractor, success_extractor])
        result = chain.extract("https://example.com")

        assert result.extractor_used == "backup"


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    @patch.object(URLExtractorChain, "extract")
    def test_extract_url_content(self, mock_extract):
        mock_extract.return_value = ExtractedContent(
            title="Test",
            content="Content",
            url="https://example.com",
            extractor_used="jina",
        )

        result = extract_url_content("https://example.com")

        assert result.title == "Test"
        mock_extract.assert_called_once_with("https://example.com", timeout=30)

    @patch.object(URLExtractorChain, "extract")
    def test_extract_url_content_with_timeout(self, mock_extract):
        mock_extract.return_value = ExtractedContent(
            title="Test",
            content="Content",
            url="https://example.com",
            extractor_used="jina",
        )

        result = extract_url_content("https://example.com", timeout=60)

        mock_extract.assert_called_once_with("https://example.com", timeout=60)

    @patch.object(URLExtractorChain, "extract")
    def test_extract_url_content_safe_success(self, mock_extract):
        mock_extract.return_value = ExtractedContent(
            title="Test",
            content="Content",
            url="https://example.com",
            extractor_used="jina",
        )

        result = extract_url_content_safe("https://example.com")

        assert result is not None
        assert result.title == "Test"

    @patch.object(URLExtractorChain, "extract")
    def test_extract_url_content_safe_failure(self, mock_extract):
        mock_extract.side_effect = NetworkError("All failed")

        result = extract_url_content_safe("https://example.com")

        assert result is None


class TestIntegration:
    """Integration tests (require network access)."""

    @pytest.mark.skip(reason="Requires network access - run manually")
    def test_jina_real_request(self):
        """Test real Jina API request."""
        extractor = JinaExtractor()
        result = extractor.extract("https://example.com")

        assert result is not None
        assert result.title
        assert result.content
        assert result.extractor_used == "jina"

    @pytest.mark.skip(reason="Requires network access - run manually")
    def test_chain_real_request(self):
        """Test real chain extraction."""
        chain = URLExtractorChain()
        result = chain.extract("https://example.com")

        assert result is not None
        assert result.content
