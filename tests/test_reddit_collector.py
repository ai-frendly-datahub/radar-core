"""Tests for Reddit JSON API collector."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from radar_core.models import Source
from radar_core.reddit_collector import (
    _build_reddit_json_url,
    _extract_reddit_text,
    _normalize_reddit_url,
    _parse_reddit_response,
    _parse_reddit_timestamp,
    collect_reddit_source,
    collect_reddit_sources,
)


class TestNormalizeRedditUrl:
    def test_basic_subreddit_url(self) -> None:
        url = "https://www.reddit.com/r/RealEstate/"
        assert _normalize_reddit_url(url) == "https://www.reddit.com/r/RealEstate/"

    def test_rss_suffix_removed(self) -> None:
        url = "https://www.reddit.com/r/RealEstate/.rss"
        assert _normalize_reddit_url(url) == "https://www.reddit.com/r/RealEstate/"

    def test_adds_www_prefix(self) -> None:
        url = "https://reddit.com/r/RealEstate"
        assert _normalize_reddit_url(url) == "https://www.reddit.com/r/RealEstate/"

    def test_adds_trailing_slash(self) -> None:
        url = "https://www.reddit.com/r/RealEstate"
        assert _normalize_reddit_url(url) == "https://www.reddit.com/r/RealEstate/"


class TestBuildRedditJsonUrl:
    def test_default_params(self) -> None:
        url = _build_reddit_json_url("https://www.reddit.com/r/test/")
        assert "new.json" in url
        assert "limit=25" in url
        assert "raw_json=1" in url

    def test_custom_sort(self) -> None:
        url = _build_reddit_json_url("https://www.reddit.com/r/test/", sort="hot")
        assert "hot.json" in url

    def test_custom_limit(self) -> None:
        url = _build_reddit_json_url("https://www.reddit.com/r/test/", limit=50)
        assert "limit=50" in url

    def test_limit_capped_at_100(self) -> None:
        url = _build_reddit_json_url("https://www.reddit.com/r/test/", limit=200)
        assert "limit=100" in url


class TestParseRedditTimestamp:
    def test_valid_timestamp(self) -> None:
        timestamp = 1700000000.0
        result = _parse_reddit_timestamp(timestamp)
        assert result is not None
        assert result.tzinfo == UTC

    def test_none_timestamp(self) -> None:
        assert _parse_reddit_timestamp(None) is None

    def test_integer_timestamp(self) -> None:
        result = _parse_reddit_timestamp(1700000000)
        assert result is not None


class TestExtractRedditText:
    def test_selftext_post(self) -> None:
        data = {"selftext": "This is the post content"}
        assert _extract_reddit_text(data) == "This is the post content"

    def test_removed_post(self) -> None:
        data = {"selftext": "[removed]"}
        assert _extract_reddit_text(data) == ""

    def test_deleted_post(self) -> None:
        data = {"selftext": "[deleted]"}
        assert _extract_reddit_text(data) == ""

    def test_link_post(self) -> None:
        data = {"selftext": "", "url": "https://example.com/article"}
        assert _extract_reddit_text(data) == "Link: https://example.com/article"

    def test_reddit_link_excluded(self) -> None:
        data = {"selftext": "", "url": "https://www.reddit.com/r/test/comments/123/"}
        assert _extract_reddit_text(data) == ""

    def test_long_text_truncated(self) -> None:
        data = {"selftext": "x" * 3000}
        result = _extract_reddit_text(data)
        assert len(result) == 2003  # 2000 + "..."


class TestParseRedditResponse:
    @pytest.fixture
    def sample_response(self) -> dict:
        return {
            "kind": "Listing",
            "data": {
                "children": [
                    {
                        "kind": "t3",
                        "data": {
                            "title": "Test Post",
                            "selftext": "Test content",
                            "permalink": "/r/test/comments/abc123/test_post/",
                            "created_utc": 1700000000.0,
                            "author": "testuser",
                            "score": 100,
                            "num_comments": 50,
                            "subreddit": "test",
                        },
                    }
                ]
            },
        }

    def test_parses_valid_response(self, sample_response: dict) -> None:
        articles = _parse_reddit_response(sample_response, "r/test", "property")
        assert len(articles) == 1
        assert articles[0].title == "Test Post"
        assert articles[0].source == "r/test"
        assert articles[0].category == "property"

    def test_skips_removed_posts(self, sample_response: dict) -> None:
        sample_response["data"]["children"][0]["data"]["removed_by_category"] = "spam"
        articles = _parse_reddit_response(sample_response, "r/test", "property")
        assert len(articles) == 0

    def test_skips_deleted_titles(self, sample_response: dict) -> None:
        sample_response["data"]["children"][0]["data"]["title"] = "[deleted]"
        articles = _parse_reddit_response(sample_response, "r/test", "property")
        assert len(articles) == 0

    def test_invalid_kind_returns_empty(self) -> None:
        response = {"kind": "NotListing", "data": {}}
        articles = _parse_reddit_response(response, "r/test", "property")
        assert len(articles) == 0

    def test_metadata_in_summary(self, sample_response: dict) -> None:
        articles = _parse_reddit_response(sample_response, "r/test", "property")
        assert "[r/test]" in articles[0].summary
        assert "u/testuser" in articles[0].summary
        assert "100 points" in articles[0].summary


class TestCollectRedditSource:
    @pytest.fixture
    def mock_source(self) -> Source:
        return Source(
            name="r/RealEstate",
            type="reddit",
            url="https://www.reddit.com/r/RealEstate/",
        )

    @patch("radar_core.reddit_collector._reddit_rate_limit")
    @patch("radar_core.reddit_collector._create_reddit_session")
    def test_successful_collection(
        self,
        mock_session_factory: MagicMock,
        mock_rate_limit: MagicMock,
        mock_source: Source,
    ) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "kind": "Listing",
            "data": {
                "children": [
                    {
                        "kind": "t3",
                        "data": {
                            "title": "Test Post",
                            "selftext": "Content",
                            "permalink": "/r/RealEstate/comments/123/",
                            "created_utc": 1700000000.0,
                            "author": "user",
                            "score": 10,
                            "num_comments": 5,
                            "subreddit": "RealEstate",
                        },
                    }
                ]
            },
        }

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_factory.return_value = mock_session

        articles = collect_reddit_source(mock_source, category="property")
        assert len(articles) == 1
        assert articles[0].title == "Test Post"

    @patch("radar_core.reddit_collector._reddit_rate_limit")
    @patch("radar_core.reddit_collector._create_reddit_session")
    def test_rate_limit_error(
        self,
        mock_session_factory: MagicMock,
        mock_rate_limit: MagicMock,
        mock_source: Source,
    ) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "60"}

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_factory.return_value = mock_session

        from radar_core.exceptions import SourceError

        with pytest.raises(SourceError, match="rate limit"):
            collect_reddit_source(mock_source, category="property")


class TestCollectRedditSources:
    @patch("radar_core.reddit_collector.collect_reddit_source")
    @patch("radar_core.reddit_collector._create_reddit_session")
    def test_collects_multiple_sources(
        self, mock_session_factory: MagicMock, mock_collect: MagicMock
    ) -> None:
        from radar_core.models import Article

        mock_collect.return_value = [
            Article(
                title="Test",
                link="https://reddit.com/test",
                summary="Test summary",
                published=datetime.now(UTC),
                source="r/test",
                category="property",
            )
        ]

        sources = [
            Source(
                name="r/test1", type="reddit", url="https://www.reddit.com/r/test1/"
            ),
            Source(
                name="r/test2", type="reddit", url="https://www.reddit.com/r/test2/"
            ),
        ]

        articles, errors = collect_reddit_sources(sources, category="property")
        assert len(articles) == 2
        assert len(errors) == 0
