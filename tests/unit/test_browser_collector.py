from __future__ import annotations

from typing import Any

from radar_core.browser_collector import (
    _extract_articles_from_links,
    _goto_with_retries,
    _source_positive_int,
    _wait_for_selector_with_fallback,
)


class FakeExtractionPage:
    def __init__(self, links: list[dict[str, str]]):
        self._links = links

    def eval_on_selector_all(self, selector: str, script: str) -> list[dict[str, str]]:
        _ = script
        assert selector == "a"
        return self._links


class FakeDetailContext:
    def __init__(self, page: "FakeDetailPage"):
        self.page = page

    def new_page(self) -> "FakeDetailPage":
        return self.page


class FakeDetailExtractionPage(FakeExtractionPage):
    def __init__(self, links: list[dict[str, str]], detail_page: "FakeDetailPage"):
        super().__init__(links)
        self.context = FakeDetailContext(detail_page)


class FakeDetailPage:
    def __init__(self, body: str):
        self.body = body
        self.closed = False

    def goto(self, url: str, wait_until: str, timeout: int) -> str:
        assert url == "https://example.com/board/detail?id=1"
        assert wait_until == "domcontentloaded"
        assert timeout == 8_000
        return "ok"

    def wait_for_selector(self, selector: str, timeout: int, **kwargs: Any) -> None:
        _ = timeout
        _ = kwargs
        assert selector == "article"

    @property
    def first(self) -> "FakeDetailPage":
        return self

    def locator(self, selector: str) -> "FakeDetailPage":
        assert selector == "article"
        return self

    def content(self) -> str:
        return f"<article>{self.body}</article>"

    def evaluate(self, script: str) -> str:
        _ = script
        return "utf-8"

    def inner_text(self, selector: str, timeout: int) -> str:
        _ = timeout
        assert selector in {"article", "body"}
        return self.body

    def close(self) -> None:
        self.closed = True


def _extract(links: list[dict[str, str]], config: dict[str, Any] | None = None):
    return _extract_articles_from_links(
        extraction_page=FakeExtractionPage(links),
        source_name="source",
        category="category",
        fallback_title="fallback",
        fallback_summary="summary",
        fallback_link="https://example.com/board/list",
        link_selector="a",
        config=config or {},
    )


def test_extract_articles_resolves_relative_links() -> None:
    articles = _extract([{"href": "/board/detail?id=1", "text": "Detail"}])

    assert articles[0].link == "https://example.com/board/detail?id=1"
    assert articles[0].title == "Detail"
    assert articles[0].summary == "Detail"


def test_extract_articles_uses_javascript_link_templates() -> None:
    articles = _extract(
        [{"href": "javascript:fnTbbsView('456137');", "text": "Notice"}],
        {
            "javascript_link_templates": {
                "fnTbbsView": "https://www.seoul.go.kr/news/news_notice.do?bbsNo=277&nttNo={id}",
            }
        },
    )

    assert (
        articles[0].link
        == "https://www.seoul.go.kr/news/news_notice.do?bbsNo=277&nttNo=456137"
    )


def test_extract_articles_uses_onclick_template_for_placeholder_href() -> None:
    articles = _extract(
        [
            {
                "href": "#view",
                "onclick": "doBbsFView('86','1067216','16010100','1067216');return false;",
                "text": "Notice",
            }
        ],
        {
            "javascript_link_templates": {
                "doBbsFView": (
                    "https://www.mss.go.kr/site/smba/ex/bbs/View.do"
                    "?cbIdx={0}&bcIdx={1}&parentSeq={3}"
                ),
            }
        },
    )

    assert (
        articles[0].link
        == "https://www.mss.go.kr/site/smba/ex/bbs/View.do?cbIdx=86&bcIdx=1067216&parentSeq=1067216"
    )


def test_extract_articles_skips_unmapped_javascript_links() -> None:
    articles = _extract(
        [{"href": "javascript:unknown('456137');", "text": "Notice"}],
        {
            "javascript_link_templates": {
                "fnTbbsView": "https://www.seoul.go.kr/news/news_notice.do?nttNo={id}",
            }
        },
    )

    assert articles[0].link == "https://example.com/board/list"
    assert articles[0].title == "fallback"


def test_extract_articles_can_enrich_from_detail_page() -> None:
    detail_page = FakeDetailPage("신청기간: 2026-04-01 ~ 2026-04-30")
    articles = _extract_articles_from_links(
        extraction_page=FakeDetailExtractionPage(
            [{"href": "/board/detail?id=1", "text": "Notice"}],
            detail_page,
        ),
        source_name="source",
        category="category",
        fallback_title="fallback",
        fallback_summary="summary",
        fallback_link="https://example.com/board/list",
        link_selector="a",
        config={
            "fetch_detail": True,
            "detail_wait_for": "article",
            "detail_content_selector": "article",
        },
    )

    assert articles[0].title == "Notice"
    assert articles[0].summary == "신청기간: 2026-04-01 ~ 2026-04-30"
    assert detail_page.closed is True


class FakeWaitPage:
    def __init__(self, failing_selector: str):
        self.failing_selector = failing_selector
        self.calls: list[str] = []

    def wait_for_selector(self, selector: str, timeout: int, **kwargs: Any) -> None:
        _ = timeout
        _ = kwargs
        self.calls.append(selector)
        if selector == self.failing_selector:
            raise TimeoutError(selector)


def test_wait_for_selector_uses_configured_fallback() -> None:
    page = FakeWaitPage(".event_list")

    _wait_for_selector_with_fallback(page, ".event_list", "body", 20_000)

    assert page.calls == [".event_list", "body"]


def test_wait_for_selector_reraises_without_fallback() -> None:
    page = FakeWaitPage(".event_list")

    try:
        _wait_for_selector_with_fallback(page, ".event_list", None, 20_000)
    except TimeoutError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected TimeoutError")


class FakeGotoPage:
    def __init__(self, failures: int):
        self.failures = failures
        self.calls = 0

    def goto(self, url: str, wait_until: str, timeout: int) -> str:
        assert url == "https://example.com"
        assert wait_until == "domcontentloaded"
        assert timeout == 20_000
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError("reset")
        return "ok"


def test_goto_with_retries_recovers_from_transient_failure() -> None:
    page = FakeGotoPage(failures=1)

    result = _goto_with_retries(
        page,
        "https://example.com",
        timeout_ms=20_000,
        retries=2,
        retry_delay_ms=1_000,
    )

    assert result == "ok"
    assert page.calls == 2


def test_source_positive_int_clamps_count_values() -> None:
    assert _source_positive_int({"navigation_retries": "8"}, "navigation_retries", 1, max_value=5) == 5
    assert _source_positive_int({"navigation_retries": "0"}, "navigation_retries", 1) == 1
