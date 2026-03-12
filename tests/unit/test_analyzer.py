from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from importlib import import_module
from typing import Protocol, cast


class _Article(Protocol):
    title: str
    link: str
    summary: str
    published: datetime | None
    source: str
    category: str
    matched_entities: dict[str, list[str]]
    collected_at: datetime | None


class _EntityDefinition(Protocol):
    name: str
    display_name: str
    keywords: list[str]


class _ArticleCtor(Protocol):
    def __call__(
        self,
        *,
        title: str,
        link: str,
        summary: str,
        published: datetime | None,
        source: str,
        category: str,
        matched_entities: dict[str, list[str]] = ...,
        collected_at: datetime | None = ...,
    ) -> _Article: ...


class _EntityCtor(Protocol):
    def __call__(
        self, *, name: str, display_name: str, keywords: list[str]
    ) -> _EntityDefinition: ...


class _ApplyEntityRules(Protocol):
    def __call__(
        self, articles: Iterable[_Article], entities: list[_EntityDefinition]
    ) -> list[_Article]: ...


Article = cast(_ArticleCtor, import_module("radar_core.models").Article)
EntityDefinition = cast(
    _EntityCtor, import_module("radar_core.models").EntityDefinition
)
apply_entity_rules = cast(
    _ApplyEntityRules, import_module("radar_core.analyzer").apply_entity_rules
)


def _make_article(*, title: str, summary: str) -> _Article:
    return Article(
        title=title,
        link=f"https://example.com/{title.lower().replace(' ', '-')}",
        summary=summary,
        published=datetime(2026, 3, 10, 9, 0, tzinfo=timezone.utc),
        source="Example RSS",
        category="tech",
    )


def test_apply_entity_rules_matches_keywords_in_title_and_summary() -> None:
    article = _make_article(
        title="AI adoption accelerates", summary="Cloud migration continues."
    )
    entities = [
        EntityDefinition(name="topic", display_name="Topic", keywords=["ai", "cloud"]),
        EntityDefinition(name="lang", display_name="Language", keywords=["python"]),
    ]

    analyzed = apply_entity_rules([article], entities)

    assert len(analyzed) == 1
    assert analyzed[0].matched_entities == {"topic": ["ai", "cloud"]}


def test_apply_entity_rules_with_empty_entities_returns_articles_without_matches() -> (
    None
):
    article = _make_article(title="No entities", summary="Nothing to match.")

    analyzed = apply_entity_rules([article], [])

    assert len(analyzed) == 1
    assert analyzed[0].matched_entities == {}


def test_apply_entity_rules_with_empty_articles_returns_empty_list() -> None:
    entities = [EntityDefinition(name="topic", display_name="Topic", keywords=["ai"])]

    analyzed = apply_entity_rules([], entities)

    assert analyzed == []


def test_apply_entity_rules_is_case_insensitive() -> None:
    article = _make_article(title="Ai and PYTHON", summary="CLOUD operations")
    entities = [
        EntityDefinition(
            name="topic", display_name="Topic", keywords=["AI", "python", "cloud"]
        )
    ]

    analyzed = apply_entity_rules([article], entities)

    assert analyzed[0].matched_entities == {"topic": ["ai", "python", "cloud"]}


def test_apply_entity_rules_false_positive_ai_in_chair_eliminated() -> None:
    article = _make_article(
        title="Wooden chair trends", summary="Furniture market update"
    )
    entities = [EntityDefinition(name="topic", display_name="Topic", keywords=["ai"])]

    analyzed = apply_entity_rules([article], entities)

    assert analyzed[0].matched_entities == {}


def test_apply_entity_rules_ascii_keyword_ai_true_positives_preserved() -> None:
    entities = [EntityDefinition(name="topic", display_name="Topic", keywords=["AI"])]
    articles = [
        _make_article(title="AI research roundup", summary="Weekly highlights"),
        _make_article(
            title="Computer vision", summary="Teams are using AI for diagnostics"
        ),
        _make_article(title="Model updates", summary="the AI model improved by 10%"),
    ]

    analyzed = apply_entity_rules(articles, entities)

    assert analyzed[0].matched_entities == {"topic": ["ai"]}
    assert analyzed[1].matched_entities == {"topic": ["ai"]}
    assert analyzed[2].matched_entities == {"topic": ["ai"]}


def test_apply_entity_rules_ascii_keyword_ai_false_positives_eliminated() -> None:
    entities = [EntityDefinition(name="topic", display_name="Topic", keywords=["AI"])]
    articles = [
        _make_article(title="CHAIR market trends", summary="furniture"),
        _make_article(title="PAIR programming", summary="engineering practices"),
        _make_article(title="MAIL delivery analytics", summary="logistics"),
    ]

    analyzed = apply_entity_rules(articles, entities)

    assert analyzed[0].matched_entities == {}
    assert analyzed[1].matched_entities == {}
    assert analyzed[2].matched_entities == {}


def test_apply_entity_rules_cjk_keyword_keeps_substring_matching() -> None:
    article = _make_article(title="최신 연구 동향", summary="인공지능 연구 논문 요약")
    entities = [
        EntityDefinition(name="topic", display_name="Topic", keywords=["인공지능"])
    ]

    analyzed = apply_entity_rules([article], entities)

    assert analyzed[0].matched_entities == {"topic": ["인공지능"]}
