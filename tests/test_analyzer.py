"""Tests for apply_entity_rules — keyword matching, edge cases."""

from __future__ import annotations

from datetime import UTC, datetime

from radar_core.analyzer import apply_entity_rules
from radar_core.models import Article, EntityDefinition


# ── Helpers ───────────────────────────────────────────────────────────────


def _art(title: str = "Default", summary: str = "") -> Article:
    return Article(
        title=title,
        link=f"https://example.com/{title.lower().replace(' ', '-')}",
        summary=summary,
        published=datetime(2026, 3, 10, 9, 0, tzinfo=UTC),
        source="TestSource",
        category="test",
    )


def _ent(name: str, keywords: list[str]) -> EntityDefinition:
    return EntityDefinition(name=name, display_name=name, keywords=keywords)


# ── Tests ─────────────────────────────────────────────────────────────────


def test_exact_keyword_match() -> None:
    """정확한 키워드 매칭."""
    article = _art(title="Python release notes", summary="New features.")
    entities = [_ent("lang", ["python"])]

    result = apply_entity_rules([article], entities)

    assert result[0].matched_entities == {"lang": ["python"]}


def test_case_insensitive_match() -> None:
    """대소문자 구분 없이 매칭."""
    article = _art(title="PYTHON is Great", summary="python everywhere")
    entities = [_ent("lang", ["Python"])]

    result = apply_entity_rules([article], entities)

    assert "lang" in result[0].matched_entities


def test_no_match_returns_empty() -> None:
    """매칭 없으면 빈 dict."""
    article = _art(title="Cooking recipes", summary="Pasta and salad")
    entities = [_ent("tech", ["python", "rust", "go"])]

    result = apply_entity_rules([article], entities)

    assert result[0].matched_entities == {}


def test_multiple_keywords_same_entity() -> None:
    """한 엔티티의 여러 키워드가 동시에 매칭."""
    article = _art(title="AI and ML trends", summary="Deep learning advances")
    entities = [_ent("topic", ["ai", "ml", "deep learning"])]

    result = apply_entity_rules([article], entities)

    matched = result[0].matched_entities["topic"]
    assert "ai" in matched
    assert "ml" in matched
    assert "deep learning" in matched


def test_multiple_entities() -> None:
    """여러 엔티티가 독립적으로 매칭."""
    article = _art(title="Python AI framework", summary="Cloud deployment")
    entities = [
        _ent("lang", ["python"]),
        _ent("topic", ["ai"]),
        _ent("infra", ["cloud"]),
    ]

    result = apply_entity_rules([article], entities)

    assert "lang" in result[0].matched_entities
    assert "topic" in result[0].matched_entities
    assert "infra" in result[0].matched_entities


def test_korean_keyword_match() -> None:
    """한글 키워드 매칭 (서브스트링 방식)."""
    article = _art(title="인공지능 기술 동향", summary="반도체 시장 전망")
    entities = [
        _ent("tech", ["인공지능"]),
        _ent("industry", ["반도체"]),
    ]

    result = apply_entity_rules([article], entities)

    assert "tech" in result[0].matched_entities
    assert "industry" in result[0].matched_entities


def test_partial_word_no_match_ascii() -> None:
    """ASCII 키워드는 단어 경계(word boundary) 매칭 — 부분 매칭 차단."""
    article = _art(title="CHAIR design trends", summary="Repair service")
    entities = [_ent("topic", ["ai"])]

    result = apply_entity_rules([article], entities)

    assert result[0].matched_entities == {}


def test_empty_articles_list() -> None:
    """빈 기사 목록 → 빈 결과."""
    entities = [_ent("topic", ["ai"])]

    result = apply_entity_rules([], entities)

    assert result == []


def test_empty_entities_list() -> None:
    """빈 엔티티 목록 → 모든 기사에 빈 matched_entities."""
    articles = [_art(title="Something"), _art(title="Another")]

    result = apply_entity_rules(articles, [])

    assert len(result) == 2
    assert result[0].matched_entities == {}
    assert result[1].matched_entities == {}


def test_matched_entities_populated() -> None:
    """matched_entities 필드가 올바른 구조(dict[str, list[str]])로 채워짐."""
    article = _art(title="Nintendo Switch 2", summary="PS5 competitor")
    entities = [
        _ent("Nintendo", ["nintendo", "switch"]),
        _ent("Sony", ["ps5"]),
    ]

    result = apply_entity_rules([article], entities)

    me = result[0].matched_entities
    assert isinstance(me, dict)
    assert all(isinstance(v, list) for v in me.values())
    assert "Nintendo" in me
    assert "Sony" in me


def test_keyword_in_summary_only() -> None:
    """요약에만 키워드가 있어도 매칭."""
    article = _art(title="Market update", summary="Rust language gains popularity")
    entities = [_ent("lang", ["rust"])]

    result = apply_entity_rules([article], entities)

    assert "lang" in result[0].matched_entities


def test_keyword_in_title_only() -> None:
    """제목에만 키워드가 있어도 매칭."""
    article = _art(title="Go is fast", summary="Benchmark results")
    entities = [_ent("lang", ["go"])]

    result = apply_entity_rules([article], entities)

    assert "lang" in result[0].matched_entities


def test_empty_keyword_is_skipped() -> None:
    """빈 문자열 키워드는 무시."""
    article = _art(title="Python tips", summary="Clean code")
    entities = [_ent("lang", ["", "python", ""])]

    result = apply_entity_rules([article], entities)

    assert result[0].matched_entities == {"lang": ["python"]}


def test_multiple_articles_independent_matching() -> None:
    """여러 기사가 각각 독립적으로 매칭."""
    articles = [
        _art(title="AI news", summary=""),
        _art(title="Cooking tips", summary=""),
        _art(title="Cloud migration", summary=""),
    ]
    entities = [_ent("tech", ["ai", "cloud"])]

    result = apply_entity_rules(articles, entities)

    assert result[0].matched_entities == {"tech": ["ai"]}
    assert result[1].matched_entities == {}
    assert result[2].matched_entities == {"tech": ["cloud"]}
