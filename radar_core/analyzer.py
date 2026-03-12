from __future__ import annotations

import re
from collections.abc import Iterable
from functools import lru_cache

from .models import Article, EntityDefinition


def _is_ascii_only(keyword: str) -> bool:
    return all(ord(char) < 128 for char in keyword)


@lru_cache(maxsize=2048)
def _compile_ascii_keyword_pattern(keyword: str) -> re.Pattern[str]:
    return re.compile(r"\b" + re.escape(keyword) + r"\b", re.IGNORECASE)


def apply_entity_rules(
    articles: Iterable[Article], entities: list[EntityDefinition]
) -> list[Article]:
    analyzed: list[Article] = []
    normalized_entities: list[
        tuple[EntityDefinition, list[tuple[str, re.Pattern[str] | None]]]
    ] = []
    for entity in entities:
        normalized_keywords: list[tuple[str, re.Pattern[str] | None]] = []
        for keyword in entity.keywords:
            normalized_keyword = keyword.lower()
            if not normalized_keyword:
                continue

            pattern = (
                _compile_ascii_keyword_pattern(normalized_keyword)
                if _is_ascii_only(normalized_keyword)
                else None
            )
            normalized_keywords.append((normalized_keyword, pattern))

        normalized_entities.append((entity, normalized_keywords))

    for article in articles:
        haystack = f"{article.title}\n{article.summary}"
        haystack_lower = haystack.lower()
        matches: dict[str, list[str]] = {}
        for entity, keywords_with_patterns in normalized_entities:
            hit_keywords = [
                keyword
                for keyword, pattern in keywords_with_patterns
                if (
                    pattern.search(haystack)
                    if pattern is not None
                    else keyword in haystack_lower
                )
            ]
            if hit_keywords:
                matches[entity.name] = hit_keywords
        article.matched_entities = matches
        analyzed.append(article)

    return analyzed
