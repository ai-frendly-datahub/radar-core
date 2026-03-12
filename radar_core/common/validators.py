from __future__ import annotations

import re
from difflib import SequenceMatcher
from urllib.parse import urlparse

from ..models import Article


def normalize_title(title: str) -> str:
    if not title:
        return ""

    normalized = title.lower()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = re.sub(r"[^\w\s\-]", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    return normalized


def validate_url_format(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False

    try:
        parsed = urlparse(url)
        return bool(parsed.scheme and parsed.netloc)
    except Exception:
        return False


def is_similar_url(url1: str, url2: str, threshold: float = 0.8) -> bool:
    try:
        parsed1 = urlparse(url1)
        parsed2 = urlparse(url2)

        if parsed1.netloc != parsed2.netloc:
            return False

        path1 = parsed1.path
        path2 = parsed2.path

        if path1 == path2:
            return True

        ratio = SequenceMatcher(None, path1, path2).ratio()
        return ratio >= threshold

    except Exception:
        return False


def detect_duplicate_articles(
    title1: str,
    url1: str,
    title2: str,
    url2: str,
    title_threshold: float = 0.85,
    url_threshold: float = 0.8,
) -> bool:
    norm_title1 = normalize_title(title1)
    norm_title2 = normalize_title(title2)

    title_ratio = SequenceMatcher(None, norm_title1, norm_title2).ratio()
    if title_ratio < title_threshold:
        return False

    return is_similar_url(url1, url2, url_threshold)


def validate_article(article: Article) -> tuple[bool, list[str]]:
    errors: list[str] = []

    if not article.title or not isinstance(article.title, str):
        errors.append("title is missing or not a string")
    elif len(article.title.strip()) == 0:
        errors.append("title is empty")

    if not article.link or not isinstance(article.link, str):
        errors.append("link is missing or not a string")
    elif not validate_url_format(article.link):
        errors.append(f"link has invalid URL format: {article.link}")

    if not article.summary or not isinstance(article.summary, str):
        errors.append("summary is missing or not a string")
    elif len(article.summary.strip()) == 0:
        errors.append("summary is empty")

    if not article.source or not isinstance(article.source, str):
        errors.append("source is missing or not a string")

    if not article.category or not isinstance(article.category, str):
        errors.append("category is missing or not a string")

    return len(errors) == 0, errors
