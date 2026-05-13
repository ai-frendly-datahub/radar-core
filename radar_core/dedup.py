"""Title-level near-duplicate detection.

Implements a lightweight token-set cosine similarity over normalized titles
so the same news event collected by N different sources can be clustered
together. Designed to run on a few thousand titles per radar / day, so we
prefer simple Python sets over heavyweight NLP.

Public API:
- ``normalize_title(title)`` -> lower-cased token signature.
- ``title_similarity(a, b)`` -> float in [0, 1].
- ``cluster_titles(titles, threshold=0.85)`` -> list[int] cluster ids in
  the same order as the input titles. Items below the threshold get their
  own cluster id.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Sequence
from math import sqrt
from typing import Optional


_TOKEN_RE = re.compile(r"[\w가-힣]+", re.UNICODE)
# Common English / Korean stopwords. Conservative — we keep it small so we
# don't accidentally make every "AI" / "보안" headline collapse.
_STOPWORDS: frozenset[str] = frozenset(
    {
        "the",
        "a",
        "an",
        "of",
        "and",
        "or",
        "for",
        "to",
        "in",
        "on",
        "at",
        "by",
        "with",
        "as",
        "is",
        "are",
        "was",
        "were",
        "this",
        "that",
        "기자",
        "오늘",
        "내일",
        "어제",
        "이번",
        "지난",
        "다음",
        "최근",
        "현재",
        "이후",
        "이전",
    }
)


def normalize_title(title: str | None) -> list[str]:
    """Return a list of normalized token strings.

    - NFKC normalize, lower-case
    - Remove punctuation
    - Drop stopwords + 1-char tokens
    """
    if not title:
        return []
    text = unicodedata.normalize("NFKC", title).lower()
    tokens = _TOKEN_RE.findall(text)
    return [t for t in tokens if len(t) > 1 and t not in _STOPWORDS]


def _cosine(tokens_a: Sequence[str], tokens_b: Sequence[str]) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    counts_a: dict[str, int] = {}
    counts_b: dict[str, int] = {}
    for tok in tokens_a:
        counts_a[tok] = counts_a.get(tok, 0) + 1
    for tok in tokens_b:
        counts_b[tok] = counts_b.get(tok, 0) + 1
    dot = sum(counts_a[k] * counts_b[k] for k in counts_a if k in counts_b)
    norm_a = sqrt(sum(v * v for v in counts_a.values()))
    norm_b = sqrt(sum(v * v for v in counts_b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def title_similarity(a: str | None, b: str | None) -> float:
    """Return cosine similarity over normalized title token sets."""
    return _cosine(normalize_title(a), normalize_title(b))


def cluster_titles(
    titles: Iterable[str | None],
    *,
    threshold: float = 0.85,
) -> list[int]:
    """Greedy single-link clustering. Returns a cluster id per input title."""
    titles_list = list(titles)
    n = len(titles_list)
    tokens = [normalize_title(t) for t in titles_list]
    cluster_ids: list[Optional[int]] = [None] * n
    next_id = 0
    for i in range(n):
        if cluster_ids[i] is not None:
            continue
        cluster_ids[i] = next_id
        for j in range(i + 1, n):
            if cluster_ids[j] is not None:
                continue
            if _cosine(tokens[i], tokens[j]) >= threshold:
                cluster_ids[j] = next_id
        next_id += 1
    # mypy: cluster_ids is fully populated above.
    return [cid if cid is not None else -1 for cid in cluster_ids]


__all__ = ["normalize_title", "title_similarity", "cluster_titles"]
