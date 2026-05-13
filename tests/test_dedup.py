from __future__ import annotations

import pytest

from radar_core.dedup import cluster_titles, normalize_title, title_similarity


def test_normalize_title_drops_punctuation_and_stopwords() -> None:
    assert normalize_title("The Quick Brown Fox") == ["quick", "brown", "fox"]
    assert normalize_title("AI 보안 위협 보고서 (2026)") == ["ai", "보안", "위협", "보고서", "2026"]


def test_normalize_title_handles_none_and_empty() -> None:
    assert normalize_title(None) == []
    assert normalize_title("") == []
    assert normalize_title("   ") == []


def test_title_similarity_exact_match_is_one() -> None:
    assert title_similarity("AI security report", "AI security report") == pytest.approx(1.0)


def test_title_similarity_partial_overlap() -> None:
    sim = title_similarity("AI security report 2026", "AI security report")
    assert 0.7 < sim < 1.0


def test_title_similarity_no_overlap_is_zero() -> None:
    assert title_similarity("AI security", "Stock market drop") == 0.0


def test_cluster_titles_groups_near_duplicates() -> None:
    titles = [
        "OpenAI announces new model GPT-X",
        "OpenAI announces a new model GPT-X",
        "Stock market falls today",
        "Apple unveils new iPhone",
        "Stock market drops today",
    ]
    ids = cluster_titles(titles, threshold=0.6)
    # First two should cluster, last two should cluster, iPhone alone
    assert ids[0] == ids[1]
    assert ids[2] == ids[4]
    assert ids[3] != ids[0] and ids[3] != ids[2]


def test_cluster_titles_threshold_one_only_exact() -> None:
    titles = ["A", "A copy", "B"]
    ids = cluster_titles(titles, threshold=1.0)
    # No two titles are identical token-wise, so each gets its own cluster.
    assert ids[0] != ids[1]
    assert ids[1] != ids[2]


def test_cluster_titles_empty_input() -> None:
    assert cluster_titles([]) == []
