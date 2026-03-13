from __future__ import annotations

import pytest
from radar_core.common import korean_analyzer
from radar_core.common.korean_analyzer import KoreanAnalyzer


def test_korean_analyzer_instantiates_without_kiwipiepy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(korean_analyzer, "_KIWI_AVAILABLE", False)

    analyzer = KoreanAnalyzer()

    assert analyzer.tokenize("인공지능 기술") == ["인공지능", "기술"]


def test_match_keyword_fallback_uses_substring(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(korean_analyzer, "_KIWI_AVAILABLE", False)
    analyzer = KoreanAnalyzer()

    assert analyzer.match_keyword("인공지능 기술 동향", "공지") is True
    assert analyzer.match_keyword("인공지능 기술 동향", "데이터") is False


def test_tokenize_fallback_splits_whitespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(korean_analyzer, "_KIWI_AVAILABLE", False)
    analyzer = KoreanAnalyzer()

    assert analyzer.tokenize("인공지능 기술 동향") == ["인공지능", "기술", "동향"]


def test_match_keyword_with_kiwi_when_installed() -> None:
    pytest.importorskip("kiwipiepy")
    analyzer = KoreanAnalyzer()

    assert analyzer.match_keyword("인공지능 기술 동향", "인공지능") is True
