from __future__ import annotations

import re
from importlib import import_module
from typing import Protocol, cast


class _KiwiToken(Protocol):
    form: str
    tag: str
    lemma: str


class _KiwiLike(Protocol):
    def tokenize(self, text: str) -> list[_KiwiToken]: ...


def _load_kiwi_class() -> type[_KiwiLike] | None:
    try:
        kiwi_module = import_module("kiwipiepy")
    except ModuleNotFoundError:
        return None

    kiwi_class = getattr(kiwi_module, "Kiwi", None)
    if kiwi_class is None:
        return None

    return cast(type[_KiwiLike], kiwi_class)


_kiwi_class = _load_kiwi_class()
_KIWI_AVAILABLE = _kiwi_class is not None

_kiwi_instance: _KiwiLike | None = None


def is_kiwi_available() -> bool:
    return _KIWI_AVAILABLE


def _get_kiwi() -> _KiwiLike | None:
    global _kiwi_instance

    if not _KIWI_AVAILABLE or _kiwi_class is None:
        return None

    if _kiwi_instance is None:
        _kiwi_instance = _kiwi_class()

    return _kiwi_instance


def tokenize_korean(text: str) -> list[str]:
    normalized_text = text.strip()
    if not normalized_text:
        return []

    kiwi = _get_kiwi()
    if kiwi is None:
        return []

    return [token.form for token in kiwi.tokenize(normalized_text)]


def extract_stems(text: str) -> list[str]:
    kiwi = _get_kiwi()
    if kiwi is None:
        return []

    normalized_text = text.strip()
    if not normalized_text:
        return []

    stems: list[str] = []
    seen: set[str] = set()
    for token in kiwi.tokenize(normalized_text):
        if not token.tag.startswith("N"):
            continue

        lemma = cast(str, getattr(token, "lemma", token.form))
        if lemma and lemma not in seen:
            stems.append(lemma)
            seen.add(lemma)

    return stems


def _build_simple_pattern(keyword: str) -> str:
    if keyword.isascii() and any(character.isalnum() for character in keyword):
        return rf"\b{re.escape(keyword)}\b"
    return re.escape(keyword)


def build_korean_pattern(keyword: str) -> str:
    normalized_keyword = keyword.strip()
    if not normalized_keyword:
        return ""

    kiwi = _get_kiwi()
    if kiwi is None:
        return _build_simple_pattern(normalized_keyword)

    stems = extract_stems(normalized_keyword)
    if not stems:
        return _build_simple_pattern(normalized_keyword)

    escaped_stems = [re.escape(stem) for stem in stems]
    return f"({'|'.join(escaped_stems)})"


class KoreanAnalyzer:
    def __init__(self) -> None:
        self._kiwi: _KiwiLike | None = _get_kiwi()

    def tokenize(self, text: str) -> list[str]:
        normalized_text = text.strip()
        if not normalized_text:
            return []

        if self._kiwi is not None:
            return [token.form for token in self._kiwi.tokenize(normalized_text)]

        return normalized_text.split()

    def match_keyword(self, text: str, keyword: str) -> bool:
        if not keyword:
            return False

        if self._kiwi is not None:
            return keyword in self.tokenize(text)

        return keyword in text
