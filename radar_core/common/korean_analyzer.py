from __future__ import annotations

from importlib import import_module
import logging
from typing import Protocol, cast

logger = logging.getLogger(__name__)


class _KiwiToken(Protocol):
    form: str


class _KiwiLike(Protocol):
    def tokenize(self, text: str) -> list[_KiwiToken]: ...


def _load_kiwi_constructor() -> type[_KiwiLike] | None:
    try:
        kiwi_module = import_module("kiwipiepy")
    except ModuleNotFoundError:
        return None

    kiwi_constructor = getattr(kiwi_module, "Kiwi", None)
    if kiwi_constructor is None:
        return None

    return cast(type[_KiwiLike], kiwi_constructor)


_KIWI_CONSTRUCTOR = _load_kiwi_constructor()
_KIWI_AVAILABLE = _KIWI_CONSTRUCTOR is not None


class KoreanAnalyzer:
    def __init__(self) -> None:
        self._kiwi: _KiwiLike | None = None
        if _KIWI_AVAILABLE and _KIWI_CONSTRUCTOR is not None:
            self._kiwi = _KIWI_CONSTRUCTOR()
        else:
            logger.warning(
                "kiwipiepy not installed; Korean analyzer using fallback substring matching"
            )

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
