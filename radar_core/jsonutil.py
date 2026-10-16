"""JSON encoding for values collectors store on articles and logs."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def json_default(value: object) -> Any:
    """Return a JSON-encodable stand-in for values ``json.dumps`` rejects.

    ``datetime`` and ``date`` become ISO 8601 strings. ``Decimal`` becomes a
    string so the exact numeric text is preserved. ``set`` and ``frozenset``
    become lists, sorted when the items can be ordered together.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (set, frozenset)):
        items = list(value)
        try:
            return sorted(items)
        except TypeError:
            return items
    raise TypeError(
        f"Object of type {value.__class__.__name__} is not JSON serializable"
    )


def json_dumps(value: object, **kwargs: Any) -> str:
    """``json.dumps`` that uses :func:`json_default` for non-standard values."""
    kwargs.setdefault("ensure_ascii", False)
    return json.dumps(value, default=json_default, **kwargs)
