from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from radar_core.jsonutil import json_default, json_dumps


def test_json_default_encodes_datetime_date_decimal_and_set() -> None:
    published = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
    encoded = json_dumps(
        {
            "published": published,
            "accepted_on": date(2026, 10, 14),
            "score": Decimal("0.91"),
            "categories": {"cs.LG", "cs.AI"},
            "frozen": frozenset({"b", "a"}),
            "nested": [{"when": published}],
        }
    )

    assert json.loads(encoded) == {
        "published": "2026-10-14T12:00:00+00:00",
        "accepted_on": "2026-10-14",
        "score": "0.91",
        "categories": ["cs.AI", "cs.LG"],
        "frozen": ["a", "b"],
        "nested": [{"when": "2026-10-14T12:00:00+00:00"}],
    }


def test_json_default_keeps_set_when_items_cannot_be_sorted() -> None:
    encoded = json.loads(json_dumps({1, "a"}))

    assert isinstance(encoded, list)
    assert set(encoded) == {1, "a"}


def test_json_default_rejects_unknown_types() -> None:
    with pytest.raises(TypeError, match="not JSON serializable"):
        json_default(object())
    with pytest.raises(TypeError, match="not JSON serializable"):
        json_dumps({"value": object()})
