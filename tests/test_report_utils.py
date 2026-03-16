from __future__ import annotations

import json
import re
from datetime import datetime

from radar_core.report_utils import generate_summary_json


def test_generate_summary_json(tmp_path) -> None:
    output_dir = tmp_path / "summaries"
    articles = [
        {
            "source": "YTN",
            "matched_entities": ["Nintendo", "PlayStation", "Nintendo"],
        },
        {
            "source": "MBC",
            "matched_entities": ["Nintendo", "Xbox"],
        },
        {
            "source": "YTN",
            "matched_entities": [],
        },
        {
            "source": "KBS",
        },
        {
            "source": "MBC",
            "matched_entities": ["PlayStation"],
        },
    ]

    result_path = generate_summary_json(
        category_name="game",
        articles=articles,
        stats={"article_count": 42, "source_count": 8, "matched_count": 35},
        output_dir=output_dir,
    )

    assert result_path.exists()
    assert re.fullmatch(r"game_\d{8}_summary\.json", result_path.name)

    payload = json.loads(result_path.read_text(encoding="utf-8"))
    assert payload["date"] == datetime.fromisoformat(payload["date"]).date().isoformat()
    assert (
        payload["generated_at"]
        == datetime.fromisoformat(payload["generated_at"]).isoformat()
    )
    assert datetime.fromisoformat(payload["generated_at"]).tzinfo is not None

    assert payload["category"] == "game"
    assert payload["article_count"] == 42
    assert payload["source_count"] == 8
    assert payload["matched_count"] == 35
    assert payload["sources"] == {"YTN": 2, "MBC": 2, "KBS": 1}
    assert payload["top_entities"] == [
        {"name": "Nintendo", "count": 3},
        {"name": "PlayStation", "count": 2},
        {"name": "Xbox", "count": 1},
    ]
    assert payload["top_entities"] == sorted(
        payload["top_entities"],
        key=lambda entity: entity["count"],
        reverse=True,
    )

    empty_path = generate_summary_json(
        category_name="game",
        articles=[],
        stats={},
        output_dir=output_dir,
    )
    empty_payload = json.loads(empty_path.read_text(encoding="utf-8"))
    assert empty_payload["article_count"] == 0
    assert empty_payload["source_count"] == 0
    assert empty_payload["matched_count"] == 0
    assert empty_payload["top_entities"] == []
    assert empty_payload["sources"] == {}

    no_entities_path = generate_summary_json(
        category_name="game",
        articles=[{"source": "YTN"}, {"source": "MBC", "matched_entities": []}],
        stats={},
        output_dir=output_dir,
    )
    no_entities_payload = json.loads(no_entities_path.read_text(encoding="utf-8"))
    assert no_entities_payload["top_entities"] == []
    assert no_entities_payload["matched_count"] == 0
    assert no_entities_payload["sources"] == {"YTN": 1, "MBC": 1}
