from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

from .dates import daily_partition_date, to_utc_iso
from .jsonutil import json_dumps
from .models import Article


class RawLogger:
    def __init__(self, raw_dir: Path):
        self.raw_dir: Path = raw_dir

    def log(
        self,
        articles: Iterable[Article],
        *,
        source_name: str,
        run_id: str | None = None,
    ) -> Path:
        now = datetime.now(UTC)
        # Daily directory is the KST calendar date of this collection, not UTC.
        date_dir = self.raw_dir / daily_partition_date(now).isoformat()
        safe_source_name = source_name.replace("/", "_").replace("\\", "_")

        if run_id is not None:
            output_path = date_dir / f"{safe_source_name}_{run_id}.jsonl"
        else:
            output_path = date_dir / f"{safe_source_name}.jsonl"

        output_path.parent.mkdir(parents=True, exist_ok=True)

        existing_links: set[str] = set()
        if run_id is not None and output_path.exists():
            try:
                with output_path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        if line.strip():
                            record = json.loads(line)
                            existing_links.add(record.get("link", ""))
            except (json.JSONDecodeError, IOError):
                pass

        with output_path.open("a", encoding="utf-8") as handle:
            for article in articles:
                if run_id is not None and article.link in existing_links:
                    continue

                record = {
                    "title": article.title,
                    "link": article.link,
                    "summary": article.summary,
                    "published": (
                        to_utc_iso(article.published) if article.published else None
                    ),
                    "published_raw": getattr(article, "published_raw", None),
                    "source": article.source,
                    "category": article.category,
                    "matched_entities": article.matched_entities,
                    "logged_at": to_utc_iso(now),
                }
                if article.ontology:
                    record["ontology"] = article.ontology
                _ = handle.write(json_dumps(record))
                _ = handle.write("\n")
                if run_id is not None:
                    existing_links.add(article.link)

        return output_path
