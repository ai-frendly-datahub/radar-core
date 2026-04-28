from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, TypedDict


class EventModelFieldSpec(TypedDict):
    required_fields: list[str]
    optional_fields: list[str]
    field_enums: dict[str, list[str]]


def resolve_runtime_contract_dir(search_from: Path | None = None) -> Path | None:
    candidates: list[Path] = []

    runtime_dir_env = os.getenv("RADAR_ONTOLOGY_RUNTIME_DIR", "").strip()
    if runtime_dir_env:
        candidates.append(Path(runtime_dir_env).expanduser())

    ontology_dir_env = os.getenv("RADAR_ONTOLOGY_DIR", "").strip()
    if ontology_dir_env:
        candidates.append(Path(ontology_dir_env).expanduser() / "runtime_contracts")

    search_roots = [Path.cwd()]
    if search_from is not None:
        search_roots.append(search_from)
    search_roots.append(Path(__file__).resolve())

    seen: set[Path] = set()
    for root in search_roots:
        base = root if root.is_dir() else root.parent
        for parent in (base, *base.parents):
            candidates.append(parent / "radar-ontology" / "runtime_contracts")
            candidates.append(parent / "runtime_contracts")

    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.is_dir():
            return resolved
    return None


def load_runtime_contract(
    repo_name: str,
    *,
    runtime_contract_dir: Path | None = None,
    search_from: Path | None = None,
) -> dict[str, Any] | None:
    contract_dir = runtime_contract_dir or resolve_runtime_contract_dir(search_from)
    if contract_dir is None:
        return None

    contract_path = contract_dir / f"{repo_name}.json"
    if not contract_path.exists():
        return None

    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def build_summary_ontology_metadata(
    repo_name: str,
    *,
    category_name: str | None = None,
    runtime_contract_dir: Path | None = None,
    search_from: Path | None = None,
) -> dict[str, Any] | None:
    contract = load_runtime_contract(
        repo_name,
        runtime_contract_dir=runtime_contract_dir,
        search_from=search_from,
    )
    if contract is None:
        return None

    event_model_mappings = _string_mapping(contract.get("event_model_mappings"))
    source_role_mappings = _string_mapping(contract.get("source_role_mappings"))
    entity_type_hints = _string_list(contract.get("entity_type_hints"))
    evidence_policy_ids = _string_list(contract.get("evidence_policy_ids"))

    metadata = {
        "repo": repo_name,
        "category": str(contract.get("category") or category_name or "").strip(),
        "ontology_version": str(contract.get("ontology_version") or "").strip(),
        "event_model_ids": sorted(set(event_model_mappings.values())),
        "event_model_mappings": event_model_mappings,
        "entity_type_hints": entity_type_hints,
        "source_role_ids": sorted(set(source_role_mappings.values())),
        "source_role_mappings": source_role_mappings,
        "evidence_policy_ids": evidence_policy_ids,
    }
    return {key: value for key, value in metadata.items() if _has_value(value)}


_DEFAULT_FIELD_EXTRACTORS: dict[str, str] = {
    # field_name -> attribute on Article (or article-like obj) used as default source.
    # Kept narrow on purpose: only fields the canonical radar-core Article carries.
    # Anything else is left absent so callers can supply their own values.
    "source_name": "source",
    "headline": "title",
    "source_url": "link",
    "summary": "summary",
}


def _default_published_at(article: object) -> str | None:
    value = getattr(article, "published", None)
    if value is None:
        return None
    iso = getattr(value, "isoformat", None)
    if callable(iso):
        try:
            return iso()
        except (TypeError, ValueError):
            return None
    text = str(value).strip()
    return text or None


def _default_tags(article: object) -> list[str] | None:
    matched = getattr(article, "matched_entities", None)
    if not isinstance(matched, Mapping):
        return None
    keys = [str(key).strip() for key in matched.keys() if str(key).strip()]
    return sorted(set(keys)) if keys else None


class EnumValueError(ValueError):
    """Raised when `build_event_model_payload(strict_enums=True)` sees an out-of-vocabulary value."""

    def __init__(self, field_name: str, value: object, allowed: list[str]) -> None:
        message = (
            f"event_model_payload field {field_name!r} value {value!r} "
            f"is not in the declared vocabulary {allowed!r}"
        )
        super().__init__(message)
        self.field_name = field_name
        self.value = value
        self.allowed = list(allowed)


def build_event_model_payload(
    article: object,
    *,
    repo_name: str,
    event_model_key: str,
    overrides: Mapping[str, Any] | None = None,
    runtime_contract_dir: Path | None = None,
    search_from: Path | None = None,
    strict_enums: bool = False,
) -> dict[str, Any] | None:
    """Build a `{field: value}` payload for the event_model bound to `event_model_key`.

    The function consults the runtime contract's `event_model_field_specs` to determine
    which required + optional fields are declared, then populates them from the article
    using a small built-in extractor for canonical Article attributes
    (`source_name`/`headline`/`source_url`/`summary`/`published_at`/`tags`).

    Fields the extractor cannot fill are absent from the returned dict so emitters
    can opt to fill them later. Pass `overrides` to inject domain-specific values
    (e.g. `{"asset_symbols": ["BTC", "ETH"]}` for `crypto.market_commentary`).

    `strict_enums=True` validates each populated field against the contract's
    `field_enums`, raising `EnumValueError` if a value is outside the declared
    vocabulary. List-valued fields are checked element-wise. Fields without a declared
    enum are not constrained.

    Accepts canonical `Article` instances or plain `Mapping` (dict) row payloads —
    Mapping inputs are converted to `SimpleNamespace` internally so the same
    `getattr` / `hasattr` extraction path applies uniformly.

    Returns None when the contract or the field spec for `event_model_key` is missing.
    """
    if isinstance(article, Mapping):
        article = SimpleNamespace(**article)
    spec = get_event_model_field_spec(
        repo_name,
        event_model_key,
        runtime_contract_dir=runtime_contract_dir,
        search_from=search_from,
    )
    if spec is None:
        return None
    declared_fields: list[str] = []
    seen: set[str] = set()
    for field_name in [*spec["required_fields"], *spec["optional_fields"]]:
        if field_name in seen:
            continue
        seen.add(field_name)
        declared_fields.append(field_name)

    field_enums: Mapping[str, list[str]] = spec.get("field_enums") or {}
    overrides_map: Mapping[str, Any] = overrides or {}
    payload: dict[str, Any] = {}
    for field_name in declared_fields:
        if field_name in overrides_map:
            value = overrides_map[field_name]
        elif field_name == "published_at":
            value = _default_published_at(article)
        elif field_name == "tags":
            value = _default_tags(article)
        elif field_name in _DEFAULT_FIELD_EXTRACTORS:
            attr = _DEFAULT_FIELD_EXTRACTORS[field_name]
            value = getattr(article, attr, None)
            if isinstance(value, str):
                value = value.strip() or None
            # When the canonical Article attribute is absent (non-standard schema
            # row objects don't have `link`/`title`/`source`), fall through to a
            # same-named attribute as a last resort.
            if value is None and not hasattr(article, attr):
                value = getattr(article, field_name, None)
                if isinstance(value, str):
                    value = value.strip() or None
        else:
            # Direct getattr fallback for domain-specific fields. Non-standard schema
            # repos (Home / Price / Property / Trend / Wine) carry their domain values
            # as attributes on their row objects (`lawd_cd`, `effective_price`,
            # `keyword_set_name`, etc.); the contract declares those fields per
            # event_model, and this fallback wires them through without forcing each
            # repo to register a custom extractor.
            value = getattr(article, field_name, None)
            if isinstance(value, str):
                value = value.strip() or None
        if value is None:
            continue
        if isinstance(value, str) and not value:
            continue
        if isinstance(value, (list, tuple)) and not value:
            continue
        if strict_enums and field_name in field_enums:
            allowed = list(field_enums[field_name])
            allowed_set = set(allowed)
            if isinstance(value, (list, tuple)):
                for element in value:
                    if element not in allowed_set:
                        raise EnumValueError(field_name, element, allowed)
            elif value not in allowed_set:
                raise EnumValueError(field_name, value, allowed)
        payload[field_name] = list(value) if isinstance(value, tuple) else value
    return payload


def get_event_model_field_spec(
    repo_name: str,
    event_model_key: str,
    *,
    runtime_contract_dir: Path | None = None,
    search_from: Path | None = None,
) -> EventModelFieldSpec | None:
    """Return `{required_fields, optional_fields}` for `event_model_key` in repo's contract.

    `event_model_key` is the local key under `event_model_mappings` (e.g. ``"editorial_coverage"``),
    not the global ontology id. Returns None if the contract or the key is missing, so emitters
    can choose to no-op when running without an ontology installation.
    """
    contract = load_runtime_contract(
        repo_name,
        runtime_contract_dir=runtime_contract_dir,
        search_from=search_from,
    )
    if contract is None:
        return None
    field_specs = contract.get("event_model_field_specs")
    if not isinstance(field_specs, Mapping):
        return None
    spec = field_specs.get(event_model_key.strip())
    if not isinstance(spec, Mapping):
        return None
    required = _string_list(spec.get("required_fields"))
    optional = _string_list(spec.get("optional_fields"))
    raw_enums = spec.get("field_enums")
    field_enums: dict[str, list[str]] = {}
    if isinstance(raw_enums, Mapping):
        for fname, values in raw_enums.items():
            normalized_name = str(fname).strip()
            if not normalized_name or not isinstance(values, list):
                continue
            field_enums[normalized_name] = _string_list(values)
    return {
        "required_fields": required,
        "optional_fields": optional,
        "field_enums": field_enums,
    }


def build_article_ontology_metadata(
    repo_name: str,
    *,
    source_name: str,
    source_event_model: str | None = None,
    category_name: str | None = None,
    runtime_contract_dir: Path | None = None,
    search_from: Path | None = None,
) -> dict[str, Any] | None:
    contract = load_runtime_contract(
        repo_name,
        runtime_contract_dir=runtime_contract_dir,
        search_from=search_from,
    )
    if contract is None:
        return None

    source_name_normalized = source_name.strip()
    source_event_model_normalized = str(source_event_model or "").strip()
    event_model_mappings = _string_mapping(contract.get("event_model_mappings"))
    source_role_mappings = _string_mapping(contract.get("source_role_mappings"))
    evidence_policy_ids = _string_list(contract.get("evidence_policy_ids"))

    metadata = {
        "repo": repo_name,
        "category": str(contract.get("category") or category_name or "").strip(),
        "ontology_version": str(contract.get("ontology_version") or "").strip(),
        "source_role_id": source_role_mappings.get(source_name_normalized, "").strip(),
        "source_event_model": source_event_model_normalized,
        "event_model_id": event_model_mappings.get(source_event_model_normalized, "").strip(),
        "evidence_policy_ids": evidence_policy_ids,
    }
    return {key: value for key, value in metadata.items() if _has_value(value)}


def annotate_articles_with_ontology(
    articles: list[Any],
    *,
    repo_name: str,
    sources_by_name: Mapping[str, object],
    category_name: str | None = None,
    runtime_contract_dir: Path | None = None,
    search_from: Path | None = None,
    attach_event_model_payload: bool = False,
    payload_overrides_by_source: Mapping[str, Mapping[str, Any]] | None = None,
    strict_enums: bool = False,
    enum_violations: list[dict[str, Any]] | None = None,
) -> list[Any]:
    """Attach ontology metadata to each article in place, returning the same list.

    When `attach_event_model_payload=True` and the article's source resolves to an
    event_model in the runtime contract, the resulting metadata also includes an
    `event_model_payload` dict populated from the article's canonical fields plus
    any per-source overrides supplied via `payload_overrides_by_source`. Sources
    without a contracted event_model are left without a payload — opt-in by design
    so legacy emitters keep their existing payload shape.
    """
    explicit_overrides = payload_overrides_by_source is not None
    overrides_table: Mapping[str, Mapping[str, Any]] = (
        payload_overrides_by_source if payload_overrides_by_source else {}
    )
    for article in articles:
        source_name = str(getattr(article, "source", "") or "").strip()
        source = sources_by_name.get(source_name)
        source_event_model = _extract_source_event_model(source)
        metadata = build_article_ontology_metadata(
            repo_name,
            source_name=source_name,
            source_event_model=source_event_model,
            category_name=category_name,
            runtime_contract_dir=runtime_contract_dir,
            search_from=search_from,
        )
        if metadata is None:
            continue
        if (
            attach_event_model_payload
            and source_event_model
            and metadata.get("event_model_id")
        ):
            if explicit_overrides:
                source_overrides = overrides_table.get(source_name)
            else:
                source_overrides = _extract_source_payload_overrides(source)
            try:
                payload = build_event_model_payload(
                    article,
                    repo_name=repo_name,
                    event_model_key=source_event_model,
                    overrides=source_overrides,
                    runtime_contract_dir=runtime_contract_dir,
                    search_from=search_from,
                    strict_enums=strict_enums or enum_violations is not None,
                )
            except EnumValueError as exc:
                if enum_violations is None:
                    raise
                enum_violations.append(
                    {
                        "repo": repo_name,
                        "source_name": source_name,
                        "event_model_key": source_event_model,
                        "field_name": exc.field_name,
                        "value": exc.value,
                        "allowed": list(exc.allowed),
                        "article_link": getattr(article, "link", None),
                    }
                )
                payload = None
            if payload:
                metadata["event_model_payload"] = payload
        setattr(article, "ontology", metadata)
    return articles


def backfill_duckdb_ontology(
    db_path: Path,
    *,
    repo_name: str,
    sources_by_name: Mapping[str, object],
    category_name: str | None = None,
    runtime_contract_dir: Path | None = None,
    search_from: Path | None = None,
    table_name: str = "articles",
    default_event_model_key: str | None = None,
    attach_event_model_payload: bool = False,
    payload_overrides_by_source: Mapping[str, Mapping[str, Any]] | None = None,
    strict_enums: bool = False,
    enum_violations: list[dict[str, Any]] | None = None,
) -> dict[str, int]:
    """Re-emit ontology_json for existing rows using current source config + runtime contract.

    Returns a dict with `scanned`, `updated`, `with_event_model_id`,
    `default_fallback_applied`, and (when payload attach is on) `with_event_model_payload`
    counts. Safe to run repeatedly: only rewrites rows whose ontology_json would change.

    `default_event_model_key`, when set, is used as `source_event_model` whenever an
    article's source either is not in `sources_by_name` (legacy/yaml-deleted sources)
    or has no `config.event_model` set. This lets historical rows recover an
    `event_model_id` without re-tagging deleted yaml entries. The key must already be
    present in the runtime contract's `event_model_mappings`; otherwise it falls
    through and event_model_id remains empty.

    `attach_event_model_payload=True` makes backfill rebuild the per-row
    `event_model_payload` from the existing `title`/`summary`/`published`/`entities_json`
    columns (when present) plus any `payload_overrides_by_source[source_name]`. Rows are
    skipped silently when those columns don't exist in this schema, so the function
    still works on minimal/legacy table shapes.
    """
    import duckdb  # imported lazily so radar-core consumers without duckdb still work

    quoted_table = '"' + table_name.replace('"', '""') + '"'
    counts = {
        "scanned": 0,
        "updated": 0,
        "with_event_model_id": 0,
        "default_fallback_applied": 0,
        "with_event_model_payload": 0,
    }
    overrides_table: Mapping[str, Mapping[str, Any]] = (
        payload_overrides_by_source if payload_overrides_by_source else {}
    )
    with duckdb.connect(str(db_path)) as conn:
        existing_columns = {
            str(row[0])
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'main' AND table_name = ?",
                [table_name],
            ).fetchall()
        }
        if "ontology_json" not in existing_columns or "link" not in existing_columns:
            return counts

        # Build a row-aware select that pulls article-level columns when available.
        select_columns: list[str] = ["link", "source", "ontology_json"]
        for optional_col in ("title", "summary", "published", "entities_json"):
            if optional_col in existing_columns:
                select_columns.append(optional_col)
        column_index = {name: i for i, name in enumerate(select_columns)}
        select_clause = ", ".join(select_columns)
        rows = conn.execute(f"SELECT {select_clause} FROM {quoted_table}").fetchall()

        updates: list[tuple[str, str]] = []
        for row in rows:
            counts["scanned"] += 1
            link = row[column_index["link"]]
            source_name = row[column_index["source"]]
            existing_ontology_json = row[column_index["ontology_json"]]
            normalized_source = str(source_name or "").strip()
            source = sources_by_name.get(normalized_source)
            source_event_model = _extract_source_event_model(source)
            used_default = False
            if not source_event_model and default_event_model_key:
                source_event_model = default_event_model_key
                used_default = True
            metadata = build_article_ontology_metadata(
                repo_name,
                source_name=normalized_source,
                source_event_model=source_event_model,
                category_name=category_name,
                runtime_contract_dir=runtime_contract_dir,
                search_from=search_from,
            )
            if metadata is None:
                continue
            if (
                attach_event_model_payload
                and source_event_model
                and metadata.get("event_model_id")
            ):
                article_proxy = _RowArticleProxy(
                    title=row[column_index["title"]] if "title" in column_index else None,
                    summary=row[column_index["summary"]] if "summary" in column_index else None,
                    published=row[column_index["published"]] if "published" in column_index else None,
                    link=link,
                    source=normalized_source,
                    matched_entities=_decode_entities_json(
                        row[column_index["entities_json"]]
                        if "entities_json" in column_index
                        else None
                    ),
                )
                try:
                    payload = build_event_model_payload(
                        article_proxy,
                        repo_name=repo_name,
                        event_model_key=source_event_model,
                        overrides=overrides_table.get(normalized_source),
                        runtime_contract_dir=runtime_contract_dir,
                        search_from=search_from,
                        strict_enums=strict_enums or enum_violations is not None,
                    )
                except EnumValueError as exc:
                    if enum_violations is None:
                        raise
                    enum_violations.append(
                        {
                            "repo": repo_name,
                            "source_name": normalized_source,
                            "event_model_key": source_event_model,
                            "field_name": exc.field_name,
                            "value": exc.value,
                            "allowed": list(exc.allowed),
                            "article_link": str(link) if link is not None else None,
                        }
                    )
                    payload = None
                if payload:
                    metadata["event_model_payload"] = payload
                    counts["with_event_model_payload"] += 1
            new_payload = json.dumps(metadata, ensure_ascii=False)
            payload_changed = existing_ontology_json != new_payload
            has_event_model = bool(metadata.get("event_model_id"))
            if payload_changed:
                updates.append((new_payload, str(link)))
            if has_event_model:
                counts["with_event_model_id"] += 1
                if used_default:
                    counts["default_fallback_applied"] += 1

        if updates:
            conn.executemany(
                f"UPDATE {quoted_table} SET ontology_json = ? WHERE link = ?",
                updates,
            )
            counts["updated"] = len(updates)
    return counts


class _RowArticleProxy:
    """Lightweight stand-in for an Article when re-emitting from DuckDB rows."""

    __slots__ = ("title", "summary", "published", "link", "source", "matched_entities")

    def __init__(
        self,
        *,
        title: object,
        summary: object,
        published: object,
        link: object,
        source: object,
        matched_entities: dict[str, list[str]] | None,
    ) -> None:
        self.title = str(title) if title is not None else ""
        self.summary = str(summary) if summary is not None else ""
        self.published = published
        self.link = str(link) if link is not None else ""
        self.source = str(source) if source is not None else ""
        self.matched_entities = matched_entities or {}


def _decode_entities_json(raw: object) -> dict[str, list[str]]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return {str(k): list(v) if isinstance(v, list) else [] for k, v in raw.items()}
    text = str(raw).strip()
    if not text:
        return {}
    try:
        decoded = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(decoded, dict):
        return {}
    return {str(k): list(v) if isinstance(v, list) else [] for k, v in decoded.items()}


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized


def _string_mapping(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    normalized: dict[str, str] = {}
    for key, item in value.items():
        normalized_key = str(key).strip()
        normalized_value = str(item).strip()
        if normalized_key and normalized_value:
            normalized[normalized_key] = normalized_value
    return normalized


def _extract_source_event_model(source: object) -> str | None:
    if source is None:
        return None
    config = getattr(source, "config", None)
    if not isinstance(config, Mapping):
        return None
    raw_value = str(config.get("event_model") or "").strip()
    return raw_value or None


def _extract_source_payload_overrides(source: object) -> Mapping[str, Any] | None:
    """Read `event_model_payload_overrides` from the source's config dict.

    Sources can declare per-source enum / domain values in their YAML config so
    collectors don't have to hand-craft the overrides table at every annotate call.
    Returns None when the source has no config or no override block.
    """
    if source is None:
        return None
    config = getattr(source, "config", None)
    if not isinstance(config, Mapping):
        return None
    raw = config.get("event_model_payload_overrides")
    if not isinstance(raw, Mapping):
        return None
    cleaned: dict[str, Any] = {}
    for key, value in raw.items():
        normalized_key = str(key).strip()
        if not normalized_key:
            continue
        cleaned[normalized_key] = value
    return cleaned or None


def _has_value(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return bool(value)
    return True
