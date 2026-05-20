from __future__ import annotations

import json

from radar_core.models import Article
from radar_core.ontology import (
    EnumValueError,
    annotate_articles_with_ontology,
    build_article_ontology_metadata,
    build_event_model_payload,
    build_summary_ontology_metadata,
    get_event_model_field_spec,
    load_runtime_contract,
)


def test_load_runtime_contract_from_env_dir(tmp_path, monkeypatch) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "GovRadar",
        "category": "govsupport",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"application_deadline": "govsupport.application_deadline"},
    }
    (runtime_dir / "GovRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    loaded = load_runtime_contract("GovRadar")

    assert loaded == contract


def test_build_summary_ontology_metadata_normalizes_runtime_contract(
    tmp_path,
    monkeypatch,
) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "PolicyRadar",
        "category": "policy",
        "ontology_version": "0.1.0",
        "event_model_mappings": {
            "enforcement_action": "policy.enforcement_action",
            "public_consultation": "policy.public_consultation",
        },
        "entity_type_hints": ["policy.regulation", "organization.agency"],
        "source_role_mappings": {
            "Federal Register": "primary_evidence",
            "NIST News": "context_only",
        },
        "evidence_policy_ids": ["traceable_url_required"],
    }
    (runtime_dir / "PolicyRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    metadata = build_summary_ontology_metadata("PolicyRadar")

    assert metadata == {
        "repo": "PolicyRadar",
        "category": "policy",
        "ontology_version": "0.1.0",
        "event_model_ids": [
            "policy.enforcement_action",
            "policy.public_consultation",
        ],
        "event_model_mappings": {
            "enforcement_action": "policy.enforcement_action",
            "public_consultation": "policy.public_consultation",
        },
        "entity_type_hints": ["policy.regulation", "organization.agency"],
        "source_role_ids": ["context_only", "primary_evidence"],
        "source_role_mappings": {
            "Federal Register": "primary_evidence",
            "NIST News": "context_only",
        },
        "evidence_policy_ids": ["traceable_url_required"],
    }


def test_build_article_ontology_metadata_maps_source_role_and_event_model(
    tmp_path,
    monkeypatch,
) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "TrustRadar",
        "category": "trust",
        "ontology_version": "0.1.0",
        "event_model_mappings": {
            "incident_disclosure": "trust.incident_disclosure",
        },
        "source_role_mappings": {
            "KISA Security Notice": "primary_evidence",
        },
        "evidence_policy_ids": ["traceable_url_required", "event_date_required"],
    }
    (runtime_dir / "TrustRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    metadata = build_article_ontology_metadata(
        "TrustRadar",
        source_name="KISA Security Notice",
        source_event_model="incident_disclosure",
    )

    assert metadata == {
        "repo": "TrustRadar",
        "category": "trust",
        "ontology_version": "0.1.0",
        "source_role_id": "primary_evidence",
        "source_event_model": "incident_disclosure",
        "event_model_id": "trust.incident_disclosure",
        "evidence_policy_ids": [
            "traceable_url_required",
            "event_date_required",
        ],
    }


def test_annotate_articles_with_ontology_sets_article_field(tmp_path, monkeypatch) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "MovieRadar",
        "category": "movie",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"box_office": "media.box_office"},
        "source_role_mappings": {"KOFIC 박스오피스": "operational_evidence"},
        "evidence_policy_ids": ["traceable_url_required"],
    }
    (runtime_dir / "MovieRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))
    article = Article(
        title="Daily box office",
        link="https://example.com/box-office",
        summary="summary",
        published=None,
        source="KOFIC 박스오피스",
        category="movie",
    )

    annotated = annotate_articles_with_ontology(
        [article],
        repo_name="MovieRadar",
        sources_by_name={
            "KOFIC 박스오피스": type(
                "SourceStub",
                (),
                {"config": {"event_model": "box_office"}},
            )()
        },
        category_name="movie",
    )

    assert annotated[0].ontology == {
        "repo": "MovieRadar",
        "category": "movie",
        "ontology_version": "0.1.0",
        "source_role_id": "operational_evidence",
        "source_event_model": "box_office",
        "event_model_id": "media.box_office",
        "evidence_policy_ids": ["traceable_url_required"],
    }


def test_backfill_duckdb_ontology_updates_existing_rows(tmp_path, monkeypatch) -> None:
    import duckdb

    from radar_core.ontology import backfill_duckdb_ontology

    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "QueueRadar",
        "category": "queue",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"wait_time_snapshot": "queue.wait_time_snapshot"},
        "source_role_mappings": {
            "Disney Magic Kingdom": "operational_evidence",
            "Theme Park Insider": "context_only",
        },
        "evidence_policy_ids": ["traceable_url_required"],
    }
    (runtime_dir / "QueueRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    db_path = tmp_path / "queue.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE articles (link VARCHAR PRIMARY KEY, source VARCHAR, ontology_json VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO articles VALUES (?, ?, ?)",
            [
                ("https://example.com/disney/1", "Disney Magic Kingdom", None),
                ("https://example.com/insider/1", "Theme Park Insider", "{}"),
                ("https://example.com/disney/2", "Disney Magic Kingdom", None),
            ],
        )

    sources = {
        "Disney Magic Kingdom": type(
            "SourceStub",
            (),
            {"config": {"event_model": "wait_time_snapshot"}},
        )(),
        "Theme Park Insider": type("SourceStub", (), {"config": {}})(),
    }
    counts = backfill_duckdb_ontology(
        db_path,
        repo_name="QueueRadar",
        sources_by_name=sources,
        category_name="queue",
    )

    assert counts["scanned"] == 3
    assert counts["updated"] == 3
    assert counts["with_event_model_id"] == 2

    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            "SELECT source, ontology_json FROM articles ORDER BY link"
        ).fetchall()

    parsed = [json.loads(payload) if payload else None for _, payload in rows]
    sources_seen = [source for source, _ in rows]
    assert sources_seen == [
        "Disney Magic Kingdom",
        "Disney Magic Kingdom",
        "Theme Park Insider",
    ]
    assert parsed[0]["event_model_id"] == "queue.wait_time_snapshot"
    assert parsed[0]["source_role_id"] == "operational_evidence"
    assert parsed[2]["source_role_id"] == "context_only"
    assert "event_model_id" not in parsed[2]

    counts_again = backfill_duckdb_ontology(
        db_path,
        repo_name="QueueRadar",
        sources_by_name=sources,
        category_name="queue",
    )
    assert counts_again["scanned"] == 3
    assert counts_again["updated"] == 0
    assert counts_again["with_event_model_id"] == 2


def test_backfill_duckdb_ontology_default_event_model_fallback(tmp_path, monkeypatch) -> None:
    """default_event_model_key recovers historical rows whose source is no longer in yaml."""
    import duckdb

    from radar_core.ontology import backfill_duckdb_ontology

    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "PaperRadar",
        "category": "paper",
        "ontology_version": "0.1.0",
        "event_model_mappings": {
            "paper_release": "paper.paper_release",
            "editorial_coverage": "paper.editorial_coverage",
        },
        "evidence_policy_ids": ["traceable_url_required"],
    }
    (runtime_dir / "PaperRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    db_path = tmp_path / "papers.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE articles (link VARCHAR PRIMARY KEY, source VARCHAR, ontology_json VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO articles VALUES (?, ?, ?)",
            [
                ("https://arxiv.org/abs/1", "arXiv CS.AI", None),
                ("https://example.com/legacy/1", "Legacy Source Removed", None),
                ("https://example.com/legacy/2", "Another Removed Source", None),
            ],
        )

    sources = {
        "arXiv CS.AI": type(
            "SourceStub",
            (),
            {"config": {"event_model": "paper_release"}},
        )(),
    }

    counts_without = backfill_duckdb_ontology(
        db_path,
        repo_name="PaperRadar",
        sources_by_name=sources,
        category_name="paper",
    )
    assert counts_without["scanned"] == 3
    assert counts_without["with_event_model_id"] == 1
    assert counts_without["default_fallback_applied"] == 0

    counts_with = backfill_duckdb_ontology(
        db_path,
        repo_name="PaperRadar",
        sources_by_name=sources,
        category_name="paper",
        default_event_model_key="editorial_coverage",
    )
    assert counts_with["scanned"] == 3
    assert counts_with["with_event_model_id"] == 3
    assert counts_with["default_fallback_applied"] == 2

    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            "SELECT source, ontology_json FROM articles ORDER BY link"
        ).fetchall()
    parsed = [json.loads(payload) if payload else {} for _, payload in rows]
    assert parsed[0]["event_model_id"] == "paper.paper_release"
    assert parsed[1]["event_model_id"] == "paper.editorial_coverage"
    assert parsed[2]["event_model_id"] == "paper.editorial_coverage"

    counts_again = backfill_duckdb_ontology(
        db_path,
        repo_name="PaperRadar",
        sources_by_name=sources,
        category_name="paper",
        default_event_model_key="editorial_coverage",
    )
    assert counts_again["updated"] == 0


def test_backfill_duckdb_ontology_attaches_event_model_payload(tmp_path, monkeypatch) -> None:
    """Backfill rebuilds event_model_payload from existing article columns."""
    import duckdb
    from datetime import UTC, datetime

    from radar_core.ontology import backfill_duckdb_ontology

    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "PolicyRadar",
        "category": "policy",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"editorial_coverage": "policy.editorial_coverage"},
        "event_model_field_specs": {
            "editorial_coverage": {
                "ontology_id": "policy.editorial_coverage",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": ["published_at", "summary", "source_kind", "tags"],
                "field_enums": {
                    "source_kind": ["news", "blog", "press_release", "academic", "official"]
                },
            }
        },
    }
    (runtime_dir / "PolicyRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    db_path = tmp_path / "policy.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE articles ("
            " link VARCHAR PRIMARY KEY,"
            " source VARCHAR,"
            " title VARCHAR,"
            " summary VARCHAR,"
            " published TIMESTAMP,"
            " entities_json VARCHAR,"
            " ontology_json VARCHAR"
            ")"
        )
        conn.execute(
            "INSERT INTO articles VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                "https://gov.example/news/9",
                "Senate Wire",
                "Bill Q approved",
                "Q clears committee.",
                datetime(2026, 4, 27, 12, 0, tzinfo=UTC),
                json.dumps({"committee": ["body"]}, ensure_ascii=False),
                None,
            ],
        )

    sources = {
        "Senate Wire": type(
            "SourceStub",
            (),
            {"config": {"event_model": "editorial_coverage"}},
        )(),
    }

    counts = backfill_duckdb_ontology(
        db_path,
        repo_name="PolicyRadar",
        sources_by_name=sources,
        category_name="policy",
        attach_event_model_payload=True,
        payload_overrides_by_source={"Senate Wire": {"source_kind": "official"}},
    )
    assert counts["scanned"] == 1
    assert counts["with_event_model_id"] == 1
    assert counts["with_event_model_payload"] == 1
    assert counts["updated"] == 1

    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute("SELECT ontology_json FROM articles").fetchone()
    metadata = json.loads(row[0])
    payload = metadata["event_model_payload"]
    assert payload["source_name"] == "Senate Wire"
    assert payload["headline"] == "Bill Q approved"
    assert payload["source_url"] == "https://gov.example/news/9"
    assert payload["summary"] == "Q clears committee."
    assert payload["source_kind"] == "official"
    assert payload["tags"] == ["committee"]
    assert "published_at" in payload  # extracted from `published` timestamp


def test_get_event_model_field_spec_reads_runtime_contract(tmp_path, monkeypatch) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "ArtRadar",
        "category": "art",
        "ontology_version": "0.1.0",
        "event_model_mappings": {
            "auction_result": "art.auction_result",
            "editorial_coverage": "art.editorial_coverage",
        },
        "event_model_field_specs": {
            "auction_result": {
                "ontology_id": "art.auction_result",
                "required_fields": ["auction_house", "lot_id", "source_url"],
                "optional_fields": [],
            },
            "editorial_coverage": {
                "ontology_id": "art.editorial_coverage",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": ["published_at", "summary", "source_kind", "tags"],
            },
        },
    }
    (runtime_dir / "ArtRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    editorial = get_event_model_field_spec("ArtRadar", "editorial_coverage")
    assert editorial == {
        "required_fields": ["source_name", "headline", "source_url"],
        "optional_fields": ["published_at", "summary", "source_kind", "tags"],
        "field_enums": {},
    }

    auction = get_event_model_field_spec("ArtRadar", "auction_result")
    assert auction == {
        "required_fields": ["auction_house", "lot_id", "source_url"],
        "optional_fields": [],
        "field_enums": {},
    }

    assert get_event_model_field_spec("ArtRadar", "missing_key") is None
    assert get_event_model_field_spec("UnknownRepo", "editorial_coverage") is None


def test_get_event_model_field_spec_returns_none_when_field_specs_absent(
    tmp_path, monkeypatch
) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    legacy_contract = {
        "repo": "GovRadar",
        "category": "govsupport",
        "ontology_version": "0.0.9",
        "event_model_mappings": {"editorial_coverage": "govsupport.editorial_coverage"},
    }
    (runtime_dir / "GovRadar.json").write_text(
        json.dumps(legacy_contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    assert get_event_model_field_spec("GovRadar", "editorial_coverage") is None


def test_get_event_model_field_spec_surfaces_field_enums(tmp_path, monkeypatch) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "CryptoRadar",
        "category": "crypto",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"market_commentary": "crypto.market_commentary"},
        "event_model_field_specs": {
            "market_commentary": {
                "ontology_id": "crypto.market_commentary",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": ["published_at", "summary", "asset_symbols", "sentiment"],
                "field_enums": {"sentiment": ["bullish", "bearish", "neutral", "mixed"]},
            }
        },
    }
    (runtime_dir / "CryptoRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    spec = get_event_model_field_spec("CryptoRadar", "market_commentary")
    assert spec == {
        "required_fields": ["source_name", "headline", "source_url"],
        "optional_fields": ["published_at", "summary", "asset_symbols", "sentiment"],
        "field_enums": {"sentiment": ["bullish", "bearish", "neutral", "mixed"]},
    }


def _write_crypto_contract(runtime_dir, monkeypatch) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    contract = {
        "repo": "CryptoRadar",
        "category": "crypto",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"market_commentary": "crypto.market_commentary"},
        "event_model_field_specs": {
            "market_commentary": {
                "ontology_id": "crypto.market_commentary",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": ["published_at", "summary", "asset_symbols", "sentiment"],
                "field_enums": {"sentiment": ["bullish", "bearish", "neutral", "mixed"]},
            }
        },
    }
    (runtime_dir / "CryptoRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))


def test_build_event_model_payload_strict_enums_accepts_valid(
    tmp_path, monkeypatch
) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    _write_crypto_contract(runtime_dir, monkeypatch)
    article = Article(
        title="t",
        link="https://x.example/",
        summary="s",
        published=None,
        source="Wire",
        category="crypto",
    )
    payload = build_event_model_payload(
        article,
        repo_name="CryptoRadar",
        event_model_key="market_commentary",
        overrides={"sentiment": "bullish"},
        strict_enums=True,
    )
    assert payload["sentiment"] == "bullish"


def test_build_event_model_payload_strict_enums_rejects_unknown_value(
    tmp_path, monkeypatch
) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    _write_crypto_contract(runtime_dir, monkeypatch)
    article = Article(
        title="t",
        link="https://x.example/",
        summary="s",
        published=None,
        source="Wire",
        category="crypto",
    )
    try:
        build_event_model_payload(
            article,
            repo_name="CryptoRadar",
            event_model_key="market_commentary",
            overrides={"sentiment": "ecstatic"},
            strict_enums=True,
        )
    except EnumValueError as exc:
        assert exc.field_name == "sentiment"
        assert exc.value == "ecstatic"
        assert "bullish" in exc.allowed
    else:
        raise AssertionError("expected EnumValueError for invalid enum value")


def test_build_event_model_payload_direct_getattr_fallback_for_domain_fields(
    tmp_path, monkeypatch
) -> None:
    """Non-standard schema repos can populate domain fields via plain attributes."""
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "HomeRadar",
        "category": "home",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"transaction_price": "home.transaction_price"},
        "event_model_field_specs": {
            "transaction_price": {
                "ontology_id": "home.transaction_price",
                "required_fields": ["lawd_cd", "deal_date", "source_url"],
                "optional_fields": [],
                "field_enums": {},
            }
        },
    }
    (runtime_dir / "HomeRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    # Domain-specific row object — no Article shape, just attributes that match
    # the contract's required_fields. The fallback should pick them up.
    listing = type(
        "Listing",
        (),
        {
            "lawd_cd": "11680",
            "deal_date": "2026-04-15",
            "source_url": "https://land.example/deals/9",
        },
    )()
    payload = build_event_model_payload(
        listing,
        repo_name="HomeRadar",
        event_model_key="transaction_price",
    )
    assert payload == {
        "lawd_cd": "11680",
        "deal_date": "2026-04-15",
        "source_url": "https://land.example/deals/9",
    }


def test_build_event_model_payload_strict_enums_off_lets_invalid_pass(
    tmp_path, monkeypatch
) -> None:
    """Default strict_enums=False -> emitter ergonomics; backward compatible."""
    runtime_dir = tmp_path / "runtime_contracts"
    _write_crypto_contract(runtime_dir, monkeypatch)
    article = Article(
        title="t",
        link="https://x.example/",
        summary="s",
        published=None,
        source="Wire",
        category="crypto",
    )
    payload = build_event_model_payload(
        article,
        repo_name="CryptoRadar",
        event_model_key="market_commentary",
        overrides={"sentiment": "ecstatic"},
    )
    assert payload["sentiment"] == "ecstatic"


def _write_editorial_contract(runtime_dir, monkeypatch) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    contract = {
        "repo": "ArtRadar",
        "category": "art",
        "ontology_version": "0.1.0",
        "event_model_mappings": {
            "editorial_coverage": "art.editorial_coverage",
        },
        "event_model_field_specs": {
            "editorial_coverage": {
                "ontology_id": "art.editorial_coverage",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": ["published_at", "summary", "source_kind", "tags"],
            },
        },
    }
    (runtime_dir / "ArtRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))


def test_build_event_model_payload_extracts_canonical_article_fields(
    tmp_path, monkeypatch
) -> None:
    from datetime import UTC, datetime

    runtime_dir = tmp_path / "runtime_contracts"
    _write_editorial_contract(runtime_dir, monkeypatch)

    article = Article(
        title="Renaissance redefined",
        link="https://museum.example/news/42",
        summary="Curators trace a new lineage.",
        published=datetime(2026, 4, 27, 10, 30, tzinfo=UTC),
        source="Museum Press",
        category="art",
        matched_entities={"renaissance": ["era"], "museum": ["org"]},
    )
    payload = build_event_model_payload(
        article,
        repo_name="ArtRadar",
        event_model_key="editorial_coverage",
    )
    assert payload == {
        "source_name": "Museum Press",
        "headline": "Renaissance redefined",
        "source_url": "https://museum.example/news/42",
        "published_at": "2026-04-27T10:30:00+00:00",
        "summary": "Curators trace a new lineage.",
        "tags": ["museum", "renaissance"],
    }
    # `source_kind` is declared optional but not extractable from Article -> absent
    assert "source_kind" not in payload


def test_build_event_model_payload_overrides_win(tmp_path, monkeypatch) -> None:
    from datetime import UTC, datetime

    runtime_dir = tmp_path / "runtime_contracts"
    _write_editorial_contract(runtime_dir, monkeypatch)

    article = Article(
        title="Headline",
        link="https://x.example/a",
        summary="",
        published=datetime(2026, 4, 27, tzinfo=UTC),
        source="Wire",
        category="art",
    )
    payload = build_event_model_payload(
        article,
        repo_name="ArtRadar",
        event_model_key="editorial_coverage",
        overrides={"source_kind": "press_release", "tags": ["alpha"]},
    )
    assert payload["source_kind"] == "press_release"
    assert payload["tags"] == ["alpha"]
    assert payload["headline"] == "Headline"
    # empty summary on Article should not appear
    assert "summary" not in payload


def test_build_event_model_payload_returns_none_when_spec_absent(
    tmp_path, monkeypatch
) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    legacy_contract = {
        "repo": "ArtRadar",
        "category": "art",
        "ontology_version": "0.0.9",
        "event_model_mappings": {"editorial_coverage": "art.editorial_coverage"},
    }
    (runtime_dir / "ArtRadar.json").write_text(
        json.dumps(legacy_contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    article = Article(
        title="t",
        link="https://x.example/",
        summary="s",
        published=None,
        source="Wire",
        category="art",
    )
    assert (
        build_event_model_payload(
            article,
            repo_name="ArtRadar",
            event_model_key="editorial_coverage",
        )
        is None
    )


def test_annotate_articles_attaches_event_model_payload_when_requested(
    tmp_path, monkeypatch
) -> None:
    from datetime import UTC, datetime

    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "PolicyRadar",
        "category": "policy",
        "ontology_version": "0.1.0",
        "event_model_mappings": {
            "legislative_action": "policy.legislative_action",
        },
        "event_model_field_specs": {
            "legislative_action": {
                "ontology_id": "policy.legislative_action",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": [
                    "published_at",
                    "summary",
                    "jurisdiction",
                    "action_type",
                    "bill_number",
                ],
            }
        },
    }
    (runtime_dir / "PolicyRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    article = Article(
        title="Bill Q passes committee",
        link="https://gov.example/bills/Q-2026",
        summary="Q advances on a 12-3 vote.",
        published=datetime(2026, 4, 27, 9, 0, tzinfo=UTC),
        source="Senate Wire",
        category="policy",
        matched_entities={"committee": ["body"]},
    )

    annotated = annotate_articles_with_ontology(
        [article],
        repo_name="PolicyRadar",
        sources_by_name={
            "Senate Wire": type(
                "SourceStub",
                (),
                {"config": {"event_model": "legislative_action"}},
            )(),
        },
        category_name="policy",
        attach_event_model_payload=True,
        payload_overrides_by_source={
            "Senate Wire": {
                "jurisdiction": "US",
                "action_type": "committee_advance",
                "bill_number": "Q-2026",
            }
        },
    )

    payload = annotated[0].ontology["event_model_payload"]
    assert payload == {
        "source_name": "Senate Wire",
        "headline": "Bill Q passes committee",
        "source_url": "https://gov.example/bills/Q-2026",
        "published_at": "2026-04-27T09:00:00+00:00",
        "summary": "Q advances on a 12-3 vote.",
        "jurisdiction": "US",
        "action_type": "committee_advance",
        "bill_number": "Q-2026",
    }
    # tags wasn't declared in this event_model's optional_fields -> absent
    assert "tags" not in payload


def test_annotate_articles_auto_extracts_source_config_overrides(
    tmp_path, monkeypatch
) -> None:
    """When payload_overrides_by_source isn't supplied, source.config.event_model_payload_overrides wins."""
    from datetime import UTC, datetime

    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "PolicyRadar",
        "category": "policy",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"legislative_action": "policy.legislative_action"},
        "event_model_field_specs": {
            "legislative_action": {
                "ontology_id": "policy.legislative_action",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": [
                    "published_at",
                    "summary",
                    "jurisdiction",
                    "action_type",
                    "bill_number",
                ],
                "field_enums": {
                    "jurisdiction": ["KR", "US", "EU", "JP", "CN", "GB", "INTL", "OTHER"],
                    "action_type": ["introduced", "committee_advance", "passed"],
                },
            }
        },
    }
    (runtime_dir / "PolicyRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    article = Article(
        title="Bill Q passes",
        link="https://gov.example/bills/Q",
        summary="Q advances.",
        published=datetime(2026, 4, 27, tzinfo=UTC),
        source="Senate Wire",
        category="policy",
    )
    source_stub = type(
        "SourceStub",
        (),
        {
            "config": {
                "event_model": "legislative_action",
                "event_model_payload_overrides": {
                    "jurisdiction": "US",
                    "action_type": "passed",
                },
            }
        },
    )()

    annotated = annotate_articles_with_ontology(
        [article],
        repo_name="PolicyRadar",
        sources_by_name={"Senate Wire": source_stub},
        category_name="policy",
        attach_event_model_payload=True,
    )
    payload = annotated[0].ontology["event_model_payload"]
    assert payload["jurisdiction"] == "US"
    assert payload["action_type"] == "passed"
    assert payload["headline"] == "Bill Q passes"


def test_annotate_articles_explicit_overrides_supersede_source_config(
    tmp_path, monkeypatch
) -> None:
    """When payload_overrides_by_source IS supplied, it overrides the auto-extract path."""
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "PolicyRadar",
        "category": "policy",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"legislative_action": "policy.legislative_action"},
        "event_model_field_specs": {
            "legislative_action": {
                "ontology_id": "policy.legislative_action",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": ["jurisdiction"],
                "field_enums": {"jurisdiction": ["KR", "US", "EU"]},
            }
        },
    }
    (runtime_dir / "PolicyRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    article = Article(
        title="t",
        link="https://x.example/",
        summary="s",
        published=None,
        source="Wire",
        category="policy",
    )
    source_stub = type(
        "SourceStub",
        (),
        {
            "config": {
                "event_model": "legislative_action",
                "event_model_payload_overrides": {"jurisdiction": "KR"},
            }
        },
    )()
    annotated = annotate_articles_with_ontology(
        [article],
        repo_name="PolicyRadar",
        sources_by_name={"Wire": source_stub},
        category_name="policy",
        attach_event_model_payload=True,
        payload_overrides_by_source={"Wire": {"jurisdiction": "US"}},
    )
    # Explicit map wins over source.config
    assert annotated[0].ontology["event_model_payload"]["jurisdiction"] == "US"


def test_annotate_articles_enum_violations_collects_without_raising(
    tmp_path, monkeypatch
) -> None:
    """When enum_violations list is supplied, bad enum values are collected, payload skipped, no raise."""
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "CryptoRadar",
        "category": "crypto",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"market_commentary": "crypto.market_commentary"},
        "event_model_field_specs": {
            "market_commentary": {
                "ontology_id": "crypto.market_commentary",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": ["sentiment"],
                "field_enums": {"sentiment": ["bullish", "bearish", "neutral", "mixed"]},
            }
        },
    }
    (runtime_dir / "CryptoRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    valid_article = Article(
        title="OK",
        link="https://x.example/ok",
        summary="s",
        published=None,
        source="WireA",
        category="crypto",
    )
    bad_article = Article(
        title="Bad",
        link="https://x.example/bad",
        summary="s",
        published=None,
        source="WireB",
        category="crypto",
    )
    valid_source = type(
        "Stub",
        (),
        {
            "config": {
                "event_model": "market_commentary",
                "event_model_payload_overrides": {"sentiment": "bullish"},
            }
        },
    )()
    bad_source = type(
        "Stub",
        (),
        {
            "config": {
                "event_model": "market_commentary",
                "event_model_payload_overrides": {"sentiment": "ecstatic"},
            }
        },
    )()
    violations: list[dict] = []
    annotate_articles_with_ontology(
        [valid_article, bad_article],
        repo_name="CryptoRadar",
        sources_by_name={"WireA": valid_source, "WireB": bad_source},
        category_name="crypto",
        attach_event_model_payload=True,
        enum_violations=violations,
    )
    # valid one carries payload
    assert valid_article.ontology.get("event_model_payload", {}).get("sentiment") == "bullish"
    # bad one has metadata but no payload
    assert "event_model_payload" not in bad_article.ontology
    # violation captured
    assert len(violations) == 1
    v = violations[0]
    assert v["repo"] == "CryptoRadar"
    assert v["source_name"] == "WireB"
    assert v["field_name"] == "sentiment"
    assert v["value"] == "ecstatic"
    assert v["article_link"] == "https://x.example/bad"


def test_backfill_enum_violations_collects_without_raising(tmp_path, monkeypatch) -> None:
    import duckdb

    from radar_core.ontology import backfill_duckdb_ontology

    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "PolicyRadar",
        "category": "policy",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"editorial_coverage": "policy.editorial_coverage"},
        "event_model_field_specs": {
            "editorial_coverage": {
                "ontology_id": "policy.editorial_coverage",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": ["source_kind"],
                "field_enums": {
                    "source_kind": ["news", "blog", "press_release", "academic", "official"]
                },
            }
        },
    }
    (runtime_dir / "PolicyRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    db_path = tmp_path / "policy.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE articles (link VARCHAR PRIMARY KEY, source VARCHAR, title VARCHAR, summary VARCHAR, published TIMESTAMP, entities_json VARCHAR, ontology_json VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO articles VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("https://gov.example/a", "Wire", "ok title", "s", None, None, None),
                ("https://gov.example/b", "Wire", "bad title", "s", None, None, None),
            ],
        )
    sources = {
        "Wire": type(
            "Stub", (), {"config": {"event_model": "editorial_coverage"}}
        )()
    }
    violations: list[dict] = []
    counts = backfill_duckdb_ontology(
        db_path,
        repo_name="PolicyRadar",
        sources_by_name=sources,
        category_name="policy",
        attach_event_model_payload=True,
        # pass overrides only for the second link with a bad enum value
        payload_overrides_by_source={"Wire": {"source_kind": "rumor"}},
        enum_violations=violations,
    )
    # Both rows hit the bad override -> both report violations, neither gets payload
    assert counts["with_event_model_payload"] == 0
    assert len(violations) == 2
    assert all(v["field_name"] == "source_kind" and v["value"] == "rumor" for v in violations)
    assert {v["article_link"] for v in violations} == {
        "https://gov.example/a",
        "https://gov.example/b",
    }


def test_annotate_articles_skips_payload_by_default(tmp_path, monkeypatch) -> None:
    runtime_dir = tmp_path / "runtime_contracts"
    runtime_dir.mkdir(parents=True)
    contract = {
        "repo": "PolicyRadar",
        "category": "policy",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"legislative_action": "policy.legislative_action"},
        "event_model_field_specs": {
            "legislative_action": {
                "ontology_id": "policy.legislative_action",
                "required_fields": ["source_name", "headline", "source_url"],
                "optional_fields": ["published_at", "summary"],
            }
        },
    }
    (runtime_dir / "PolicyRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))

    article = Article(
        title="t",
        link="https://x.example/",
        summary="s",
        published=None,
        source="Wire",
        category="policy",
    )
    annotated = annotate_articles_with_ontology(
        [article],
        repo_name="PolicyRadar",
        sources_by_name={
            "Wire": type(
                "SourceStub",
                (),
                {"config": {"event_model": "legislative_action"}},
            )(),
        },
        category_name="policy",
    )
    # default=False -> no payload attached
    assert "event_model_payload" not in annotated[0].ontology


# ── Cycle 11: dict-aware build_event_model_payload (option C) ────────────


def _write_home_transaction_contract(runtime_dir, monkeypatch) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    contract = {
        "repo": "HomeRadar",
        "category": "home",
        "ontology_version": "0.1.0",
        "event_model_mappings": {"transaction_price": "home.transaction_price"},
        "event_model_field_specs": {
            "transaction_price": {
                "ontology_id": "home.transaction_price",
                "required_fields": ["lawd_cd", "deal_date", "source_url"],
                "optional_fields": [],
                "field_enums": {},
            }
        },
    }
    (runtime_dir / "HomeRadar.json").write_text(
        json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    monkeypatch.setenv("RADAR_ONTOLOGY_RUNTIME_DIR", str(runtime_dir))


def test_build_event_model_payload_accepts_dict_article(
    tmp_path, monkeypatch
) -> None:
    """Plain dict (Mapping) input is converted via SimpleNamespace and yields
    the same canonical payload as an attribute-bearing object."""
    runtime_dir = tmp_path / "runtime_contracts"
    _write_home_transaction_contract(runtime_dir, monkeypatch)

    dict_article = {
        "lawd_cd": "11680",
        "deal_date": "2026-04-15",
        "source_url": "https://land.example/deals/9",
    }
    payload = build_event_model_payload(
        dict_article,
        repo_name="HomeRadar",
        event_model_key="transaction_price",
    )
    assert payload == {
        "lawd_cd": "11680",
        "deal_date": "2026-04-15",
        "source_url": "https://land.example/deals/9",
    }


def test_build_event_model_payload_dict_input_matches_object_input(
    tmp_path, monkeypatch
) -> None:
    """Mapping input must produce exactly the same payload as an object input
    carrying the identical attributes — no path divergence."""
    runtime_dir = tmp_path / "runtime_contracts"
    _write_home_transaction_contract(runtime_dir, monkeypatch)

    data = {
        "lawd_cd": "41135",
        "deal_date": "2026-04-20",
        "source_url": "https://land.example/deals/77",
    }
    listing = type("Listing", (), dict(data))()

    payload_from_dict = build_event_model_payload(
        data,
        repo_name="HomeRadar",
        event_model_key="transaction_price",
    )
    payload_from_object = build_event_model_payload(
        listing,
        repo_name="HomeRadar",
        event_model_key="transaction_price",
    )
    assert payload_from_dict == payload_from_object
    assert payload_from_dict == data


def test_build_event_model_payload_dict_input_overrides_win(
    tmp_path, monkeypatch
) -> None:
    """Overrides must still take precedence when the article is a dict."""
    runtime_dir = tmp_path / "runtime_contracts"
    _write_home_transaction_contract(runtime_dir, monkeypatch)

    dict_article = {
        "lawd_cd": "11680",
        "deal_date": "2026-04-15",
        "source_url": "https://land.example/deals/9",
    }
    payload = build_event_model_payload(
        dict_article,
        repo_name="HomeRadar",
        event_model_key="transaction_price",
        overrides={"deal_date": "2026-04-20"},
    )
    assert payload["deal_date"] == "2026-04-20"
    assert payload["lawd_cd"] == "11680"
    assert payload["source_url"] == "https://land.example/deals/9"


def test_build_event_model_payload_dict_input_canonical_extractors(
    tmp_path, monkeypatch
) -> None:
    """Dict input wired via canonical Article-shaped keys (source/title/link/summary)
    must hit the DEFAULT_FIELD_EXTRACTORS path and populate
    source_name/headline/source_url/summary."""
    runtime_dir = tmp_path / "runtime_contracts"
    _write_editorial_contract(runtime_dir, monkeypatch)

    dict_article = {
        "source": "YTN",
        "title": "T",
        "link": "https://x.example/news/1",
        "summary": "S",
    }
    payload = build_event_model_payload(
        dict_article,
        repo_name="ArtRadar",
        event_model_key="editorial_coverage",
    )
    assert payload == {
        "source_name": "YTN",
        "headline": "T",
        "source_url": "https://x.example/news/1",
        "summary": "S",
    }


def test_validate_article_ontology_accepts_empty_or_legacy_payload() -> None:
    from radar_core.ontology import validate_article_ontology

    contract = {
        "event_model_mappings": {"editorial_coverage": "art.editorial_coverage"},
        "source_role_mappings": {"YTN": "primary_evidence"},
        "event_model_field_specs": {
            "editorial_coverage": {
                "ontology_id": "art.editorial_coverage",
                "required_fields": ["source_url"],
            }
        },
    }
    # Empty ontology dict -> no validation errors (legacy emitters tolerated).
    assert validate_article_ontology({}, contract=contract) == []
    assert validate_article_ontology(None, contract=contract) == []


def test_validate_article_ontology_rejects_unknown_event_model_id() -> None:
    from radar_core.ontology import validate_article_ontology

    contract = {
        "event_model_mappings": {"editorial_coverage": "art.editorial_coverage"},
        "source_role_mappings": {"YTN": "primary_evidence"},
        "event_model_field_specs": {},
    }
    errors = validate_article_ontology(
        {"event_model_id": "art.UNKNOWN_event"}, contract=contract
    )
    assert any("event_model_id" in e for e in errors)


def test_validate_article_ontology_rejects_unknown_source_role_id() -> None:
    from radar_core.ontology import validate_article_ontology

    contract = {
        "event_model_mappings": {},
        "source_role_mappings": {"YTN": "primary_evidence"},
    }
    errors = validate_article_ontology(
        {"source_role_id": "UNREGISTERED_ROLE"}, contract=contract
    )
    assert any("source_role_id" in e for e in errors)


def test_validate_article_ontology_flags_missing_required_payload_fields() -> None:
    from radar_core.ontology import validate_article_ontology

    contract = {
        "event_model_mappings": {"editorial_coverage": "art.editorial_coverage"},
        "source_role_mappings": {},
        "event_model_field_specs": {
            "editorial_coverage": {
                "ontology_id": "art.editorial_coverage",
                "required_fields": ["source_url", "headline"],
            }
        },
    }
    errors = validate_article_ontology(
        {
            "event_model_id": "art.editorial_coverage",
            "event_model_payload": {"source_url": "https://x.example/1"},
        },
        contract=contract,
    )
    assert any("headline" in e for e in errors)


def test_validate_article_ontology_passes_complete_payload() -> None:
    from radar_core.ontology import validate_article_ontology

    contract = {
        "event_model_mappings": {"editorial_coverage": "art.editorial_coverage"},
        "source_role_mappings": {"YTN": "primary_evidence"},
        "event_model_field_specs": {
            "editorial_coverage": {
                "ontology_id": "art.editorial_coverage",
                "required_fields": ["source_url", "headline"],
            }
        },
    }
    assert validate_article_ontology(
        {
            "event_model_id": "art.editorial_coverage",
            "source_role_id": "primary_evidence",
            "event_model_payload": {
                "source_url": "https://x.example/1",
                "headline": "A title",
            },
        },
        contract=contract,
    ) == []
