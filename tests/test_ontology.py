from __future__ import annotations

import json

from radar_core.models import Article
from radar_core.ontology import (
    annotate_articles_with_ontology,
    build_article_ontology_metadata,
    build_summary_ontology_metadata,
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
    assert counts_again["with_event_model_id"] == 3
