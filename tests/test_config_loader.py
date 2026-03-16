"""Tests for config_loader — YAML loading, path resolution, parsing."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from radar_core.config_loader import (
    _dict_items,
    _parse_entity,
    _parse_source,
    _read_yaml_dict,
    _resolve_env_refs,
    _resolve_path,
    _string_value,
    load_category_config,
    load_notification_config,
    load_settings,
)
from radar_core.models import CategoryConfig, NotificationConfig, RadarSettings


# ── Helpers ───────────────────────────────────────────────────────────────


def _write_yaml(path: Path, data: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data, allow_unicode=True), encoding="utf-8")
    return path


# ── load_settings ─────────────────────────────────────────────────────────


def test_load_settings_from_yaml(tmp_path: Path) -> None:
    """유효한 config.yaml에서 RadarSettings 로딩."""
    config_data = {
        "database_path": "/data/radar.duckdb",
        "report_dir": "/reports",
        "raw_data_dir": "/data/raw",
        "search_db_path": "/data/search.db",
    }
    config_file = _write_yaml(tmp_path / "config.yaml", config_data)

    settings = load_settings(config_file)

    assert isinstance(settings, RadarSettings)
    assert settings.database_path == Path("/data/radar.duckdb")
    assert settings.report_dir == Path("/reports")


def test_load_settings_missing_file_raises() -> None:
    """존재하지 않는 config 파일 → FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_settings(Path("/nonexistent/config.yaml"))


def test_load_settings_defaults_for_missing_keys(tmp_path: Path) -> None:
    """누락된 키는 기본값으로 대체."""
    config_file = _write_yaml(tmp_path / "config.yaml", {})

    settings = load_settings(config_file)

    assert isinstance(settings, RadarSettings)
    # 기본값 경로가 존재해야 함
    assert settings.database_path is not None
    assert settings.report_dir is not None


# ── load_category_config ──────────────────────────────────────────────────


def test_load_category_config_from_yaml(tmp_path: Path) -> None:
    """카테고리 YAML에서 CategoryConfig 로딩."""
    cat_dir = tmp_path / "categories"
    _write_yaml(
        cat_dir / "game.yaml",
        {
            "category_name": "game",
            "display_name": "Game Radar",
            "sources": [
                {"name": "IGN", "type": "rss", "url": "https://ign.com/feed"},
            ],
            "entities": [
                {
                    "name": "nintendo",
                    "display_name": "Nintendo",
                    "keywords": ["닌텐도", "nintendo"],
                },
            ],
        },
    )

    config = load_category_config("game", categories_dir=cat_dir)

    assert isinstance(config, CategoryConfig)
    assert config.category_name == "game"
    assert config.display_name == "Game Radar"
    assert len(config.sources) == 1
    assert config.sources[0].name == "IGN"
    assert len(config.entities) == 1
    assert "닌텐도" in config.entities[0].keywords


def test_load_category_config_missing_file_raises(tmp_path: Path) -> None:
    """존재하지 않는 카테고리 파일 → FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_category_config("nonexistent", categories_dir=tmp_path)


def test_load_category_config_uses_name_as_display_fallback(tmp_path: Path) -> None:
    """display_name 누락 시 category_name으로 fallback."""
    cat_dir = tmp_path / "categories"
    _write_yaml(cat_dir / "tech.yaml", {"sources": [], "entities": []})

    config = load_category_config("tech", categories_dir=cat_dir)

    assert config.display_name == "tech"  # fallback to category_name arg


# ── load_notification_config ──────────────────────────────────────────────


def test_load_notification_config_missing_file_returns_disabled(
    tmp_path: Path,
) -> None:
    """알림 설정 파일 없으면 disabled NotificationConfig 반환."""
    config = load_notification_config(tmp_path / "nonexistent.yaml")

    assert isinstance(config, NotificationConfig)
    assert config.enabled is False
    assert config.channels == []


def test_load_notification_config_with_email(tmp_path: Path) -> None:
    """이메일 알림 설정 로딩."""
    _write_yaml(
        tmp_path / "notifications.yaml",
        {
            "notifications": {
                "enabled": True,
                "channels": ["email"],
                "email": {
                    "smtp_host": "smtp.example.com",
                    "smtp_port": 587,
                    "username": "user",
                    "password": "pass",
                    "from_address": "from@ex.com",
                    "to_addresses": ["to@ex.com"],
                },
            }
        },
    )

    config = load_notification_config(tmp_path / "notifications.yaml")

    assert config.enabled is True
    assert "email" in config.channels
    assert config.email is not None
    assert config.email.smtp_host == "smtp.example.com"


def test_load_notification_config_with_telegram(tmp_path: Path) -> None:
    """텔레그램 알림 설정 로딩."""
    _write_yaml(
        tmp_path / "notifications.yaml",
        {
            "notifications": {
                "enabled": True,
                "channels": ["telegram"],
                "telegram": {
                    "bot_token": "123:ABC",
                    "chat_id": "-100123",
                },
            }
        },
    )

    config = load_notification_config(tmp_path / "notifications.yaml")

    assert config.telegram is not None
    assert config.telegram.bot_token == "123:ABC"
    assert config.telegram.chat_id == "-100123"


# ── Helper functions ──────────────────────────────────────────────────────


def test_resolve_path_absolute() -> None:
    """절대 경로는 그대로 반환."""
    result = _resolve_path("/absolute/path", project_root=Path("/root"))
    assert result == Path("/absolute/path")


def test_resolve_path_relative(tmp_path: Path) -> None:
    """상대 경로는 project_root 기준으로 resolve."""
    result = _resolve_path("data/db.duckdb", project_root=tmp_path)
    assert result == (tmp_path / "data/db.duckdb").resolve()


def test_read_yaml_dict_valid(tmp_path: Path) -> None:
    """유효한 YAML dict 읽기."""
    path = _write_yaml(tmp_path / "test.yaml", {"key": "value", "num": 42})
    result = _read_yaml_dict(path)

    assert result == {"key": "value", "num": 42}


def test_read_yaml_dict_non_dict_returns_empty(tmp_path: Path) -> None:
    """YAML이 dict가 아닌 경우 빈 dict 반환."""
    path = tmp_path / "list.yaml"
    path.write_text("- item1\n- item2\n", encoding="utf-8")

    result = _read_yaml_dict(path)
    assert result == {}


def test_string_value_returns_existing() -> None:
    """존재하는 문자열 키 반환."""
    assert _string_value({"key": "val"}, "key", "default") == "val"


def test_string_value_returns_default_for_missing() -> None:
    """누락된 키 → 기본값."""
    assert _string_value({}, "key", "default") == "default"


def test_string_value_returns_default_for_empty() -> None:
    """빈 문자열 → 기본값."""
    assert _string_value({"key": "  "}, "key", "default") == "default"


def test_dict_items_from_list() -> None:
    """list[dict] → list[dict[str, object]]로 변환."""
    raw = [{"name": "a"}, {"name": "b"}]
    result = _dict_items(raw)

    assert len(result) == 2
    assert result[0]["name"] == "a"


def test_dict_items_non_list_returns_empty() -> None:
    """list가 아닌 값 → 빈 리스트."""
    assert _dict_items("not a list") == []
    assert _dict_items(None) == []
    assert _dict_items(42) == []


def test_parse_source_valid() -> None:
    """유효한 source dict 파싱."""
    source = _parse_source({"name": "IGN", "type": "rss", "url": "https://ign.com"})
    assert source.name == "IGN"
    assert source.type == "rss"


def test_parse_source_empty_raises() -> None:
    """빈 dict → ValueError."""
    with pytest.raises(ValueError, match="Empty source"):
        _parse_source({})


def test_parse_entity_valid() -> None:
    """유효한 entity dict 파싱."""
    entity = _parse_entity(
        {"name": "topic", "display_name": "Topic", "keywords": ["ai", "ml"]}
    )
    assert entity.name == "topic"
    assert entity.keywords == ["ai", "ml"]


def test_parse_entity_empty_raises() -> None:
    """빈 dict → ValueError."""
    with pytest.raises(ValueError, match="Empty entity"):
        _parse_entity({})


def test_resolve_env_refs_replaces_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """${VAR} 패턴을 환경변수 값으로 치환."""
    monkeypatch.setenv("TEST_HOST", "smtp.test.com")

    result = _resolve_env_refs("host=${TEST_HOST}")
    assert result == "host=smtp.test.com"


def test_resolve_env_refs_missing_var_empty_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """없는 환경변수는 빈 문자열로 치환."""
    monkeypatch.delenv("NONEXISTENT_VAR", raising=False)

    result = _resolve_env_refs("val=${NONEXISTENT_VAR}")
    assert result == "val="


def test_resolve_env_refs_nested_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    """중첩 dict/list도 재귀적으로 치환."""
    monkeypatch.setenv("DB_HOST", "localhost")

    result = _resolve_env_refs({"host": "${DB_HOST}", "ports": ["${DB_HOST}"]})
    assert result == {"host": "localhost", "ports": ["localhost"]}
