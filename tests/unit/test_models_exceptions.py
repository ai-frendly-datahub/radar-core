from __future__ import annotations

from pathlib import Path

from radar_core.exceptions import NotificationError, RadarError, ReportError
from radar_core.models import (
    EmailSettings,
    NotificationConfig,
    RadarSettings,
    TelegramSettings,
)


def test_radar_settings_stores_paths() -> None:
    settings = RadarSettings(
        database_path=Path("data/radar.duckdb"),
        report_dir=Path("reports"),
        raw_data_dir=Path("data/raw"),
        search_db_path=Path("data/search.db"),
    )

    assert settings.database_path == Path("data/radar.duckdb")
    assert settings.report_dir == Path("reports")
    assert settings.raw_data_dir == Path("data/raw")
    assert settings.search_db_path == Path("data/search.db")


def test_email_settings_stores_smtp_and_recipients() -> None:
    email = EmailSettings(
        smtp_host="smtp.example.com",
        smtp_port=587,
        username="user",
        password="pass",
        from_address="from@example.com",
        to_addresses=["to1@example.com", "to2@example.com"],
    )

    assert email.smtp_host == "smtp.example.com"
    assert email.smtp_port == 587
    assert email.username == "user"
    assert email.password == "pass"
    assert email.from_address == "from@example.com"
    assert email.to_addresses == ["to1@example.com", "to2@example.com"]


def test_telegram_settings_stores_bot_config() -> None:
    telegram = TelegramSettings(bot_token="bot-token", chat_id="chat-id")

    assert telegram.bot_token == "bot-token"
    assert telegram.chat_id == "chat-id"


def test_notification_config_supports_email_and_telegram_channels() -> None:
    email = EmailSettings(
        smtp_host="smtp.example.com",
        smtp_port=587,
        username="user",
        password="pass",
        from_address="from@example.com",
        to_addresses=["to@example.com"],
    )
    telegram = TelegramSettings(bot_token="bot-token", chat_id="chat-id")
    config = NotificationConfig(
        enabled=True,
        channels=["email", "telegram"],
        email=email,
        telegram=telegram,
    )

    assert config.enabled is True
    assert config.channels == ["email", "telegram"]
    assert config.email == email
    assert config.telegram == telegram
    assert config.webhook_url is None
    assert config.rules == {}


def test_report_error_is_radar_error() -> None:
    error = ReportError("report failed")

    assert isinstance(error, ReportError)
    assert isinstance(error, RadarError)
    assert str(error) == "report failed"


def test_notification_error_is_radar_error() -> None:
    error = NotificationError("notify failed")

    assert isinstance(error, NotificationError)
    assert isinstance(error, RadarError)
    assert str(error) == "notify failed"
