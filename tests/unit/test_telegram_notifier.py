from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import requests

from radar_core.models import TelegramSettings
from radar_core.notifier import NotificationPayload
from radar_core.telegram_notifier import TelegramNotifier


def _payload(category_name: str = "tech") -> NotificationPayload:
    return NotificationPayload(
        category_name=category_name,
        sources_count=10,
        collected_count=25,
        matched_count=12,
        errors_count=1,
        timestamp=datetime(2026, 3, 14, 9, 0, tzinfo=UTC),
        report_url="https://example.com/report",
    )


def _notifier() -> TelegramNotifier:
    settings = TelegramSettings(bot_token="token-123", chat_id="chat-456")
    return TelegramNotifier(settings)


def test_send_success_returns_true() -> None:
    notifier = _notifier()
    payload = _payload()
    mock_response = Mock()
    mock_response.raise_for_status = Mock()

    with patch(
        "radar_core.telegram_notifier.requests.post", return_value=mock_response
    ) as mock_post:
        result = notifier.send(payload)

    assert result is True
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args.kwargs
    assert call_kwargs["timeout"] == 10
    assert call_kwargs["json"]["chat_id"] == "chat-456"
    assert call_kwargs["json"]["parse_mode"] == "Markdown"


def test_send_http_error_returns_false() -> None:
    notifier = _notifier()
    payload = _payload()

    response = Mock(status_code=500, headers={})
    error = requests.HTTPError("server failure")
    error.response = response

    mock_response = Mock()
    mock_response.raise_for_status.side_effect = error

    with (
        patch("radar_core.telegram_notifier.requests.post", return_value=mock_response),
        patch("radar_core.telegram_notifier.logger") as mock_logger,
    ):
        result = notifier.send(payload)

    assert result is False
    mock_logger.error.assert_called_once()


def test_send_429_logs_retry_after() -> None:
    notifier = _notifier()
    payload = _payload()

    response = Mock(status_code=429, headers={"Retry-After": "30"})
    error = requests.HTTPError("rate limited")
    error.response = response

    mock_response = Mock()
    mock_response.raise_for_status.side_effect = error

    with (
        patch("radar_core.telegram_notifier.requests.post", return_value=mock_response),
        patch("radar_core.telegram_notifier.logger") as mock_logger,
    ):
        result = notifier.send(payload)

    assert result is False
    mock_logger.warning.assert_called_once_with(
        "telegram_rate_limit",
        category=payload.category_name,
        retry_after="30",
    )


def test_message_truncation_at_4096() -> None:
    notifier = _notifier()
    payload = _payload(category_name="x" * 5000)

    message = notifier._format_message(payload)

    assert len(message) == 4096
    assert message.endswith("...")


def test_markdown_formatting() -> None:
    notifier = _notifier()
    payload = _payload("policy")

    message = notifier._format_message(payload)

    assert "*Radar Pipeline Completion Report*" in message
    assert "*Category:* policy" in message
    assert "*Statistics:*" in message
    assert "[Report](https://example.com/report)" in message


def test_timeout_on_request() -> None:
    notifier = _notifier()
    payload = _payload()

    with (
        patch(
            "radar_core.telegram_notifier.requests.post",
            side_effect=requests.Timeout("request timed out"),
        ),
        patch("radar_core.telegram_notifier.logger") as mock_logger,
    ):
        result = notifier.send(payload)

    assert result is False
    mock_logger.error.assert_called_once()
