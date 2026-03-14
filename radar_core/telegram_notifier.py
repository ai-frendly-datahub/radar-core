from __future__ import annotations

import requests
import structlog

from radar_core.models import TelegramSettings
from radar_core.notifier import NotificationPayload


logger = structlog.get_logger(__name__)


class TelegramNotifier:
    """Send notifications via Telegram Bot API."""

    def __init__(self, settings: TelegramSettings) -> None:
        self.settings = settings
        self.api_url = f"https://api.telegram.org/bot{settings.bot_token}/sendMessage"

    def send(self, payload: NotificationPayload) -> bool:
        """Send notification via Telegram Bot API.

        Args:
            payload: Notification payload with category and statistics.

        Returns:
            True if notification sent successfully, False otherwise.
        """
        try:
            message = self._format_message(payload)
            response = requests.post(
                self.api_url,
                json={
                    "chat_id": self.settings.chat_id,
                    "text": message,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
            response.raise_for_status()
            logger.info("telegram_notification_sent", category=payload.category_name)
            return True

        except requests.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = e.response.headers.get("Retry-After")
                logger.warning(
                    "telegram_rate_limit",
                    category=payload.category_name,
                    retry_after=retry_after,
                )
            else:
                logger.error(
                    "telegram_notification_failed",
                    category=payload.category_name,
                    status_code=e.response.status_code,
                    error=str(e),
                )
            return False

        except Exception as e:
            logger.error(
                "telegram_notification_failed",
                category=payload.category_name,
                error=str(e),
            )
            return False

    def _format_message(self, payload: NotificationPayload) -> str:
        """Format notification as Markdown message.

        Args:
            payload: Notification payload.

        Returns:
            Formatted message string (truncated to 4096 chars if needed).
        """
        lines = [
            "*Radar Pipeline Completion Report*",
            "",
            f"*Category:* {payload.category_name}",
            f"*Timestamp:* {payload.timestamp.isoformat()}",
            "",
            "*Statistics:*",
            f"  Sources: {payload.sources_count}",
            f"  Collected: {payload.collected_count}",
            f"  Matched: {payload.matched_count}",
            f"  Errors: {payload.errors_count}",
        ]

        if payload.report_url:
            lines.append("")
            lines.append(f"[Report]({payload.report_url})")

        message = "\n".join(lines)

        # Truncate to Telegram's 4096 character limit
        if len(message) > 4096:
            message = message[:4093] + "..."

        return message
