"""通知システム。

Slack Webhook / SMTP Email / コンソールログの3チャネルで通知を送信する。
"""

import json
import smtplib
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from typing import Any

from loguru import logger


@dataclass
class NotificationConfig:
    """通知設定。"""

    # Slack
    slack_webhook_url: str = ""
    slack_channel: str = ""

    # Email (SMTP)
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    email_from: str = ""
    email_to: list[str] = field(default_factory=list)

    # 通知レベル閾値
    min_level: str = "INFO"  # DEBUG / INFO / WARNING / ERROR


class Notifier:
    """マルチチャネル通知クラス。

    Slack Webhook、SMTP Email、コンソールログの3チャネルをサポート。
    設定されていないチャネルはスキップされる。
    """

    LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}

    def __init__(self, config: NotificationConfig | None = None) -> None:
        self._config = config or NotificationConfig()

    def send(
        self,
        title: str,
        message: str,
        level: str = "INFO",
        data: dict[str, Any] | None = None,
    ) -> dict[str, bool]:
        """通知を送信する。

        Args:
            title: 通知タイトル
            message: 本文
            level: ログレベル (DEBUG/INFO/WARNING/ERROR)
            data: 追加データ（JSON化して付加）

        Returns:
            チャネルごとの送信結果 {"slack": bool, "email": bool, "console": bool}
        """
        min_lvl = self.LEVEL_ORDER.get(self._config.min_level, 1)
        cur_lvl = self.LEVEL_ORDER.get(level, 1)
        if cur_lvl < min_lvl:
            return {"slack": False, "email": False, "console": False}

        results = {
            "console": self._send_console(title, message, level, data),
            "slack": self._send_slack(title, message, level, data),
            "email": self._send_email(title, message, level, data),
        }
        return results

    def notify_bet_result(self, bet_summary: dict[str, Any]) -> dict[str, bool]:
        """ベット結果を通知する。"""
        total = bet_summary.get("total_bets", 0)
        wins = bet_summary.get("wins", 0)
        pnl = bet_summary.get("pnl", 0)
        title = f"ベット結果: {wins}/{total}的中"
        message = (
            f"投票数: {total}\n"
            f"的中数: {wins}\n"
            f"損益: {pnl:+,}円\n"
        )
        level = "INFO" if pnl >= 0 else "WARNING"
        return self.send(title, message, level, bet_summary)

    def notify_sync_result(self, sync_result: dict[str, Any]) -> dict[str, bool]:
        """同期結果を通知する。"""
        status = sync_result.get("status", "UNKNOWN")
        added = sync_result.get("records_added", 0)
        title = f"データ同期: {status}"
        message = f"ステータス: {status}\n追加レコード: {added:,}件"
        level = "INFO" if status == "SUCCESS" else "WARNING"
        return self.send(title, message, level, sync_result)

    def notify_value_bets(
        self,
        race_name: str,
        value_bets: list[dict[str, Any]],
    ) -> dict[str, bool]:
        """バリューベット検出を通知する。"""
        if not value_bets:
            return {"slack": False, "email": False, "console": False}

        title = f"バリューベット検出: {race_name} ({len(value_bets)}頭)"
        lines = [f"レース: {race_name}", ""]
        for vb in value_bets[:5]:
            lines.append(
                f"  馬番{vb.get('umaban', '?')}: "
                f"EV={vb.get('expected_value', 0):.3f} "
                f"オッズ={vb.get('actual_odds', 0):.1f} "
                f"推定確率={vb.get('estimated_prob', 0):.1%}"
            )
        return self.send(title, "\n".join(lines), "INFO")

    def _send_console(
        self, title: str, message: str, level: str, data: dict[str, Any] | None
    ) -> bool:
        """コンソール（loguru）に通知する。"""
        log_msg = f"[通知] {title}\n{message}"
        if data:
            log_msg += f"\ndata={json.dumps(data, ensure_ascii=False, default=str)[:500]}"

        log_fn = {
            "DEBUG": logger.debug,
            "INFO": logger.info,
            "WARNING": logger.warning,
            "ERROR": logger.error,
        }.get(level, logger.info)
        log_fn(log_msg)
        return True

    def _send_slack(
        self, title: str, message: str, level: str, data: dict[str, Any] | None
    ) -> bool:
        """Slack Webhookで通知する。"""
        url = self._config.slack_webhook_url
        if not url:
            return False

        emoji_map = {
            "DEBUG": ":mag:", "INFO": ":white_check_mark:",
            "WARNING": ":warning:", "ERROR": ":x:",
        }
        emoji = emoji_map.get(level, ":bell:")
        payload = {
            "text": f"{emoji} *{title}*\n```{message}```",
        }
        if self._config.slack_channel:
            payload["channel"] = self._config.slack_channel

        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status == 200
        except (urllib.error.URLError, Exception) as e:
            logger.error(f"Slack通知エラー: {e}")
            return False

    def _send_email(
        self, title: str, message: str, level: str, data: dict[str, Any] | None
    ) -> bool:
        """SMTP Emailで通知する。"""
        cfg = self._config
        if not cfg.smtp_host or not cfg.email_to:
            return False

        try:
            body = message
            if data:
                body += f"\n\n---\n{json.dumps(data, ensure_ascii=False, indent=2, default=str)[:2000]}"

            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = f"[競馬分析] {title}"
            msg["From"] = cfg.email_from or cfg.smtp_user
            msg["To"] = ", ".join(cfg.email_to)

            with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=15) as server:
                server.starttls()
                if cfg.smtp_user and cfg.smtp_password:
                    server.login(cfg.smtp_user, cfg.smtp_password)
                server.send_message(msg)
            return True
        except Exception as e:
            logger.error(f"Email通知エラー: {e}")
            return False
