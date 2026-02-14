"""通知システムのテスト。"""

from unittest.mock import MagicMock, patch

from src.notifications.notifier import NotificationConfig, Notifier


class TestNotificationConfig:
    """NotificationConfigのテスト。"""

    def test_defaults(self) -> None:
        """デフォルト値の確認。"""
        cfg = NotificationConfig()
        assert cfg.slack_webhook_url == ""
        assert cfg.smtp_host == ""
        assert cfg.min_level == "INFO"
        assert cfg.email_to == []


class TestNotifier:
    """Notifierのテスト。"""

    def test_send_console_only(self) -> None:
        """コンソール通知のみの動作確認。"""
        notifier = Notifier()
        result = notifier.send("テスト", "テストメッセージ", "INFO")
        assert result["console"] is True
        assert result["slack"] is False
        assert result["email"] is False

    def test_send_below_min_level(self) -> None:
        """閾値以下のレベルは通知しないこと。"""
        cfg = NotificationConfig(min_level="ERROR")
        notifier = Notifier(cfg)
        result = notifier.send("テスト", "低レベル", "INFO")
        assert result["console"] is False

    def test_send_above_min_level(self) -> None:
        """閾値以上のレベルは通知すること。"""
        cfg = NotificationConfig(min_level="WARNING")
        notifier = Notifier(cfg)
        result = notifier.send("エラー通知", "テスト", "ERROR")
        assert result["console"] is True

    def test_notify_bet_result_positive(self) -> None:
        """プラス収支のベット結果通知。"""
        notifier = Notifier()
        result = notifier.notify_bet_result({
            "total_bets": 5,
            "wins": 2,
            "pnl": 15000,
        })
        assert result["console"] is True

    def test_notify_bet_result_negative(self) -> None:
        """マイナス収支のベット結果通知。"""
        notifier = Notifier()
        result = notifier.notify_bet_result({
            "total_bets": 5,
            "wins": 0,
            "pnl": -5000,
        })
        assert result["console"] is True

    def test_notify_sync_result(self) -> None:
        """同期結果通知。"""
        notifier = Notifier()
        result = notifier.notify_sync_result({
            "status": "SUCCESS",
            "records_added": 100,
        })
        assert result["console"] is True

    def test_notify_value_bets(self) -> None:
        """バリューベット検出通知。"""
        notifier = Notifier()
        result = notifier.notify_value_bets(
            "テストレース",
            [{"umaban": "03", "expected_value": 1.15, "actual_odds": 8.0, "estimated_prob": 0.15}],
        )
        assert result["console"] is True

    def test_notify_value_bets_empty(self) -> None:
        """バリューベットなしの場合はスキップ。"""
        notifier = Notifier()
        result = notifier.notify_value_bets("テストレース", [])
        assert result["console"] is False

    def test_slack_send_success(self) -> None:
        """Slack送信の成功ケース。"""
        cfg = NotificationConfig(slack_webhook_url="https://hooks.slack.com/test")
        notifier = Notifier(cfg)

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = notifier.send("テスト", "メッセージ", "INFO")
        assert result["slack"] is True

    def test_slack_send_failure(self) -> None:
        """Slack送信の失敗ケース。"""
        cfg = NotificationConfig(slack_webhook_url="https://hooks.slack.com/test")
        notifier = Notifier(cfg)

        with patch("urllib.request.urlopen", side_effect=Exception("network error")):
            result = notifier.send("テスト", "メッセージ", "INFO")
        assert result["slack"] is False

    def test_console_with_data(self) -> None:
        """追加データ付きコンソール通知。"""
        notifier = Notifier()
        result = notifier.send(
            "テスト", "メッセージ", "WARNING",
            data={"key": "value", "num": 123}
        )
        assert result["console"] is True
