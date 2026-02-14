"""Selenium IPATエグゼキュータのテスト。"""

from src.betting.selenium_executor import SeleniumConfig, SeleniumIPATExecutor
from src.strategy.base import Bet


class TestSeleniumConfig:
    """SeleniumConfig のテスト。"""

    def test_default_config(self) -> None:
        """デフォルト設定が正しいこと。"""
        config = SeleniumConfig()
        assert config.headless is True
        assert config.timeout == 30
        assert "ipat.jra.go.jp" in config.ipat_url

    def test_custom_config(self) -> None:
        """カスタム設定が反映されること。"""
        config = SeleniumConfig(
            inet_id="test_id",
            kanyusya_no="12345",
            password="secret",
            headless=False,
        )
        assert config.inet_id == "test_id"
        assert config.headless is False


class TestSeleniumIPATExecutor:
    """SeleniumIPATExecutor のテスト。"""

    def test_login_without_credentials(self) -> None:
        """認証情報なしでログイン失敗すること。"""
        executor = SeleniumIPATExecutor(SeleniumConfig())
        assert executor.login() is False

    def test_place_bet_without_login(self) -> None:
        """未ログインで投票失敗すること。"""
        executor = SeleniumIPATExecutor(SeleniumConfig())
        bet = Bet(
            race_key="R001", bet_type="WIN", selection="01",
            stake_yen=1000, est_prob=0.2, odds_at_bet=5.0,
            est_ev=1.0, factor_details={},
        )
        result = executor.place_bet(bet)
        assert result["success"] is False
        assert "未ログイン" in result["message"]

    def test_execute_bets_login_failure(self) -> None:
        """ログイン失敗で全ベット失敗。"""
        executor = SeleniumIPATExecutor(SeleniumConfig())
        bets = [
            Bet(
                race_key="R001", bet_type="WIN", selection="01",
                stake_yen=1000, est_prob=0.2, odds_at_bet=5.0,
                est_ev=1.0, factor_details={},
            ),
        ]
        results = executor.execute_bets(bets)
        assert len(results) == 1
        assert results[0]["success"] is False

    def test_close_without_driver(self) -> None:
        """ドライバなしでclose()がエラーにならないこと。"""
        executor = SeleniumIPATExecutor(SeleniumConfig())
        executor.close()  # エラーなし
