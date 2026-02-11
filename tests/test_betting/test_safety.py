"""安全機構の単体テスト。"""

import pytest

from src.betting.safety import SafetyGuard


@pytest.mark.unit
class TestSafetyGuard:
    """SafetyGuardクラスのテスト。"""

    def test_can_bet_initially(self) -> None:
        """初期状態で投票可能であること。"""
        guard = SafetyGuard()
        can_bet, reason = guard.check_can_bet()
        assert can_bet is True
        assert reason == "OK"

    def test_emergency_stop_on_consecutive_losses(self) -> None:
        """連敗数超過で緊急停止すること。"""
        guard = SafetyGuard(max_consecutive_losses=3)
        for _ in range(3):
            guard.record_result(is_win=False, pnl=-1000)
        can_bet, reason = guard.check_can_bet()
        assert can_bet is False
        assert "連敗" in reason

    def test_win_resets_consecutive_losses(self) -> None:
        """勝利で連敗カウンタがリセットされること。"""
        guard = SafetyGuard(max_consecutive_losses=5)
        for _ in range(4):
            guard.record_result(is_win=False, pnl=-1000)
        guard.record_result(is_win=True, pnl=5000)
        can_bet, _ = guard.check_can_bet()
        assert can_bet is True

    def test_duplicate_bet_detection(self) -> None:
        """二重投票が検出されること。"""
        guard = SafetyGuard()
        guard.register_bet("2025010106010101", "01")
        assert guard.check_duplicate_bet("2025010106010101", "01") is True
        assert guard.check_duplicate_bet("2025010106010101", "02") is False

    def test_odds_deviation_detection(self) -> None:
        """オッズ急変が検知されること。"""
        guard = SafetyGuard(odds_deviation_threshold=0.20)
        # 30%の変動
        is_deviated, deviation = guard.check_odds_deviation(10.0, 13.0)
        assert is_deviated is True
        assert deviation == pytest.approx(0.3, abs=0.01)

    def test_odds_within_threshold(self) -> None:
        """閾値内のオッズ変動では検知されないこと。"""
        guard = SafetyGuard(odds_deviation_threshold=0.30)
        is_deviated, _ = guard.check_odds_deviation(10.0, 11.0)
        assert is_deviated is False

    def test_daily_reset(self) -> None:
        """日次リセットが正常に動作すること。"""
        guard = SafetyGuard()
        guard.register_bet("race1", "01")
        guard.record_result(is_win=False, pnl=-5000)
        guard.reset_daily()
        assert guard.check_duplicate_bet("race1", "01") is False
