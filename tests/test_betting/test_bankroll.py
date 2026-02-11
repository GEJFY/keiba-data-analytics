"""資金管理の単体テスト。"""

import pytest

from src.betting.bankroll import BankrollManager, BettingMethod


@pytest.mark.unit
class TestBankrollManager:
    """BankrollManagerクラスのテスト。"""

    def test_initial_balance(self) -> None:
        """初期残高が正しく設定されること。"""
        mgr = BankrollManager(initial_balance=1_000_000)
        assert mgr.current_balance == 1_000_000

    def test_equal_method_stake(self) -> None:
        """均等配分方式で投票額が算出されること。"""
        mgr = BankrollManager(
            initial_balance=1_000_000,
            method=BettingMethod.EQUAL,
        )
        stake = mgr.calculate_stake(estimated_prob=0.1, odds=10.0, fixed_rate=0.005)
        assert stake > 0
        assert stake <= 1_000_000 * 0.05  # max_per_race_rate

    def test_quarter_kelly_positive_ev(self) -> None:
        """Quarter Kelly方式で正のEVの場合に投票額が算出されること。"""
        mgr = BankrollManager(
            initial_balance=1_000_000,
            method=BettingMethod.QUARTER_KELLY,
        )
        # p=0.2, b=9.0 → Kelly = (0.2*9 - 0.8)/9 ≒ 0.111 → Quarter = 0.028
        stake = mgr.calculate_stake(estimated_prob=0.2, odds=10.0)
        assert stake > 0

    def test_quarter_kelly_negative_ev(self) -> None:
        """Quarter Kelly方式で負のEVの場合に0が返ること。"""
        mgr = BankrollManager(
            initial_balance=1_000_000,
            method=BettingMethod.QUARTER_KELLY,
        )
        # p=0.05, b=1.5 → EV < 1.0
        stake = mgr.calculate_stake(estimated_prob=0.05, odds=1.5)
        assert stake == 0

    def test_stake_rounded_to_100(self) -> None:
        """投票額が100円単位に丸められること。"""
        mgr = BankrollManager(
            initial_balance=1_000_000,
            method=BettingMethod.EQUAL,
        )
        stake = mgr.calculate_stake(estimated_prob=0.1, odds=5.0)
        assert stake % 100 == 0

    def test_drawdown_reduces_stake(self) -> None:
        """ドローダウン時に投票額が縮小されること。"""
        mgr = BankrollManager(
            initial_balance=1_000_000,
            method=BettingMethod.EQUAL,
            drawdown_cutoff=0.30,
        )
        # 残高を大きく減らしてドローダウン状態にする
        mgr.record_bet(700_000)
        # 70%のドローダウン → 投票額50%縮小
        stake = mgr.calculate_stake(estimated_prob=0.1, odds=5.0, fixed_rate=0.01)
        normal_mgr = BankrollManager(
            initial_balance=300_000,
            method=BettingMethod.EQUAL,
        )
        normal_stake = normal_mgr.calculate_stake(estimated_prob=0.1, odds=5.0, fixed_rate=0.01)
        # ドローダウン状態では通常より少ない投票額になるはず
        assert stake <= normal_stake

    def test_daily_limit(self) -> None:
        """日次投票上限が機能すること。"""
        mgr = BankrollManager(
            initial_balance=1_000_000,
            method=BettingMethod.EQUAL,
            max_daily_rate=0.01,  # 日次1% = 10,000円
        )
        # 日次上限に近い額を投票
        mgr.record_bet(9_900)
        stake = mgr.calculate_stake(estimated_prob=0.1, odds=5.0, fixed_rate=0.01)
        assert stake <= 100  # 残り100円以下

    def test_record_payout_increases_balance(self) -> None:
        """払戻で残高が増加すること。"""
        mgr = BankrollManager(initial_balance=1_000_000)
        mgr.record_bet(10_000)
        mgr.record_payout(50_000)
        assert mgr.current_balance == 1_040_000
