"""バックテストKPI計算の単体テスト。"""

import pytest

from src.backtest.metrics import BacktestMetrics, calculate_metrics
from src.strategy.base import Bet


def _make_bet(stake: int = 1000, odds: float = 5.0, est_prob: float = 0.3) -> Bet:
    """テスト用Betオブジェクトを生成する。"""
    return Bet(
        race_key="2025010106010101",
        bet_type="WIN",
        selection="01",
        stake_yen=stake,
        est_prob=est_prob,
        odds_at_bet=odds,
        est_ev=est_prob * odds,
        factor_details={"speed": 1.5},
    )


class TestCalculateMetrics:
    """calculate_metrics関数のテスト。"""

    def test_empty_bets(self) -> None:
        """空ベットリストの場合、全KPIが0であること。"""
        metrics = calculate_metrics([], initial_bankroll=1_000_000)
        assert metrics.total_stake == 0
        assert metrics.total_payout == 0
        assert metrics.pnl == 0
        assert metrics.roi == 0.0
        assert metrics.win_rate == 0.0
        assert metrics.recovery_rate == 0.0
        assert metrics.max_drawdown == 0.0
        assert metrics.sharpe_ratio == 0.0

    def test_single_bet_metrics(self) -> None:
        """1件のベットでtotal_stakeが正しく計算されること。"""
        bets = [_make_bet(stake=5000)]
        metrics = calculate_metrics(bets, initial_bankroll=1_000_000)
        assert metrics.total_stake == 5000

    def test_multiple_bets_total_stake(self) -> None:
        """複数ベットの合計stake額が正しいこと。"""
        bets = [
            _make_bet(stake=1000),
            _make_bet(stake=2000),
            _make_bet(stake=3000),
        ]
        metrics = calculate_metrics(bets, initial_bankroll=1_000_000)
        assert metrics.total_stake == 6000

    def test_roi_calculation(self) -> None:
        """ROIが正しく計算されること（pnl / total_stake）。"""
        bets = [_make_bet(stake=10000)]
        metrics = calculate_metrics(bets, initial_bankroll=1_000_000)
        # total_payout = 0 (TODO実装前), pnl = -10000
        assert metrics.pnl == -10000
        assert metrics.roi == pytest.approx(-1.0)

    def test_recovery_rate(self) -> None:
        """回収率が正しく計算されること（payout / stake）。"""
        bets = [_make_bet(stake=10000)]
        metrics = calculate_metrics(bets, initial_bankroll=1_000_000)
        # total_payout = 0なので回収率 = 0
        assert metrics.recovery_rate == 0.0


class TestBacktestMetrics:
    """BacktestMetricsデータクラスのテスト。"""

    def test_create_metrics(self) -> None:
        """BacktestMetricsが正しく生成されること。"""
        metrics = BacktestMetrics(
            total_stake=100000,
            total_payout=120000,
            pnl=20000,
            roi=0.2,
            win_rate=0.35,
            recovery_rate=1.2,
            max_drawdown=0.15,
            max_consecutive_losses=5,
            sharpe_ratio=1.5,
            profit_factor=2.0,
            monthly_win_rate=0.6,
            calmar_ratio=3.0,
        )
        assert metrics.total_stake == 100000
        assert metrics.roi == 0.2
        assert metrics.sharpe_ratio == 1.5
        assert metrics.calmar_ratio == 3.0
