"""バックテストKPI計算の単体テスト。"""

import pytest

from src.backtest.metrics import (
    BacktestMetrics,
    calculate_metrics,
    calculate_payout,
    _resolve_actual_results,
)
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
        bets = [_make_bet(stake=10000, odds=5.0, est_prob=0.3)]
        metrics = calculate_metrics(bets, initial_bankroll=1_000_000)
        # expected_payout = int(0.3 * 5.0 * 10000) = 15000
        # pnl = 15000 - 10000 = 5000
        assert metrics.pnl == 5000
        assert metrics.roi == pytest.approx(0.5)

    def test_recovery_rate(self) -> None:
        """回収率が正しく計算されること（payout / stake）。"""
        bets = [_make_bet(stake=10000, odds=5.0, est_prob=0.3)]
        metrics = calculate_metrics(bets, initial_bankroll=1_000_000)
        # expected_payout = 15000, recovery_rate = 15000/10000 = 1.5
        assert metrics.recovery_rate == pytest.approx(1.5)


class TestCalculatePayout:
    """calculate_payout関数のテスト。"""

    def test_win_hit(self) -> None:
        """単勝的中: 1着馬に単勝ベット → 払戻あり。"""
        kakutei = {"03": 1, "01": 2, "07": 3}
        payouts = {
            "tansyo": [{"selection": "03", "pay": "500", "ninki": "1"}],
            "fukusyo": [],
        }
        # stake=1000, pay=500 → 500 * (1000 // 100) = 5000
        result = calculate_payout("WIN", "03", 1000, payouts, kakutei)
        assert result == 5000

    def test_win_miss(self) -> None:
        """単勝不的中: 2着馬に単勝ベット → 0。"""
        kakutei = {"03": 1, "01": 2, "07": 3}
        payouts = {
            "tansyo": [{"selection": "03", "pay": "500", "ninki": "1"}],
        }
        result = calculate_payout("WIN", "01", 1000, payouts, kakutei)
        assert result == 0

    def test_place_hit(self) -> None:
        """複勝的中: 3着以内の馬に複勝ベット → 払戻あり。"""
        kakutei = {"03": 1, "01": 2, "07": 3}
        payouts = {
            "tansyo": [],
            "fukusyo": [
                {"selection": "03", "pay": "200", "ninki": "1"},
                {"selection": "01", "pay": "350", "ninki": "2"},
                {"selection": "07", "pay": "800", "ninki": "3"},
            ],
        }
        # 馬番07は3着 → 複勝的中, pay=800 * (1000//100) = 8000
        result = calculate_payout("PLACE", "07", 1000, payouts, kakutei)
        assert result == 8000

    def test_place_miss(self) -> None:
        """複勝不的中: 4着以下の馬に複勝ベット → 0。"""
        kakutei = {"03": 1, "01": 2, "07": 3, "05": 4}
        payouts = {
            "fukusyo": [
                {"selection": "03", "pay": "200", "ninki": "1"},
                {"selection": "01", "pay": "350", "ninki": "2"},
                {"selection": "07", "pay": "800", "ninki": "3"},
            ],
        }
        result = calculate_payout("PLACE", "05", 1000, payouts, kakutei)
        assert result == 0

    def test_unknown_bet_type(self) -> None:
        """未対応の券種 → 0。"""
        result = calculate_payout("EXACTA", "01", 1000, {}, {"01": 1})
        assert result == 0

    def test_no_matching_selection_in_payouts(self) -> None:
        """的中したが払戻データに馬番がない → 0。"""
        kakutei = {"01": 1}
        payouts = {"tansyo": [{"selection": "03", "pay": "500"}]}
        result = calculate_payout("WIN", "01", 1000, payouts, kakutei)
        assert result == 0


class TestResolveActualResults:
    """_resolve_actual_results関数のテスト。"""

    def test_win_hit_resolved(self) -> None:
        """単勝的中ベットが正しく判定されること。"""
        bets = [Bet(
            race_key="R001", bet_type="WIN", selection="03",
            stake_yen=1000, est_prob=0.2, odds_at_bet=5.0,
            est_ev=1.0, factor_details={},
        )]
        race_results = {
            "R001": {
                "kakutei": {"03": 1, "01": 2},
                "payouts": {
                    "tansyo": [{"selection": "03", "pay": "500"}],
                },
            },
        }
        results = _resolve_actual_results(bets, race_results)
        assert len(results) == 1
        assert results[0]["is_win"] is True
        assert results[0]["payout"] == 5000
        assert results[0]["pnl"] == 4000

    def test_win_miss_resolved(self) -> None:
        """単勝不的中ベットが正しく判定されること。"""
        bets = [Bet(
            race_key="R001", bet_type="WIN", selection="01",
            stake_yen=1000, est_prob=0.2, odds_at_bet=5.0,
            est_ev=1.0, factor_details={},
        )]
        race_results = {
            "R001": {
                "kakutei": {"03": 1, "01": 2},
                "payouts": {
                    "tansyo": [{"selection": "03", "pay": "500"}],
                },
            },
        }
        results = _resolve_actual_results(bets, race_results)
        assert len(results) == 1
        assert results[0]["is_win"] is False
        assert results[0]["payout"] == 0
        assert results[0]["pnl"] == -1000

    def test_unknown_race_key(self) -> None:
        """race_resultsにないレースのベット → 不的中扱い。"""
        bets = [Bet(
            race_key="R999", bet_type="WIN", selection="01",
            stake_yen=1000, est_prob=0.2, odds_at_bet=5.0,
            est_ev=1.0, factor_details={},
        )]
        results = _resolve_actual_results(bets, {})
        assert results[0]["is_win"] is False
        assert results[0]["payout"] == 0


class TestCalculateMetricsActual:
    """calculate_metricsの実績ベーステスト。"""

    def test_actual_results_used(self) -> None:
        """race_results指定時に実績ベースでKPIが算出されること。"""
        bets = [
            Bet(
                race_key="R001", bet_type="WIN", selection="03",
                stake_yen=1000, est_prob=0.2, odds_at_bet=5.0,
                est_ev=1.0, factor_details={},
            ),
            Bet(
                race_key="R001", bet_type="WIN", selection="01",
                stake_yen=1000, est_prob=0.3, odds_at_bet=3.0,
                est_ev=0.9, factor_details={},
            ),
        ]
        race_results = {
            "R001": {
                "kakutei": {"03": 1, "01": 2},
                "payouts": {
                    "tansyo": [{"selection": "03", "pay": "500"}],
                },
            },
        }
        metrics = calculate_metrics(bets, 1_000_000, race_results=race_results)
        # bet1: WIN 03, 1着 → payout=5000, pnl=+4000
        # bet2: WIN 01, 2着 → payout=0, pnl=-1000
        assert metrics.total_stake == 2000
        assert metrics.total_payout == 5000
        assert metrics.pnl == 3000
        assert metrics.win_rate == pytest.approx(0.5)

    def test_fallback_to_simulation(self) -> None:
        """race_results=Noneの場合、推定ベースで計算されること。"""
        bets = [_make_bet(stake=10000, odds=5.0, est_prob=0.3)]
        metrics = calculate_metrics(bets, 1_000_000, race_results=None)
        # 推定: int(0.3 * 5.0 * 10000) = 15000
        assert metrics.total_payout == 15000
        assert metrics.pnl == 5000


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
