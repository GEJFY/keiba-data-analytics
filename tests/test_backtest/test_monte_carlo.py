"""モンテカルロシミュレーションのテスト。"""

import pytest

from src.backtest.monte_carlo import MonteCarloResult, MonteCarloSimulator


class TestMonteCarloSimulator:
    """MonteCarloSimulatorのテスト。"""

    def test_basic_simulation(self) -> None:
        """基本的なシミュレーションが実行できること。"""
        sim = MonteCarloSimulator(seed=42)
        # 50%勝率、勝ち=+4000円、負け=-1000円
        pnls = [4000.0] * 50 + [-1000.0] * 50
        result = sim.run(pnls, n_simulations=1000)

        assert isinstance(result, MonteCarloResult)
        assert result.n_simulations == 1000
        assert result.n_bets == 100
        assert result.pnl_mean > 0  # 期待値プラスの戦略

    def test_losing_strategy(self) -> None:
        """期待値マイナスの戦略で正しくPnLがマイナスになること。"""
        sim = MonteCarloSimulator(seed=42)
        pnls = [-1000.0] * 80 + [2000.0] * 20
        result = sim.run(pnls, n_simulations=1000)
        assert result.pnl_mean < 0

    def test_ruin_probability(self) -> None:
        """破産確率が計算されること。"""
        sim = MonteCarloSimulator(seed=42)
        pnls = [-10000.0] * 90 + [50000.0] * 10
        result = sim.run(pnls, n_simulations=500, initial_bankroll=50_000)
        assert 0.0 <= result.ruin_probability <= 1.0

    def test_percentiles(self) -> None:
        """パーセンタイルが正しい順序であること。"""
        sim = MonteCarloSimulator(seed=42)
        pnls = [3000.0] * 60 + [-1000.0] * 40
        result = sim.run(pnls, n_simulations=1000)
        assert result.pnl_5th <= result.pnl_median <= result.pnl_95th

    def test_empty_pnls_raises(self) -> None:
        """空のPnLリストでValueError。"""
        sim = MonteCarloSimulator()
        with pytest.raises(ValueError, match="ベットデータが空"):
            sim.run([])

    def test_custom_n_bets(self) -> None:
        """カスタムベット数でシミュレーションできること。"""
        sim = MonteCarloSimulator(seed=42)
        pnls = [1000.0, -500.0] * 10
        result = sim.run(pnls, n_simulations=100, n_bets_per_sim=50)
        assert result.n_bets == 50

    def test_max_drawdown_calculated(self) -> None:
        """最大ドローダウンが計算されること。"""
        sim = MonteCarloSimulator(seed=42)
        pnls = [5000.0] * 30 + [-3000.0] * 70
        result = sim.run(pnls, n_simulations=500)
        assert result.max_drawdown_mean > 0
        assert result.max_drawdown_95th >= result.max_drawdown_mean

    def test_reproducible_with_seed(self) -> None:
        """同じシードで再現可能であること。"""
        pnls = [1000.0, -500.0, 2000.0, -800.0] * 25
        result1 = MonteCarloSimulator(seed=123).run(pnls, n_simulations=100)
        result2 = MonteCarloSimulator(seed=123).run(pnls, n_simulations=100)
        assert result1.pnl_mean == result2.pnl_mean
