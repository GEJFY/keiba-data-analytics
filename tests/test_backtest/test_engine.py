"""バックテストエンジンの単体テスト。"""

from typing import Any

import pytest

from src.backtest.engine import BacktestConfig, BacktestEngine, BacktestResult
from src.strategy.base import Bet, Strategy


class MockStrategy(Strategy):
    """テスト用の戦略実装。"""

    def __init__(self, bets_per_race: int = 1, stake: int = 1000) -> None:
        self._bets_per_race = bets_per_race
        self._stake = stake

    def name(self) -> str:
        return "mock_strategy"

    def version(self) -> str:
        return "1.0.0"

    def run(
        self,
        race_data: dict[str, Any],
        entries: list[dict[str, Any]],
        odds: dict[str, float],
        bankroll: int,
        params: dict[str, Any],
    ) -> list[Bet]:
        bets = []
        for i in range(min(self._bets_per_race, len(entries))):
            entry = entries[i] if entries else {}
            umaban = entry.get("Umaban", str(i + 1))
            bets.append(
                Bet(
                    race_key=race_data.get("race_key", "unknown"),
                    bet_type="WIN",
                    selection=umaban,
                    stake_yen=self._stake,
                    est_prob=0.2,
                    odds_at_bet=5.0,
                    est_ev=1.0,
                    factor_details={"speed": 1.0},
                )
            )
        return bets


class EmptyStrategy(Strategy):
    """投票対象なしの戦略。"""

    def name(self) -> str:
        return "empty_strategy"

    def version(self) -> str:
        return "1.0.0"

    def run(
        self,
        race_data: dict[str, Any],
        entries: list[dict[str, Any]],
        odds: dict[str, float],
        bankroll: int,
        params: dict[str, Any],
    ) -> list[Bet]:
        return []


def _make_race(race_key: str = "2025010106010101") -> dict[str, Any]:
    """テスト用レースデータを生成する。"""
    return {
        "race_info": {"race_key": race_key, "RaceName": "テストレース"},
        "entries": [
            {"Umaban": "01", "Bamei": "馬A"},
            {"Umaban": "02", "Bamei": "馬B"},
        ],
        "odds": {"01": 5.0, "02": 8.0},
    }


class TestBacktestConfig:
    """BacktestConfigデータクラスのテスト。"""

    def test_create_config(self) -> None:
        """設定が正しく生成されること。"""
        config = BacktestConfig(
            date_from="2024-01-01",
            date_to="2024-12-31",
            initial_bankroll=500_000,
            strategy_version="2.0",
        )
        assert config.date_from == "2024-01-01"
        assert config.initial_bankroll == 500_000

    def test_default_values(self) -> None:
        """デフォルト値が正しいこと。"""
        config = BacktestConfig(date_from="2024-01-01", date_to="2024-12-31")
        assert config.initial_bankroll == 1_000_000
        assert config.strategy_version == ""


class TestBacktestEngine:
    """BacktestEngineクラスのテスト。"""

    def test_run_with_bets(self) -> None:
        """ベットありの戦略でバックテストが正しく実行されること。"""
        strategy = MockStrategy(bets_per_race=1, stake=1000)
        engine = BacktestEngine(strategy)
        config = BacktestConfig(date_from="2025-01-01", date_to="2025-01-31")
        races = [_make_race("2025010106010101"), _make_race("2025010106010102")]

        result = engine.run(races, config)
        assert isinstance(result, BacktestResult)
        assert result.total_races == 2
        assert result.total_bets == 2
        assert len(result.bets) == 2

    def test_run_no_bets(self) -> None:
        """ベットなしの戦略でバックテストが正しく実行されること。"""
        strategy = EmptyStrategy()
        engine = BacktestEngine(strategy)
        config = BacktestConfig(date_from="2025-01-01", date_to="2025-01-31")
        races = [_make_race()]

        result = engine.run(races, config)
        assert result.total_races == 1
        assert result.total_bets == 0
        assert result.metrics.total_stake == 0

    def test_run_empty_races(self) -> None:
        """空のレースリストで正しく実行されること。"""
        strategy = MockStrategy()
        engine = BacktestEngine(strategy)
        config = BacktestConfig(date_from="2025-01-01", date_to="2025-01-31")

        result = engine.run([], config)
        assert result.total_races == 0
        assert result.total_bets == 0

    def test_bankroll_decreases_on_bet(self) -> None:
        """ベット分だけbankrollが減ること。"""
        strategy = MockStrategy(bets_per_race=2, stake=5000)
        engine = BacktestEngine(strategy)
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-01-31",
            initial_bankroll=100_000,
        )
        races = [_make_race()]

        result = engine.run(races, config)
        # 2ベット × 5000円 = 10000円分の投票
        assert result.metrics.total_stake == 10000

    def test_config_preserved_in_result(self) -> None:
        """結果にconfigが保存されること。"""
        strategy = MockStrategy()
        engine = BacktestEngine(strategy)
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-06-30",
            initial_bankroll=500_000,
        )
        result = engine.run([], config)
        assert result.config is config
        assert result.config.initial_bankroll == 500_000

    def test_multiple_bets_per_race(self) -> None:
        """1レースで複数ベットが正しく処理されること。"""
        strategy = MockStrategy(bets_per_race=2, stake=1000)
        engine = BacktestEngine(strategy)
        config = BacktestConfig(date_from="2025-01-01", date_to="2025-01-31")
        races = [_make_race()]

        result = engine.run(races, config)
        assert result.total_bets == 2
