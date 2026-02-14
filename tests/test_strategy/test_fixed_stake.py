"""FixedStakeStrategy のテスト。"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.strategy.plugins.fixed_stake import FixedStakeStrategy


class TestFixedStakeStrategy:
    """FixedStakeStrategy のテスト。"""

    def test_name_and_version(self) -> None:
        """名前とバージョンが正しいこと。"""
        db = MagicMock()
        db.execute_query = MagicMock(return_value=[])
        strategy = FixedStakeStrategy(db)
        assert strategy.name() == "FIXED_STAKE"
        assert strategy.version() == "1.0.0"

    def test_run_no_entries(self) -> None:
        """エントリなしで空リスト。"""
        db = MagicMock()
        db.execute_query = MagicMock(return_value=[])
        strategy = FixedStakeStrategy(db)
        result = strategy.run({}, [], {}, 100_000, {})
        assert result == []

    def test_run_no_odds(self) -> None:
        """オッズなしで空リスト。"""
        db = MagicMock()
        db.execute_query = MagicMock(return_value=[])
        strategy = FixedStakeStrategy(db)
        entries = [{"Umaban": "01"}]
        result = strategy.run({}, entries, {}, 100_000, {})
        assert result == []

    @patch.object(FixedStakeStrategy, "_build_race_key", return_value="test_key")
    def test_run_returns_fixed_stake_bets(self, mock_key) -> None:
        """固定金額でベットが返ること。"""
        db = MagicMock()
        db.execute_query = MagicMock(return_value=[])

        strategy = FixedStakeStrategy(db, stake_yen=2000)

        # ScoringEngineをモック
        mock_scored = [
            {
                "umaban": "01",
                "win_prob": 0.25,
                "odds": 5.0,
                "expected_value": 1.25,
                "factor_scores": {"speed": 1.0},
            },
            {
                "umaban": "03",
                "win_prob": 0.15,
                "odds": 8.0,
                "expected_value": 1.10,
                "factor_scores": {"dm": 0.5},
            },
        ]
        with patch.object(strategy._engine, "score_race", return_value=mock_scored):
            bets = strategy.run(
                {"race_key": "test"},
                [{"Umaban": "01"}, {"Umaban": "03"}],
                {"01": 5.0, "03": 8.0},
                100_000,
                {},
            )

        assert len(bets) == 2
        # 全ベットが固定金額
        assert all(b.stake_yen == 2000 for b in bets)
        assert bets[0].selection == "01"
        assert bets[1].selection == "03"

    @patch.object(FixedStakeStrategy, "_build_race_key", return_value="test_key")
    def test_max_bets_per_race(self, mock_key) -> None:
        """max_bets_per_raceで制限されること。"""
        db = MagicMock()
        db.execute_query = MagicMock(return_value=[])
        strategy = FixedStakeStrategy(db, max_bets_per_race=1)

        mock_scored = [
            {"umaban": "01", "win_prob": 0.3, "odds": 5.0, "expected_value": 1.5, "factor_scores": {}},
            {"umaban": "02", "win_prob": 0.2, "odds": 8.0, "expected_value": 1.2, "factor_scores": {}},
        ]
        with patch.object(strategy._engine, "score_race", return_value=mock_scored):
            bets = strategy.run({}, [{"Umaban": "01"}], {"01": 5.0}, 100_000, {})

        assert len(bets) == 1
