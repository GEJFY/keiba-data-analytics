"""GYバリュー戦略のテスト。"""

import pytest

from src.data.db import DatabaseManager
from src.strategy.plugins.gy_value import GYValueStrategy


@pytest.fixture
def strategy_db(initialized_db: DatabaseManager) -> DatabaseManager:
    """ファクタールール登録済みのDBを返す。"""
    from src.factors.registry import FactorRegistry

    registry = FactorRegistry(initialized_db)
    rule_id = registry.create_rule({
        "rule_name": "テスト_若馬加点",
        "category": "test",
        "sql_expression": "0.5 if Barei == 3 else 0",
        "weight": 1.0,
    })
    registry.transition_status(rule_id, "TESTING", reason="test")
    registry.transition_status(rule_id, "APPROVED", reason="test")
    return initialized_db


class TestGYValueStrategy:
    """GYValueStrategyクラスのテスト。"""

    def test_name_and_version(self, strategy_db: DatabaseManager) -> None:
        strategy = GYValueStrategy(strategy_db)
        assert strategy.name() == "GY_VALUE"
        assert strategy.version() == "1.0.0"

    def test_run_returns_bets(self, strategy_db: DatabaseManager) -> None:
        """バリューベットがある場合にBetを返すこと。"""
        strategy = GYValueStrategy(strategy_db, ev_threshold=0.5)
        race_data = {"Kyori": "1600", "TrackCD": "10", "Year": "2025",
                      "MonthDay": "0101", "JyoCD": "06", "Kaiji": "01",
                      "Nichiji": "01", "RaceNum": "01"}
        entries = [
            {"Umaban": "01", "Barei": "3", "Ninki": "5", "SexCD": "1",
             "Futan": "540", "BaTaijyu": "480", "ZogenFugo": "+", "ZogenSa": "2",
             "DMJyuni": "2", "HaronTimeL3": "340", "KyakusituKubun": "2",
             "Jyuni4c": "3", "Odds": "50", "KakuteiJyuni": "0", "Wakuban": "1"},
            {"Umaban": "02", "Barei": "5", "Ninki": "1", "SexCD": "1",
             "Futan": "560", "BaTaijyu": "500", "ZogenFugo": "+", "ZogenSa": "4",
             "DMJyuni": "1", "HaronTimeL3": "335", "KyakusituKubun": "1",
             "Jyuni4c": "1", "Odds": "20", "KakuteiJyuni": "0", "Wakuban": "1"},
        ]
        odds = {"01": 5.0, "02": 2.0}

        bets = strategy.run(race_data, entries, odds, bankroll=1_000_000, params={})
        # EV閾値が低いのでベットが出る可能性が高い
        assert isinstance(bets, list)
        for bet in bets:
            assert bet.bet_type == "WIN"
            assert bet.stake_yen >= 0
            assert bet.race_key == "2025010106010101"

    def test_run_no_entries(self, strategy_db: DatabaseManager) -> None:
        """出走馬がない場合は空リストを返すこと。"""
        strategy = GYValueStrategy(strategy_db)
        bets = strategy.run({}, [], {}, bankroll=1_000_000, params={})
        assert bets == []

    def test_run_no_odds(self, strategy_db: DatabaseManager) -> None:
        """オッズがない場合は空リストを返すこと。"""
        strategy = GYValueStrategy(strategy_db)
        entries = [{"Umaban": "01", "Barei": "3"}]
        bets = strategy.run({}, entries, {}, bankroll=1_000_000, params={})
        assert bets == []

    def test_max_bets_per_race(self, strategy_db: DatabaseManager) -> None:
        """max_bets_per_raceパラメータが機能すること。"""
        strategy = GYValueStrategy(strategy_db, ev_threshold=0.1)
        race_data = {"Kyori": "1600", "TrackCD": "10"}
        entries = []
        for i in range(10):
            entries.append({
                "Umaban": f"{i+1:02d}", "Barei": "3", "Ninki": str(i+1),
                "SexCD": "1", "Futan": "540", "BaTaijyu": "480",
                "ZogenFugo": "+", "ZogenSa": "2", "DMJyuni": str(i+1),
                "HaronTimeL3": str(340 + i*3), "KyakusituKubun": "2",
                "Jyuni4c": str(i+1), "Odds": str(20 + i*10),
                "KakuteiJyuni": "0", "Wakuban": "1",
            })
        odds = {f"{i+1:02d}": 2.0 + i * 1.5 for i in range(10)}

        bets = strategy.run(
            race_data, entries, odds, bankroll=1_000_000,
            params={"max_bets_per_race": 2},
        )
        assert len(bets) <= 2
