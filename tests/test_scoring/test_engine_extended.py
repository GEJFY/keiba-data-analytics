"""ScoringEngineの拡張テスト（score_race、校正モデル連携）。"""

from unittest.mock import MagicMock

import pytest

from src.data.db import DatabaseManager
from src.scoring.engine import ScoringEngine


@pytest.fixture
def scoring_db(initialized_db: DatabaseManager) -> DatabaseManager:
    """スコアリングテスト用のルールが登録されたDBを返す。"""
    from src.factors.registry import FactorRegistry

    registry = FactorRegistry(initialized_db)
    # テスト用ルール作成 → APPROVED化
    rule_id = registry.create_rule({
        "rule_name": "test_speed",
        "category": "speed",
        "weight": 2.0,
    })
    registry.transition_status(rule_id, "TESTING", reason="テスト")
    registry.transition_status(rule_id, "APPROVED", reason="承認")
    return initialized_db


class TestScoringEngineExtended:
    """ScoringEngineの拡張テスト。"""

    def test_score_race_returns_sorted_by_ev(self, scoring_db: DatabaseManager) -> None:
        """score_raceがEV降順でソートされた結果を返すこと。"""
        engine = ScoringEngine(scoring_db, ev_threshold=1.0)
        race = {"RaceName": "テストレース"}
        entries = [
            {"Umaban": "01"},
            {"Umaban": "02"},
            {"Umaban": "03"},
        ]
        odds_map = {"01": 5.0, "02": 10.0, "03": 2.0}

        results = engine.score_race(race, entries, odds_map)
        assert len(results) == 3
        # EV降順に並んでいること
        evs = [r["expected_value"] for r in results]
        assert evs == sorted(evs, reverse=True)

    def test_score_race_skips_zero_odds(self, scoring_db: DatabaseManager) -> None:
        """オッズが0の馬はスキップされること。"""
        engine = ScoringEngine(scoring_db)
        race = {"RaceName": "テスト"}
        entries = [
            {"Umaban": "01"},
            {"Umaban": "02"},
        ]
        odds_map = {"01": 5.0, "02": 0.0}

        results = engine.score_race(race, entries, odds_map)
        assert len(results) == 1
        assert results[0]["umaban"] == "01"

    def test_score_race_missing_odds(self, scoring_db: DatabaseManager) -> None:
        """odds_mapに馬番が存在しない場合スキップされること。"""
        engine = ScoringEngine(scoring_db)
        race = {"RaceName": "テスト"}
        entries = [
            {"Umaban": "01"},
            {"Umaban": "02"},
        ]
        odds_map = {"01": 5.0}  # 02のオッズがない

        results = engine.score_race(race, entries, odds_map)
        assert len(results) == 1

    def test_score_horse_with_rules(self, scoring_db: DatabaseManager) -> None:
        """ルール適用時にファクター詳細が含まれること。"""
        engine = ScoringEngine(scoring_db)
        rules = [{"rule_name": "test_speed", "weight": 2.0}]
        result = engine.score_horse(
            horse={"Umaban": "01"},
            race={"RaceName": "テスト"},
            all_entries=[],
            rules=rules,
        )
        assert "factor_details" in result
        assert "test_speed" in result["factor_details"]

    def test_calculate_ev_with_calibrator(self, scoring_db: DatabaseManager) -> None:
        """校正モデル使用時にpredict_probaが呼ばれること。"""
        mock_calibrator = MagicMock()
        mock_calibrator.predict_proba.return_value = 0.25

        engine = ScoringEngine(scoring_db, calibrator=mock_calibrator, ev_threshold=1.0)
        score_result = {"total_score": 120, "umaban": "01", "factor_details": {}}

        ev_result = engine.calculate_ev(score_result, actual_odds=5.0)
        mock_calibrator.predict_proba.assert_called_once_with(120)
        assert ev_result["estimated_prob"] == 0.25
        assert ev_result["expected_value"] == 0.25 * 5.0
        assert ev_result["is_value_bet"] is True

    def test_calculate_ev_without_calibrator_fallback(self, scoring_db: DatabaseManager) -> None:
        """校正モデルなしの場合フォールバック変換が使われること。"""
        engine = ScoringEngine(scoring_db, calibrator=None)
        score_result = {"total_score": 100, "umaban": "01", "factor_details": {}}

        ev_result = engine.calculate_ev(score_result, actual_odds=3.0)
        # フォールバック: total_score / 200.0 = 0.5
        assert ev_result["estimated_prob"] == 0.5
        assert ev_result["expected_value"] == 0.5 * 3.0

    def test_ev_threshold_parameter(self, scoring_db: DatabaseManager) -> None:
        """ev_thresholdパラメータが正しく機能すること。"""
        engine = ScoringEngine(scoring_db, calibrator=None, ev_threshold=2.0)
        score_result = {"total_score": 100, "umaban": "01", "factor_details": {}}

        ev_result = engine.calculate_ev(score_result, actual_odds=3.0)
        # EV = 0.5 * 3.0 = 1.5 < 2.0
        assert ev_result["is_value_bet"] is False
