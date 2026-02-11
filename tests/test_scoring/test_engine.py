"""スコアリングエンジンの単体テスト。"""

import pytest

from src.data.db import DatabaseManager
from src.scoring.engine import ScoringEngine


@pytest.mark.unit
class TestScoringEngine:
    """ScoringEngineクラスのテスト。"""

    def test_score_horse_base_score(self, initialized_db: DatabaseManager) -> None:
        """ルールなしの場合ベーススコア100が返ること。"""
        engine = ScoringEngine(initialized_db)
        result = engine.score_horse(
            horse={"Umaban": "01"},
            race={},
            all_entries=[{"Umaban": "01"}],
            rules=[],
        )
        assert result["total_score"] == 100
        assert result["umaban"] == "01"

    def test_calculate_ev_value_bet(self, initialized_db: DatabaseManager) -> None:
        """期待値が閾値を超える場合にvalue_betがTrueになること。"""
        engine = ScoringEngine(initialized_db, ev_threshold=1.0)
        score_result = {"umaban": "01", "total_score": 120, "factor_details": {}}
        # 高オッズで期待値が高い場合
        result = engine.calculate_ev(score_result, actual_odds=10.0)
        assert "expected_value" in result
        assert "is_value_bet" in result

    def test_calculate_ev_no_value_bet(self, initialized_db: DatabaseManager) -> None:
        """期待値が低い場合にvalue_betがFalseになること。"""
        engine = ScoringEngine(initialized_db, ev_threshold=2.0)
        score_result = {"umaban": "01", "total_score": 80, "factor_details": {}}
        result = engine.calculate_ev(score_result, actual_odds=1.5)
        assert result["is_value_bet"] is False
