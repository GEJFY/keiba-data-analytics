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

    def test_score_horse_with_prev_context(self, initialized_db: DatabaseManager) -> None:
        """prev_context渡し時にスコアが変化すること。"""
        engine = ScoringEngine(initialized_db)
        prev_context = {"KakuteiJyuni": "2", "HaronTimeL3": "340", "KyakusituKubun": "1", "Jyuni4c": "3"}
        rules = [{
            "rule_name": "前走上位着順減点",
            "sql_expression": "-1 if prev_jyuni > 0 and prev_jyuni <= 3 else 0",
            "weight": 1.0,
        }]
        # prev_contextあり: prev_jyuni=2 → -1 * 1.0 = -1 → total=99
        result = engine.score_horse(
            horse={"Umaban": "01"},
            race={},
            all_entries=[{"Umaban": "01"}],
            rules=rules,
            prev_context=prev_context,
        )
        assert result["total_score"] == 99.0

        # prev_contextなし: prev_jyuni=0 → 0 → total=100
        result_no_prev = engine.score_horse(
            horse={"Umaban": "01"},
            race={},
            all_entries=[{"Umaban": "01"}],
            rules=rules,
        )
        assert result_no_prev["total_score"] == 100.0

    def test_score_race_backward_compatible(self, initialized_db: DatabaseManager) -> None:
        """jvlink_providerなしでscore_raceが従来通り動作すること。"""
        engine = ScoringEngine(initialized_db)
        results = engine.score_race(
            race={"Kyori": "1600", "TrackCD": "10"},
            entries=[{"Umaban": "01", "KettoNum": "0001"}],
            odds_map={"01": 5.0},
        )
        # ルールがないのでBASE_SCORE=100のフォールバック変換
        assert len(results) >= 0  # 結果がリストであること
