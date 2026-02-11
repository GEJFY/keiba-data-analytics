"""ファクター基底クラスの単体テスト。"""

from typing import Any

import pytest

from src.factors.base import BaseFactor, FactorResult


class ConcreteFactor(BaseFactor):
    """テスト用の具象ファクタークラス。"""

    def name(self) -> str:
        return "test_factor"

    def category(self) -> str:
        return "speed"

    def evaluate(
        self,
        horse: dict[str, Any],
        race: dict[str, Any],
        all_entries: list[dict[str, Any]],
    ) -> FactorResult:
        # テスト用の簡易評価ロジック
        score = 1.0 if horse.get("speed_index", 0) > 50 else -1.0
        weight = 1.5
        return FactorResult(
            rule_id=1,
            rule_name=self.name(),
            score=score,
            weight=weight,
            weighted_score=score * weight,
            detail=f"speed_index={horse.get('speed_index', 0)}",
        )


class TestFactorResult:
    """FactorResultデータクラスのテスト。"""

    def test_create_factor_result(self) -> None:
        """FactorResultが正しく生成されること。"""
        result = FactorResult(
            rule_id=1,
            rule_name="test",
            score=1.0,
            weight=2.0,
            weighted_score=2.0,
            detail="テスト結果",
        )
        assert result.rule_id == 1
        assert result.rule_name == "test"
        assert result.score == 1.0
        assert result.weight == 2.0
        assert result.weighted_score == 2.0
        assert result.detail == "テスト結果"

    def test_factor_result_equality(self) -> None:
        """同値のFactorResultが等しいこと。"""
        r1 = FactorResult(rule_id=1, rule_name="a", score=1.0, weight=1.0, weighted_score=1.0, detail="x")
        r2 = FactorResult(rule_id=1, rule_name="a", score=1.0, weight=1.0, weighted_score=1.0, detail="x")
        assert r1 == r2


class TestBaseFactor:
    """BaseFactor ABCのテスト。"""

    def test_cannot_instantiate_abstract(self) -> None:
        """抽象クラスを直接インスタンス化できないこと。"""
        with pytest.raises(TypeError):
            BaseFactor()  # type: ignore[abstract]

    def test_concrete_factor_name(self) -> None:
        """具象クラスがname()を返すこと。"""
        factor = ConcreteFactor()
        assert factor.name() == "test_factor"

    def test_concrete_factor_category(self) -> None:
        """具象クラスがcategory()を返すこと。"""
        factor = ConcreteFactor()
        assert factor.category() == "speed"

    def test_evaluate_positive(self) -> None:
        """高スピード馬に正スコアが付与されること。"""
        factor = ConcreteFactor()
        result = factor.evaluate(
            horse={"speed_index": 80},
            race={"Kyori": "1600"},
            all_entries=[],
        )
        assert result.score == 1.0
        assert result.weighted_score == 1.5

    def test_evaluate_negative(self) -> None:
        """低スピード馬に負スコアが付与されること。"""
        factor = ConcreteFactor()
        result = factor.evaluate(
            horse={"speed_index": 30},
            race={"Kyori": "1600"},
            all_entries=[],
        )
        assert result.score == -1.0
        assert result.weighted_score == -1.5

    def test_evaluate_with_detail(self) -> None:
        """評価結果にdetailが含まれること。"""
        factor = ConcreteFactor()
        result = factor.evaluate(
            horse={"speed_index": 60},
            race={},
            all_entries=[],
        )
        assert "speed_index=60" in result.detail
