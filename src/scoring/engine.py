"""スコアリングエンジン本体。

卍指数方式に基づき、各馬にファクタースコアを付与し、
期待値を算出する。
"""

from typing import Any

from loguru import logger

from src.data.db import DatabaseManager
from src.factors.registry import FactorRegistry
from src.scoring.calibration import ProbabilityCalibrator


class ScoringEngine:
    """卍指数方式スコアリングエンジン。"""

    BASE_SCORE = 100

    def __init__(
        self,
        db: DatabaseManager,
        calibrator: ProbabilityCalibrator | None = None,
        ev_threshold: float = 1.05,
    ) -> None:
        self._db = db
        self._registry = FactorRegistry(db)
        self._calibrator = calibrator
        self._ev_threshold = ev_threshold

    def score_horse(
        self,
        horse: dict[str, Any],
        race: dict[str, Any],
        all_entries: list[dict[str, Any]],
        rules: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """1頭の馬に対してスコアを計算する。"""
        total_score = self.BASE_SCORE
        factor_details: dict[str, float] = {}

        for rule in rules:
            # TODO: ルールのSQL式/Python式を評価するロジックを実装
            rule_result = 0.0  # プレースホルダ
            weighted = rule_result * rule.get("weight", 1.0)
            total_score += weighted
            factor_details[rule["rule_name"]] = weighted

        return {
            "umaban": horse.get("Umaban", ""),
            "total_score": total_score,
            "factor_details": factor_details,
        }

    def calculate_ev(
        self,
        score_result: dict[str, Any],
        actual_odds: float,
    ) -> dict[str, Any]:
        """スコアから期待値を算出する。"""
        total_score = score_result["total_score"]

        # 確率校正
        if self._calibrator:
            estimated_prob = self._calibrator.predict_proba(total_score)
        else:
            # 校正モデルがない場合のフォールバック（仮の変換）
            logger.warning("確率校正モデルが未設定です。仮の変換を使用します。")
            estimated_prob = max(0.01, min(0.99, total_score / 200.0))

        fair_odds = 1.0 / estimated_prob if estimated_prob > 0 else float("inf")
        expected_value = estimated_prob * actual_odds

        return {
            **score_result,
            "estimated_prob": estimated_prob,
            "fair_odds": fair_odds,
            "actual_odds": actual_odds,
            "expected_value": expected_value,
            "is_value_bet": expected_value > self._ev_threshold,
        }

    def score_race(
        self,
        race: dict[str, Any],
        entries: list[dict[str, Any]],
        odds_map: dict[str, float],
    ) -> list[dict[str, Any]]:
        """1レース全馬のスコアと期待値を計算する。"""
        rules = self._registry.get_active_rules()
        results = []

        for horse in entries:
            score_result = self.score_horse(horse, race, entries, rules)
            umaban = str(horse.get("Umaban", ""))
            actual_odds = odds_map.get(umaban, 0.0)

            if actual_odds > 0:
                ev_result = self.calculate_ev(score_result, actual_odds)
                results.append(ev_result)

        return sorted(results, key=lambda x: x["expected_value"], reverse=True)
