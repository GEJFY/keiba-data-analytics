"""スコアリングエンジン本体。

卍指数方式に基づき、各馬にファクタースコアを付与し、
期待値（EV）を算出する。

スコアリングフロー:
    1. APPROVEDルール群を取得
    2. 各馬に対してルールを適用し、weighted_scoreを合算
    3. BASE_SCORE(100) + 合算値 = total_score
    4. 確率校正モデルで確率変換 → EV = prob × odds
    5. EV > ev_threshold のベットを「バリューベット」と判定
"""

from typing import Any

from loguru import logger

from src.data.db import DatabaseManager
from src.factors.registry import FactorRegistry
from src.scoring.calibration import ProbabilityCalibrator


class ScoringEngine:
    """卍指数方式スコアリングエンジン。

    Attributes:
        BASE_SCORE: 全馬共通の基礎スコア（100点）
    """

    BASE_SCORE = 100

    def __init__(
        self,
        db: DatabaseManager,
        calibrator: ProbabilityCalibrator | None = None,
        ev_threshold: float = 1.05,
    ) -> None:
        """
        Args:
            db: データベースマネージャ
            calibrator: 確率校正モデル（None時はフォールバック変換）
            ev_threshold: バリューベット判定の期待値閾値
        """
        self._db = db
        self._registry = FactorRegistry(db)
        self._calibrator = calibrator
        self._ev_threshold = ev_threshold
        self._fallback_warned = False

    def score_horse(
        self,
        horse: dict[str, Any],
        race: dict[str, Any],
        all_entries: list[dict[str, Any]],
        rules: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """1頭の馬に対してスコアを計算する。

        Args:
            horse: 対象馬データ（NL_SEレコード）
            race: レース情報（NL_RAレコード）
            all_entries: 同レース全出走馬データ
            rules: 適用するファクタールールのリスト

        Returns:
            {"umaban", "total_score", "factor_details"} のdict
        """
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
        """スコアから期待値を算出する。

        Args:
            score_result: score_horse()の戻り値
            actual_odds: 実際のオッズ

        Returns:
            score_resultに確率・EV情報を追加したdict
        """
        total_score = score_result["total_score"]

        # 確率校正
        if self._calibrator:
            estimated_prob = self._calibrator.predict_proba(total_score)
        else:
            # 校正モデルがない場合のフォールバック（線形変換）
            if not self._fallback_warned:
                logger.warning("確率校正モデルが未設定です。線形変換(score/200)を使用します。")
                self._fallback_warned = True
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
        """1レース全馬のスコアと期待値を計算する。

        Args:
            race: レース情報
            entries: 出走馬リスト
            odds_map: 馬番→オッズのマッピング

        Returns:
            EV降順にソートされたスコア結果リスト（オッズ0の馬は除外）
        """
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
