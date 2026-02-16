"""スコアリングエンジン本体。

GY指数方式に基づき、各馬にファクタースコアを付与し、
期待値（EV）を算出する。

スコアリングフロー:
    1. APPROVEDルール群を取得
    2. 各馬に対してルールを適用し、weighted_scoreを合算
    3. BASE_SCORE(100) + 合算値 = total_score
    4. 確率校正モデルで確率変換 → EV = prob × odds
    5. EV > ev_threshold のベットを「バリューベット」と判定
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider
from src.factors.registry import FactorRegistry
from src.scoring.calibration import ProbabilityCalibrator
from src.scoring.evaluator import evaluate_rule


class ScoringEngine:
    """GY指数方式スコアリングエンジン。

    Attributes:
        BASE_SCORE: 全馬共通の基礎スコア（100点）
    """

    BASE_SCORE = 100

    def __init__(
        self,
        db: DatabaseManager,
        calibrator: ProbabilityCalibrator | None = None,
        calibrator_path: str | Path | None = None,
        ev_threshold: float = 1.05,
        jvlink_provider: JVLinkDataProvider | None = None,
    ) -> None:
        """
        Args:
            db: データベースマネージャ
            calibrator: 確率校正モデル（None時はフォールバック変換）
            calibrator_path: 校正モデルファイルパス（calibrator未指定時に使用）
            ev_threshold: バリューベット判定の期待値閾値
            jvlink_provider: JVLinkデータプロバイダ（前走データ取得用、Noneで前走なし）
        """
        self._db = db
        self._registry = FactorRegistry(db)
        self._calibrator = calibrator
        self._ev_threshold = ev_threshold
        self._fallback_warned = False
        self._provider = jvlink_provider

        # calibrator未設定でパス指定があればファイルからロード
        if self._calibrator is None and calibrator_path:
            path = Path(calibrator_path)
            if path.exists():
                try:
                    self._calibrator = ProbabilityCalibrator.load(path)
                    logger.info(f"校正モデルを読み込みました: {path}")
                except Exception as e:
                    logger.warning(f"校正モデル読込エラー: {e}")

    def score_horse(
        self,
        horse: dict[str, Any],
        race: dict[str, Any],
        all_entries: list[dict[str, Any]],
        rules: list[dict[str, Any]],
        prev_context: dict[str, Any] | None = None,
        all_prev_l3f: list[float] | None = None,
    ) -> dict[str, Any]:
        """1頭の馬に対してスコアを計算する。

        Args:
            horse: 対象馬データ（NL_SEレコード）
            race: レース情報（NL_RAレコード）
            all_entries: 同レース全出走馬データ
            rules: 適用するファクタールールのリスト
            prev_context: 前走の出走馬データ（Noneで前走なし）
            all_prev_l3f: 同レース全馬の前走HaronTimeL3リスト

        Returns:
            {"umaban", "total_score", "factor_details"} のdict
        """
        total_score = self.BASE_SCORE
        factor_details: dict[str, float] = {}

        for rule in rules:
            expression = rule.get("sql_expression", "")
            rule_result = evaluate_rule(
                expression, horse, race, all_entries,
                prev_context=prev_context,
                all_prev_l3f=all_prev_l3f,
            )
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
        track_type: str = "turf",
        distance: int = 1600,
    ) -> dict[str, Any]:
        """スコアから期待値を算出する。

        Args:
            score_result: score_horse()の戻り値
            actual_odds: 実際のオッズ
            track_type: トラック種別 ("turf"/"dirt") — 層別キャリブレーション用
            distance: 距離（メートル） — 層別キャリブレーション用

        Returns:
            score_resultに確率・EV情報を追加したdict
        """
        total_score = score_result["total_score"]

        # 確率校正（StratifiedCalibrator対応）
        if self._calibrator:
            from src.scoring.stratified_calibrator import StratifiedCalibrator

            if isinstance(self._calibrator, StratifiedCalibrator):
                estimated_prob = self._calibrator.predict_proba(
                    total_score, track_type=track_type, distance=distance,
                )
            else:
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

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        """安全に浮動小数点変換する。"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        """安全に整数変換する。"""
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def score_race(
        self,
        race: dict[str, Any],
        entries: list[dict[str, Any]],
        odds_map: dict[str, float],
        race_key: str = "",
        as_of_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """1レース全馬のスコアと期待値を計算する。

        Args:
            race: レース情報
            entries: 出走馬リスト
            odds_map: 馬番→オッズのマッピング
            race_key: レースキー（前走データ取得用、空文字で前走なし）
            as_of_date: 時点日 (YYYY-MM-DD)。指定時は訓練期間が
                この日付より前のファクターのみ使用する。

        Returns:
            EV降順にソートされたスコア結果リスト（オッズ0の馬は除外）
        """
        rules = self._registry.get_active_rules(as_of_date=as_of_date)
        results = []

        # 前走データ取得（provider + race_keyが揃っている場合のみ）
        prev_contexts: list[dict[str, Any] | None] = []
        all_prev_l3f: list[float] | None = None
        if self._provider and race_key:
            l3f_list: list[float] = []
            for horse in entries:
                ketto = str(horse.get("KettoNum", ""))
                prev = (
                    self._provider.get_previous_race_entry(ketto, race_key)
                    if ketto else None
                )
                prev_contexts.append(prev)
                l3f_list.append(
                    self._safe_float(prev.get("HaronTimeL3", 0)) if prev else 0.0
                )
            all_prev_l3f = l3f_list
        else:
            prev_contexts = [None] * len(entries)

        # 層別キャリブレーション用のレース情報
        track_cd = str(race.get("TrackCD", ""))
        track_type = "dirt" if track_cd.startswith("2") else "turf"
        distance = self._safe_int(race.get("Kyori", 1600))

        for i, horse in enumerate(entries):
            score_result = self.score_horse(
                horse, race, entries, rules,
                prev_context=prev_contexts[i],
                all_prev_l3f=all_prev_l3f,
            )
            umaban = str(horse.get("Umaban", ""))
            actual_odds = odds_map.get(umaban, 0.0)

            if actual_odds > 0:
                ev_result = self.calculate_ev(
                    score_result, actual_odds,
                    track_type=track_type, distance=distance,
                )
                results.append(ev_result)

        return sorted(results, key=lambda x: x["expected_value"], reverse=True)

    def save_scores(
        self,
        race_key: str,
        scored_results: list[dict[str, Any]],
        ext_db: DatabaseManager,
        strategy_version: str = "",
    ) -> int:
        """スコア結果をhorse_scoresテーブルに保存する。

        Args:
            race_key: レースキー
            scored_results: score_race()の戻り値
            ext_db: 拡張DB（horse_scoresテーブルが存在するDB）
            strategy_version: 戦略バージョン文字列

        Returns:
            保存したレコード数
        """
        if not ext_db.table_exists("horse_scores"):
            logger.warning("horse_scoresテーブルが存在しません")
            return 0

        now = datetime.now(UTC).isoformat()
        saved = 0

        for result in scored_results:
            try:
                factor_json = json.dumps(
                    result.get("factor_details", {}),
                    ensure_ascii=False,
                    default=str,
                )
                ext_db.execute_write(
                    """INSERT INTO horse_scores
                       (race_key, umaban, total_score, factor_details,
                        estimated_prob, fair_odds, actual_odds, expected_value,
                        strategy_version, calculated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        race_key,
                        result.get("umaban", ""),
                        result.get("total_score", 0.0),
                        factor_json,
                        result.get("estimated_prob"),
                        result.get("fair_odds"),
                        result.get("actual_odds"),
                        result.get("expected_value"),
                        strategy_version,
                        now,
                    ),
                )
                saved += 1
            except Exception as e:
                logger.error(f"スコア保存エラー: umaban={result.get('umaban')} {e}")

        logger.info(f"スコア保存完了: race_key={race_key} {saved}件")
        return saved
