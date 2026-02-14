"""過去レース一括スコアリングモジュール。

horse_scoresテーブルが空でも、過去レースデータから直接
ファクター行列と着順ラベルを生成する。
Weight最適化・キャリブレーター学習の訓練データ供給源。
"""

import time
from typing import Any

import numpy as np
from loguru import logger

from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider
from src.factors.registry import FactorRegistry
from src.scoring.evaluator import evaluate_rule


class BatchScorer:
    """過去レースをファクター別に一括評価し、訓練データを生成する。"""

    BASE_SCORE = 100

    def __init__(
        self,
        jvlink_db: DatabaseManager,
        ext_db: DatabaseManager,
    ) -> None:
        """
        Args:
            jvlink_db: JVLink DBマネージャ
            ext_db: 拡張DBマネージャ（factor_rules取得用）
        """
        self._jvlink_db = jvlink_db
        self._ext_db = ext_db
        self._provider = JVLinkDataProvider(jvlink_db)
        self._registry = FactorRegistry(ext_db)

    def build_factor_matrix(
        self,
        date_from: str = "",
        date_to: str = "",
        max_races: int = 5000,
        progress_callback: Any = None,
    ) -> dict[str, Any]:
        """過去レースからファクター行列を構築する。

        Args:
            date_from: 開始日 "YYYYMMDD"（空文字で制限なし）
            date_to: 終了日 "YYYYMMDD"（空文字で制限なし）
            max_races: 最大レース数

        Returns:
            {
                "race_keys": list[str],
                "factor_names": list[str],
                "X": NDArray (N_horses, N_factors) — ファクターraw値
                "y": NDArray (N_horses,) — 1着=1, それ以外=0
                "scores": NDArray (N_horses,) — total_score (BASE+加重合計)
                "odds": NDArray (N_horses,) — 単勝オッズ
                "jyuni": NDArray (N_horses,) — 確定着順
            }

        Raises:
            ValueError: 有効なレースが見つからない場合
        """
        t_start = time.perf_counter()

        rules = self._registry.get_active_rules()
        if not rules:
            raise ValueError("APPROVEDルールが存在しません")

        factor_names = [r["rule_name"] for r in rules]

        # 日付フィルタパラメータをログ出力
        if date_from or date_to:
            logger.info(
                f"日付フィルタ: date_from={date_from or '(なし)'}, "
                f"date_to={date_to or '(なし)'}, max_races={max_races}"
            )
        else:
            logger.info(f"日付フィルタなし（全期間）, max_races={max_races}")

        # レースデータを一括取得（バッチクエリで高速化）
        with self._jvlink_db.session():
            batch_data = self._provider.fetch_races_batch(
                date_from=date_from,
                date_to=date_to,
                max_races=max_races,
                include_payouts=False,
            )
        if not batch_data:
            raise ValueError("対象期間のレースが見つかりません")

        logger.info(
            f"バッチスコアリング開始: {len(batch_data)}レース, "
            f"{len(rules)}ファクター"
        )

        all_race_keys: list[str] = []
        all_X: list[list[float]] = []
        all_scores: list[float] = []
        all_odds: list[float] = []
        all_jyuni: list[int] = []

        races_with_odds = 0
        races_without_odds = 0
        skipped_entries = 0

        # 前走データキャッシュ（KettoNum -> 直近出走データ）
        # レースはASC順（古い順）で処理し、各馬の出走後にキャッシュを更新する
        prev_entry_cache: dict[str, dict[str, Any]] = {}

        processed = 0
        for race_data in batch_data:
            race_key = race_data["race_key"]
            race_info = race_data["race_info"]
            entries = race_data["entries"]
            odds_map = race_data["odds"]

            if odds_map:
                races_with_odds += 1
            else:
                races_without_odds += 1

            # 各馬の前走データを取得（evaluate_rule呼び出し前に全馬分準備）
            horse_prev_contexts: list[dict[str, Any] | None] = []
            all_prev_l3f: list[float] = []
            for horse in entries:
                ketto = str(horse.get("KettoNum", ""))
                prev = prev_entry_cache.get(ketto) if ketto else None
                horse_prev_contexts.append(prev)
                all_prev_l3f.append(
                    self._safe_float(prev.get("HaronTimeL3", 0)) if prev else 0.0
                )

            for idx, horse in enumerate(entries):
                jyuni = self._safe_int(horse.get("KakuteiJyuni", 0))
                if jyuni <= 0:
                    skipped_entries += 1
                    continue  # 未確定馬はスキップ

                umaban = str(horse.get("Umaban", ""))
                horse_odds = odds_map.get(umaban, 0.0)
                prev_ctx = horse_prev_contexts[idx]

                # 各ファクターのraw値を計算
                factor_values: list[float] = []
                total_score = self.BASE_SCORE
                for rule in rules:
                    raw = evaluate_rule(
                        rule.get("sql_expression", ""),
                        horse,
                        race_info,
                        entries,
                        prev_context=prev_ctx,
                        all_prev_l3f=all_prev_l3f,
                    )
                    factor_values.append(raw)
                    total_score += raw * rule.get("weight", 1.0)

                all_race_keys.append(race_key)
                all_X.append(factor_values)
                all_scores.append(total_score)
                all_odds.append(horse_odds)
                all_jyuni.append(jyuni)

            # レース処理後: 全馬のキャッシュを更新
            for horse in entries:
                ketto = str(horse.get("KettoNum", ""))
                if ketto:
                    prev_entry_cache[ketto] = dict(horse)

            processed += 1
            if progress_callback:
                progress_callback(
                    processed, len(batch_data),
                    f"レース処理中 {processed}/{len(batch_data)}"
                )
            elif processed % 500 == 0:
                logger.info(f"  処理済み: {processed}/{len(batch_data)}レース")

        if not all_X:
            raise ValueError("有効なスコアリングデータがありません")

        elapsed = time.perf_counter() - t_start
        logger.info(
            f"バッチスコアリング完了: {processed}レース, {len(all_X)}頭 "
            f"(elapsed={elapsed:.2f}s)"
        )
        logger.info(
            f"  オッズ取得統計: オッズあり={races_with_odds}レース, "
            f"オッズなし={races_without_odds}レース"
        )
        logger.info(
            f"  スキップ統計: 未確定馬(jyuni<=0)={skipped_entries}件"
        )
        logger.debug(
            f"  行列形状: X={np.array(all_X).shape}, "
            f"jyuni={len(all_jyuni)}, odds={len(all_odds)}"
        )

        return {
            "race_keys": all_race_keys,
            "factor_names": factor_names,
            "X": np.array(all_X, dtype=np.float64),
            "y": (np.array(all_jyuni) == 1).astype(np.int64),
            "scores": np.array(all_scores, dtype=np.float64),
            "odds": np.array(all_odds, dtype=np.float64),
            "jyuni": np.array(all_jyuni, dtype=np.int64),
        }

    def _get_race_list(
        self,
        date_from: str,
        date_to: str,
        max_races: int,
    ) -> list[dict[str, Any]]:
        """日付範囲でフィルタリングしたレース一覧を取得する。"""
        if not self._jvlink_db.table_exists("NL_RA_RACE"):
            return []

        conditions: list[str] = []
        params: list[Any] = []

        if date_from:
            year_from = date_from[:4]
            md_from = date_from[4:8]
            conditions.append("(idYear > ? OR (idYear = ? AND idMonthDay >= ?))")
            params.extend([year_from, year_from, md_from])

        if date_to:
            year_to = date_to[:4]
            md_to = date_to[4:8]
            conditions.append("(idYear < ? OR (idYear = ? AND idMonthDay <= ?))")
            params.extend([year_to, year_to, md_to])

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(max_races)

        return self._jvlink_db.execute_query(
            f"""
            SELECT
                idYear AS Year, idMonthDay AS MonthDay,
                idJyoCD AS JyoCD, idKaiji AS Kaiji,
                idNichiji AS Nichiji, idRaceNum AS RaceNum
            FROM NL_RA_RACE
            {where}
            ORDER BY idYear ASC, idMonthDay ASC
            LIMIT ?
            """,
            tuple(params),
        )

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        """安全に整数変換する。"""
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        """安全に浮動小数点変換する。"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
