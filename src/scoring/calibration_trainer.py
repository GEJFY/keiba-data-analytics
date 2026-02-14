"""校正モデル訓練モジュール。

過去のスコアリング結果と実際の着順データから
確率校正モデル（Platt/Isotonic）を訓練する。
"""

import json
import time
from typing import Any

import numpy as np
from loguru import logger
from numpy.typing import NDArray

from src.data.db import DatabaseManager
from src.scoring.calibration import (
    IsotonicCalibrator,
    PlattCalibrator,
    ProbabilityCalibrator,
)


class CalibrationTrainer:
    """確率校正モデルの訓練を管理するクラス。

    horse_scoresテーブルの過去スコアと、
    NL_SE_RACE_UMAのKakuteiJyuniから訓練データを構築し、
    PlattCalibrator / IsotonicCalibrator を訓練する。
    """

    def __init__(
        self,
        jvlink_db: DatabaseManager,
        ext_db: DatabaseManager,
    ) -> None:
        self._jvlink_db = jvlink_db
        self._ext_db = ext_db

    def build_training_data(
        self,
        target_jyuni: int = 1,
        min_samples: int = 50,
    ) -> tuple[NDArray[np.float64], NDArray[np.int64]]:
        """訓練データを構築する。

        horse_scoresテーブルのスコアと、実際の着順を紐付ける。

        Args:
            target_jyuni: 的中とみなす着順（1=1着のみ, 3=3着以内）
            min_samples: 最低サンプル数

        Returns:
            (scores, labels) のタプル
            - scores: shape=(N,) のスコア配列
            - labels: shape=(N,) の0/1ラベル（的中=1）

        Raises:
            ValueError: サンプル数不足
        """
        if not self._ext_db.table_exists("horse_scores"):
            raise ValueError("horse_scoresテーブルが存在しません")

        # horse_scores全件取得
        score_rows = self._ext_db.execute_query(
            "SELECT race_key, umaban, total_score FROM horse_scores"
        )
        if len(score_rows) < min_samples:
            raise ValueError(
                f"サンプル数不足: {len(score_rows)}件 (最低{min_samples}件必要)"
            )

        scores_list: list[float] = []
        labels_list: list[int] = []

        # 全レースの確定着順を一括取得（N+1 → 1クエリに削減）
        unique_race_keys = {row["race_key"] for row in score_rows if len(row["race_key"]) == 16}
        kakutei_map = self._batch_get_kakutei_jyuni(unique_race_keys)

        for row in score_rows:
            race_key = row["race_key"]
            umaban = row["umaban"]
            total_score = row["total_score"]

            kakutei = kakutei_map.get((race_key, str(umaban)), 0)
            if kakutei == 0:
                continue

            scores_list.append(total_score)
            labels_list.append(1 if kakutei <= target_jyuni else 0)

        if len(scores_list) < min_samples:
            raise ValueError(
                f"有効サンプル数不足: {len(scores_list)}件 (最低{min_samples}件必要)"
            )

        logger.info(
            f"訓練データ構築完了: {len(scores_list)}件 "
            f"(的中率={sum(labels_list)/len(labels_list):.1%})"
        )

        return (
            np.array(scores_list, dtype=np.float64),
            np.array(labels_list, dtype=np.int64),
        )

    def build_training_data_from_batch(
        self,
        date_from: str = "",
        date_to: str = "",
        max_races: int = 5000,
        target_jyuni: int = 1,
        min_samples: int = 50,
    ) -> tuple[NDArray[np.float64], NDArray[np.int64]]:
        """BatchScorerを使って直接訓練データを構築する。

        horse_scoresテーブルが不要なモード。
        過去レースから直接スコアを計算してラベルと紐付ける。

        Args:
            date_from: 開始日 "YYYYMMDD"
            date_to: 終了日 "YYYYMMDD"
            max_races: 最大レース数
            target_jyuni: 的中とみなす着順
            min_samples: 最低サンプル数

        Returns:
            (scores, labels) のタプル

        Raises:
            ValueError: サンプル数不足
        """
        from src.scoring.batch_scorer import BatchScorer

        batch = BatchScorer(self._jvlink_db, self._ext_db)
        matrix = batch.build_factor_matrix(date_from, date_to, max_races)

        scores = matrix["scores"]
        jyuni = matrix["jyuni"]
        labels = (jyuni <= target_jyuni).astype(np.int64)

        if len(scores) < min_samples:
            raise ValueError(
                f"サンプル数不足: {len(scores)}件 (最低{min_samples}件必要)"
            )

        logger.info(
            f"バッチ訓練データ構築完了: {len(scores)}件 "
            f"(的中率={labels.sum()/len(labels):.1%})"
        )

        return scores, labels

    def train(
        self,
        method: str = "platt",
        target_jyuni: int = 1,
        min_samples: int = 50,
        use_batch: bool = False,
        date_from: str = "",
        date_to: str = "",
        max_races: int = 5000,
        progress_callback: Any = None,
    ) -> ProbabilityCalibrator:
        """校正モデルを訓練する。

        Args:
            method: "platt" or "isotonic"
            target_jyuni: 的中着順閾値
            min_samples: 最低サンプル数
            use_batch: Trueの場合horse_scoresテーブル不要モード
            date_from: バッチモード時の開始日
            date_to: バッチモード時の終了日
            max_races: バッチモード時の最大レース数

        Returns:
            訓練済みProbabilityCalibrator
        """
        t_start = time.perf_counter()

        if progress_callback:
            progress_callback(0, 3, "訓練データを構築中...")

        if use_batch:
            scores, labels = self.build_training_data_from_batch(
                date_from, date_to, max_races, target_jyuni, min_samples
            )
        else:
            scores, labels = self.build_training_data(target_jyuni, min_samples)

        # スコア分布統計をログ出力
        logger.info(
            f"  スコア分布: min={scores.min():.2f}, max={scores.max():.2f}, "
            f"mean={scores.mean():.2f}, std={scores.std():.2f}"
        )

        # クラスバランスをログ出力
        n_positive = int(labels.sum())
        n_negative = len(labels) - n_positive
        logger.info(
            f"  クラスバランス: positive={n_positive}, "
            f"negative={n_negative}, ratio=1:{n_negative/max(n_positive,1):.1f}"
        )

        if progress_callback:
            progress_callback(1, 3, f"校正器フィット中 (method={method})...")

        if method == "platt":
            calibrator: ProbabilityCalibrator = PlattCalibrator()
        elif method == "isotonic":
            calibrator = IsotonicCalibrator()
        else:
            raise ValueError(f"不明な校正方法: {method} (platt/isotonic)")

        logger.info(f"  校正器フィット開始: method={method}, samples={len(scores)}")
        calibrator.fit(scores, labels)

        if progress_callback:
            progress_callback(2, 3, "校正品質を評価中...")

        # フィット後の確率範囲を確認
        sample_probs = np.array([calibrator.predict_proba(s) for s in scores])
        logger.info(
            f"  校正後確率範囲: min={sample_probs.min():.4f}, "
            f"max={sample_probs.max():.4f}, mean={sample_probs.mean():.4f}"
        )

        elapsed = time.perf_counter() - t_start
        logger.info(
            f"校正モデル訓練完了: method={method}, elapsed={elapsed:.2f}s"
        )

        return calibrator

    def evaluate_calibration(
        self,
        calibrator: ProbabilityCalibrator,
        target_jyuni: int = 1,
        n_bins: int = 10,
    ) -> dict[str, Any]:
        """校正モデルの精度を評価する。

        Args:
            calibrator: 評価対象の校正モデル
            target_jyuni: 的中着順閾値
            n_bins: 信頼度ビン数

        Returns:
            {"brier_score", "calibration_error", "bin_details"} のdict
        """
        scores, labels = self.build_training_data(target_jyuni, min_samples=10)

        # 予測確率を算出
        probs = np.array([calibrator.predict_proba(s) for s in scores])

        # ブライアースコア
        brier = float(np.mean((probs - labels) ** 2))

        # キャリブレーションエラー（ECE: Expected Calibration Error）
        bin_edges = np.linspace(0, 1, n_bins + 1)
        bin_details = []
        ece = 0.0

        for i in range(n_bins):
            mask = (probs >= bin_edges[i]) & (probs < bin_edges[i + 1])
            if not mask.any():
                continue
            bin_prob = float(probs[mask].mean())
            bin_actual = float(labels[mask].mean())
            bin_count = int(mask.sum())
            ece += abs(bin_prob - bin_actual) * (bin_count / len(labels))
            bin_details.append({
                "bin_range": f"{bin_edges[i]:.2f}-{bin_edges[i+1]:.2f}",
                "predicted_prob": bin_prob,
                "actual_rate": bin_actual,
                "count": bin_count,
            })

        logger.info(f"校正評価: Brier={brier:.4f} ECE={ece:.4f}")
        return {
            "brier_score": brier,
            "calibration_error": ece,
            "total_samples": len(labels),
            "bin_details": bin_details,
        }

    def _batch_get_kakutei_jyuni(
        self, race_keys: set[str],
    ) -> dict[tuple[str, str], int]:
        """複数レースの確定着順を一括取得する。

        Args:
            race_keys: 16文字のレースキー集合

        Returns:
            {(race_key, umaban): kakutei_jyuni} のdict
        """
        if not race_keys:
            return {}
        if not self._jvlink_db.table_exists("NL_SE_RACE_UMA"):
            return {}

        # 日付範囲の境界を算出
        parsed = []
        for rk in race_keys:
            if len(rk) != 16:
                continue
            parsed.append({"year": rk[0:4], "monthday": rk[4:8], "rk": rk})

        if not parsed:
            return {}

        min_year = min(p["year"] for p in parsed)
        max_year = max(p["year"] for p in parsed)
        min_md = min(p["monthday"] for p in parsed if p["year"] == min_year)
        max_md = max(p["monthday"] for p in parsed if p["year"] == max_year)

        rows = self._jvlink_db.execute_query(
            """SELECT idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum,
                      Umaban, KakuteiJyuni
               FROM NL_SE_RACE_UMA
               WHERE (idYear > ? OR (idYear = ? AND idMonthDay >= ?))
                 AND (idYear < ? OR (idYear = ? AND idMonthDay <= ?))""",
            (min_year, min_year, min_md, max_year, max_year, max_md),
        )

        result: dict[tuple[str, str], int] = {}
        for row in rows:
            rk = (
                f"{row['idYear']}{row['idMonthDay']}{row['idJyoCD']}"
                f"{row['idKaiji']}{row['idNichiji']}{row['idRaceNum']}"
            )
            if rk in race_keys:
                jyuni_str = row.get("KakuteiJyuni", "0")
                jyuni = int(jyuni_str) if jyuni_str and str(jyuni_str) != "0" else 0
                result[(rk, str(row["Umaban"]))] = jyuni

        return result

    def _get_kakutei_jyuni(self, race_key: str, umaban: str) -> int:
        """実確定着順を取得する（単一レース用フォールバック）。"""
        if len(race_key) != 16:
            return 0

        year = race_key[0:4]
        monthday = race_key[4:8]
        jyo_cd = race_key[8:10]
        kaiji = race_key[10:12]
        nichiji = race_key[12:14]
        race_num = race_key[14:16]

        if not self._jvlink_db.table_exists("NL_SE_RACE_UMA"):
            return 0

        rows = self._jvlink_db.execute_query(
            """SELECT KakuteiJyuni FROM NL_SE_RACE_UMA
               WHERE idYear = ? AND idMonthDay = ? AND idJyoCD = ?
               AND idKaiji = ? AND idNichiji = ? AND idRaceNum = ?
               AND Umaban = ?""",
            (year, monthday, jyo_cd, kaiji, nichiji, race_num, umaban),
        )
        if rows:
            jyuni = rows[0].get("KakuteiJyuni", "0")
            return int(jyuni) if jyuni and jyuni != "0" else 0
        return 0
