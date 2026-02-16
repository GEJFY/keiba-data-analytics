"""層別キャリブレーションモジュール。

トラック種別（芝/ダート）× 距離カテゴリ（短距離/マイル/中距離/長距離）
ごとに個別のキャリブレーターを学習・適用する。

一律キャリブレーションでは芝短距離とダート長距離で確率分布が
大きく異なることを吸収できないため、層別化で系統的バイアスを除去する。
"""

from typing import Any

import numpy as np
from loguru import logger
from numpy.typing import NDArray

from src.scoring.calibration import (
    IsotonicCalibrator,
    PlattCalibrator,
    ProbabilityCalibrator,
)

# 層別の定義: トラック × 距離カテゴリ
STRATA = {
    "turf_sprint": {"track": "turf", "dist": (0, 1400)},
    "turf_mile": {"track": "turf", "dist": (1401, 1800)},
    "turf_middle": {"track": "turf", "dist": (1801, 2200)},
    "turf_long": {"track": "turf", "dist": (2201, 9999)},
    "dirt_sprint": {"track": "dirt", "dist": (0, 1400)},
    "dirt_mile": {"track": "dirt", "dist": (1401, 1800)},
    "dirt_long": {"track": "dirt", "dist": (1801, 9999)},
}

# 層別学習に必要な最低サンプル数（未満はfallback使用）
MIN_STRATUM_SAMPLES = 50


def get_stratum(track_type: str, distance: int) -> str:
    """トラック種別と距離から層名を返す。

    Args:
        track_type: "turf" or "dirt"
        distance: 距離（メートル）

    Returns:
        層名（例: "turf_mile"）。該当なしは "unknown"。
    """
    for name, spec in STRATA.items():
        if spec["track"] == track_type:
            lo, hi = spec["dist"]
            if lo <= distance <= hi:
                return name
    return "unknown"


def track_cd_to_type(track_cd: str) -> str:
    """JVLink TrackCDをトラック種別文字列に変換する。

    10-19: 芝系 → "turf"
    20-29: ダート系 → "dirt"
    それ以外: "turf"（デフォルト）
    """
    if track_cd.startswith("2"):
        return "dirt"
    return "turf"


class StratifiedCalibrator(ProbabilityCalibrator):
    """トラック×距離カテゴリ別の層別キャリブレーター。

    各層ごとに個別のキャリブレーターを学習し、
    予測時は該当層のキャリブレーターを使用する。
    サンプル不足の層はfallback（全データ学習モデル）で代替する。
    """

    def __init__(self, base_method: str = "platt") -> None:
        self._calibrators: dict[str, ProbabilityCalibrator] = {}
        self._fallback: ProbabilityCalibrator | None = None
        self._base_method = base_method

    def _create_calibrator(self) -> ProbabilityCalibrator:
        """base_methodに応じたキャリブレーターを生成する。"""
        if self._base_method == "isotonic":
            return IsotonicCalibrator()
        return PlattCalibrator()

    def fit(
        self,
        scores: NDArray[np.float64],
        labels: NDArray[np.int64],
        track_types: NDArray[Any] | None = None,
        distances: NDArray[np.int64] | None = None,
    ) -> None:
        """層ごとにキャリブレーターを学習する。

        Args:
            scores: スコア配列 shape=(N,)
            labels: 0/1ラベル配列 shape=(N,)
            track_types: トラック種別配列 shape=(N,) ("turf"/"dirt")
            distances: 距離配列 shape=(N,)
        """
        # fallback: 全データで学習
        self._fallback = self._create_calibrator()
        self._fallback.fit(scores, labels)

        if track_types is None or distances is None:
            logger.info("層別データなし — fallbackキャリブレーターのみ使用")
            return

        # 層ごとに分割して学習
        strata_labels = np.array([
            get_stratum(str(t), int(d))
            for t, d in zip(track_types, distances, strict=False)
        ])

        for stratum_name in STRATA:
            mask = strata_labels == stratum_name
            n_samples = int(mask.sum())

            if n_samples < MIN_STRATUM_SAMPLES:
                logger.debug(
                    f"層 {stratum_name}: {n_samples}件 < {MIN_STRATUM_SAMPLES} → fallback使用"
                )
                continue

            n_positive = int(labels[mask].sum())
            if n_positive < 5 or (n_samples - n_positive) < 5:
                logger.debug(
                    f"層 {stratum_name}: クラス不均衡(pos={n_positive}) → fallback使用"
                )
                continue

            cal = self._create_calibrator()
            try:
                cal.fit(scores[mask], labels[mask])
                self._calibrators[stratum_name] = cal
                logger.info(
                    f"層 {stratum_name}: {n_samples}件で学習完了 "
                    f"(的中率={n_positive/n_samples:.1%})"
                )
            except Exception as e:
                logger.warning(f"層 {stratum_name}: 学習失敗 ({e}) → fallback使用")

        logger.info(
            f"層別キャリブレーション完了: "
            f"{len(self._calibrators)}/{len(STRATA)}層が個別学習"
        )

    def predict_proba(
        self,
        score: float,
        track_type: str = "turf",
        distance: int = 1600,
    ) -> float:
        """該当層のキャリブレーターで確率を予測する。

        Args:
            score: GY指数スコア
            track_type: "turf" or "dirt"
            distance: 距離（メートル）

        Returns:
            推定勝率 (0.0〜1.0)
        """
        stratum = get_stratum(track_type, distance)
        cal = self._calibrators.get(stratum, self._fallback)
        if cal is None:
            raise RuntimeError("キャリブレーターが未訓練です。fit()を先に呼び出してください。")
        return cal.predict_proba(score)

    @property
    def strata_info(self) -> dict[str, str]:
        """各層の学習状態を返す（デバッグ用）。"""
        info = {}
        for name in STRATA:
            if name in self._calibrators:
                info[name] = "trained"
            else:
                info[name] = "fallback"
        return info
