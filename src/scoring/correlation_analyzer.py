"""ファクター相関分析・感度分析モジュール。

ファクター間の相関を分析して冗長なファクターを検出し、
Weight変動時のROI影響を推定する感度分析を提供する。
"""

import numpy as np
from typing import Any
from loguru import logger
from src.data.db import DatabaseManager
from src.scoring.batch_scorer import BatchScorer


class CorrelationAnalyzer:
    """ファクター間相関分析。"""

    def __init__(self, jvlink_db: DatabaseManager, ext_db: DatabaseManager) -> None:
        self._batch_scorer = BatchScorer(jvlink_db, ext_db)

    def analyze_correlations(
        self, date_from: str = "", date_to: str = "", max_races: int = 5000,
        progress_callback: Any = None,
    ) -> dict[str, Any]:
        """ファクター間の相関行列と冗長性分析を実行する。

        Returns:
            {
                "factor_names": list[str],
                "correlation_matrix": list[list[float]],  # N x N
                "redundant_pairs": list[dict],  # [{"factor_a", "factor_b", "correlation"}]
                "n_samples": int,
            }
        """
        if progress_callback:
            progress_callback(0, 2, "ファクター行列を構築中...")
        matrix = self._batch_scorer.build_factor_matrix(
            date_from, date_to, max_races, progress_callback=progress_callback
        )
        X = matrix["X"]
        factor_names = matrix["factor_names"]

        if progress_callback:
            progress_callback(1, 2, "相関行列を計算中...")

        # 相関行列計算
        # stdが0のカラムはnanになるので0に置換
        with np.errstate(invalid="ignore"):
            corr = np.corrcoef(X.T)
        corr = np.nan_to_num(corr, nan=0.0)

        # 冗長ペア検出 (|r| > 0.7)
        redundant_pairs = []
        for i in range(len(factor_names)):
            for j in range(i + 1, len(factor_names)):
                r = float(corr[i, j])
                if abs(r) > 0.7:
                    redundant_pairs.append({
                        "factor_a": factor_names[i],
                        "factor_b": factor_names[j],
                        "correlation": round(r, 3),
                    })

        redundant_pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)

        logger.info(
            f"相関分析完了: {len(factor_names)}ファクター, "
            f"{len(redundant_pairs)}冗長ペア検出"
        )

        return {
            "factor_names": factor_names,
            "correlation_matrix": corr.tolist(),
            "redundant_pairs": redundant_pairs,
            "n_samples": len(matrix["y"]),
        }

    def sensitivity_analysis(
        self,
        date_from: str = "",
        date_to: str = "",
        max_races: int = 5000,
        weight_deltas: list[float] | None = None,
        progress_callback: Any = None,
    ) -> dict[str, Any]:
        """Weight変動に対するスコア感度を分析する。

        各ファクターのWeightを±変動させた時の
        スコア平均変化量を計算し、感度ヒートマップ用データを返す。

        Args:
            weight_deltas: テストする変動幅リスト（デフォルト: [-50%, -20%, +20%, +50%]）

        Returns:
            {
                "factor_names": list[str],
                "deltas": list[float],  # 変動幅（例: [-0.5, -0.2, 0.2, 0.5]）
                "sensitivity_matrix": list[list[float]],  # N_factors x N_deltas
                "n_samples": int,
            }
        """
        if weight_deltas is None:
            weight_deltas = [-0.5, -0.2, 0.2, 0.5]

        if progress_callback:
            progress_callback(0, 2, "ファクター行列を構築中...")
        matrix = self._batch_scorer.build_factor_matrix(
            date_from, date_to, max_races, progress_callback=progress_callback
        )
        X = matrix["X"]
        factor_names = matrix["factor_names"]

        if progress_callback:
            progress_callback(1, 2, "感度マトリクスを計算中...")

        from src.factors.registry import FactorRegistry
        registry = FactorRegistry(self._batch_scorer._ext_db)
        rules = registry.get_active_rules()
        weight_map = {r["rule_name"]: r.get("weight", 1.0) for r in rules}

        # 各ファクターの各変動幅でのスコア変化
        sensitivity = []
        for i, name in enumerate(factor_names):
            row = []
            current_w = weight_map.get(name, 1.0)
            col = X[:, i]
            for delta in weight_deltas:
                new_w = current_w * (1 + delta)
                # スコア変化 = (new_w - current_w) * factor_raw_value の平均絶対値
                score_change = float(np.mean(np.abs(col * (new_w - current_w))))
                row.append(round(score_change, 3))
            sensitivity.append(row)

        logger.info(f"感度分析完了: {len(factor_names)}ファクター x {len(weight_deltas)}変動幅")

        return {
            "factor_names": factor_names,
            "deltas": weight_deltas,
            "sensitivity_matrix": sensitivity,
            "n_samples": len(matrix["y"]),
        }
