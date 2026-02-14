"""特徴量重要度分析モジュール。

各ファクターのPermutation Importance、Hit Rate、Liftを算出し、
ファクターの有効性を定量評価する。
"""

import time
from typing import Any

import numpy as np
from loguru import logger

from src.data.db import DatabaseManager
from src.factors.registry import FactorRegistry
from src.scoring.batch_scorer import BatchScorer


class FeatureImportanceAnalyzer:
    """ファクター重要度分析。

    Permutation Importanceとヒット率統計で
    各ファクターの寄与度を定量評価する。
    """

    def __init__(
        self,
        jvlink_db: DatabaseManager,
        ext_db: DatabaseManager,
    ) -> None:
        self._batch_scorer = BatchScorer(jvlink_db, ext_db)
        self._registry = FactorRegistry(ext_db)

    def analyze(
        self,
        date_from: str = "",
        date_to: str = "",
        max_races: int = 5000,
        target_jyuni: int = 1,
        n_repeats: int = 5,
        progress_callback: Any = None,
    ) -> dict[str, Any]:
        """全ファクターの重要度分析を実行する。

        Args:
            date_from: 分析対象開始日 "YYYYMMDD"
            date_to: 分析対象終了日 "YYYYMMDD"
            max_races: 最大レース数
            target_jyuni: 的中とみなす着順
            n_repeats: Permutation Importanceの繰返し回数

        Returns:
            {
                "factors": [...],
                "n_samples": int,
                "baseline_accuracy": float,
            }
        """
        from sklearn.linear_model import LogisticRegression
        from sklearn.inspection import permutation_importance

        t_start = time.perf_counter()

        if progress_callback:
            progress_callback(0, 3, "ファクター行列を構築中...")
        matrix = self._batch_scorer.build_factor_matrix(
            date_from, date_to, max_races, progress_callback=progress_callback
        )

        X = matrix["X"]
        jyuni = matrix["jyuni"]
        factor_names = matrix["factor_names"]

        y = (jyuni <= target_jyuni).astype(np.int64)

        logger.info(
            f"特徴量重要度分析開始: {len(y)}サンプル, "
            f"{len(factor_names)}ファクター"
        )

        if progress_callback:
            progress_callback(1, 3, "ベースラインモデル学習中...")

        # ベースラインモデル
        model = LogisticRegression(
            C=1.0,
            max_iter=1000,
            solver="lbfgs",
            class_weight="balanced",
        )
        model.fit(X, y)
        baseline_acc = float(model.score(X, y))

        if progress_callback:
            progress_callback(2, 3, "Permutation Importance計算中...")

        # Permutation Importance
        perm_result = permutation_importance(
            model, X, y,
            n_repeats=n_repeats,
            random_state=42,
            scoring="accuracy",
        )

        # 現在のweight取得
        rules = self._registry.get_active_rules()
        weight_map = {r["rule_name"]: r.get("weight", 1.0) for r in rules}
        category_map = {r["rule_name"]: r.get("category", "") for r in rules}

        # 各ファクターの統計を計算
        factors_info = []
        for i, name in enumerate(factor_names):
            col = X[:, i]
            perm_imp = float(perm_result.importances_mean[i])

            # Hit Rate分析
            hit_stats = self._calc_hit_rate(col, y)

            # 相関（ファクター値と着順の逆相関 = 良い指標）
            if np.std(col) > 0 and np.std(jyuni) > 0:
                correlation = float(np.corrcoef(col, -jyuni)[0, 1])
            else:
                correlation = 0.0

            factors_info.append({
                "rule_name": name,
                "category": category_map.get(name, ""),
                "current_weight": weight_map.get(name, 1.0),
                "permutation_importance": perm_imp,
                "hit_rate_with": hit_stats["hit_rate_with"],
                "hit_rate_without": hit_stats["hit_rate_without"],
                "lift": hit_stats["lift"],
                "activation_rate": hit_stats["activation_rate"],
                "correlation": correlation,
            })

        # ファクター別の詳細をログ出力
        logger.info("  ファクター別重要度詳細:")
        for fi in factors_info:
            logger.info(
                f"    {fi['rule_name']}: PI={fi['permutation_importance']:.4f}, "
                f"hit_rate_with={fi['hit_rate_with']:.3f}, "
                f"lift={fi['lift']:.2f}, "
                f"activation_rate={fi['activation_rate']:.3f}"
            )

        # 重要度降順にソート
        factors_info.sort(
            key=lambda x: x["permutation_importance"], reverse=True
        )

        # Top3ファクターをログ出力
        top3 = factors_info[:3]
        top3_names = [f"{f['rule_name']}(PI={f['permutation_importance']:.4f})" for f in top3]
        logger.info(f"  Top3重要ファクター: {', '.join(top3_names)}")

        elapsed = time.perf_counter() - t_start
        logger.info(
            f"特徴量重要度分析完了: baseline_accuracy={baseline_acc:.3f}, "
            f"elapsed={elapsed:.2f}s"
        )

        return {
            "factors": factors_info,
            "n_samples": len(y),
            "baseline_accuracy": baseline_acc,
        }

    @staticmethod
    def _calc_hit_rate(
        factor_col: np.ndarray,
        labels: np.ndarray,
    ) -> dict[str, float]:
        """ファクター発火/非発火時のヒット率を計算する。"""
        active_mask = factor_col > 0
        inactive_mask = ~active_mask

        n_active = int(active_mask.sum())
        n_inactive = int(inactive_mask.sum())
        n_total = len(labels)

        activation_rate = n_active / n_total if n_total > 0 else 0.0

        if n_active > 0:
            hit_rate_with = float(labels[active_mask].mean())
        else:
            hit_rate_with = 0.0

        if n_inactive > 0:
            hit_rate_without = float(labels[inactive_mask].mean())
        else:
            hit_rate_without = 0.0

        if hit_rate_without > 0:
            lift = hit_rate_with / hit_rate_without
        else:
            lift = 0.0

        return {
            "hit_rate_with": hit_rate_with,
            "hit_rate_without": hit_rate_without,
            "lift": lift,
            "activation_rate": activation_rate,
        }
