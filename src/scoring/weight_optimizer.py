"""ファクターWeight最適化モジュール。

LogisticRegressionで過去レースの的中データから
各ファクターの最適Weightを算出する。
"""

import time
from typing import Any

import numpy as np
from loguru import logger

from src.data.db import DatabaseManager
from src.factors.registry import FactorRegistry
from src.scoring.batch_scorer import BatchScorer


class WeightOptimizer:
    """LogisticRegressionベースのファクターWeight最適化。

    BatchScorerで構築したファクター行列に対して
    LogisticRegressionを適用し、各ファクターの係数から
    最適なWeightを算出する。
    """

    # Weight正規化の上限値
    MAX_WEIGHT = 3.0

    def __init__(
        self,
        jvlink_db: DatabaseManager,
        ext_db: DatabaseManager,
    ) -> None:
        self._batch_scorer = BatchScorer(jvlink_db, ext_db)
        self._registry = FactorRegistry(ext_db)

    def optimize(
        self,
        date_from: str = "",
        date_to: str = "",
        max_races: int = 5000,
        target_jyuni: int = 1,
        regularization: float = 1.0,
        progress_callback: Any = None,
    ) -> dict[str, Any]:
        """最適Weightを算出する。

        Args:
            date_from: 訓練データ開始日 "YYYYMMDD"
            date_to: 訓練データ終了日 "YYYYMMDD"
            max_races: 最大レース数
            target_jyuni: 的中とみなす着順（1=1着, 3=3着以内）
            regularization: L2正則化の強さ（sklearn C parameter、大きいほど弱い正則化）

        Returns:
            {
                "weights": dict[str, float],  # rule_name -> optimized_weight
                "current_weights": dict[str, float],
                "accuracy": float,
                "log_loss": float,
                "n_samples": int,
                "n_positive": int,
                "feature_coefs": dict[str, float],
            }

        Raises:
            ValueError: 訓練データ不足
        """
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score, log_loss

        t_start = time.perf_counter()

        # ファクター行列を構築
        if progress_callback:
            progress_callback(0, 3, "ファクター行列を構築中...")
        matrix = self._batch_scorer.build_factor_matrix(
            date_from, date_to, max_races, progress_callback=progress_callback
        )

        X = matrix["X"]
        jyuni = matrix["jyuni"]
        factor_names = matrix["factor_names"]

        # ラベル: target_jyuni着以内=1
        y = (jyuni <= target_jyuni).astype(np.int64)

        n_positive = int(y.sum())
        if n_positive < 10:
            raise ValueError(
                f"的中サンプル数不足: {n_positive}件 (最低10件必要)"
            )

        n_negative = len(y) - n_positive
        logger.info(
            f"Weight最適化開始: {len(y)}サンプル, "
            f"的中率={n_positive/len(y):.1%}, "
            f"{len(factor_names)}ファクター"
        )
        logger.info(
            f"  クラスバランス: positive={n_positive}, "
            f"negative={n_negative}, ratio=1:{n_negative/max(n_positive,1):.1f}"
        )

        if progress_callback:
            progress_callback(1, 3, "LogisticRegression学習中...")

        # LogisticRegression
        model = LogisticRegression(
            C=regularization,
            max_iter=1000,
            solver="lbfgs",
            class_weight="balanced",
        )
        logger.info(
            f"  モデルパラメータ: C={regularization}, solver=lbfgs, "
            f"max_iter=1000, class_weight=balanced"
        )
        model.fit(X, y)

        # 予測
        y_pred = model.predict(X)
        y_prob = model.predict_proba(X)[:, 1]
        acc = float(accuracy_score(y, y_pred))
        ll = float(log_loss(y, y_prob))

        if progress_callback:
            progress_callback(2, 3, "最適Weight算出中...")

        # 係数からWeightに変換
        coefs = model.coef_[0]
        raw_coefs = {name: float(c) for name, c in zip(factor_names, coefs)}
        optimized_weights = self._normalize_coefs(coefs, factor_names)

        # ファクター別の結果をログ出力
        logger.info("  ファクター別最適化結果:")
        for name in factor_names:
            logger.info(
                f"    {name}: raw_coef={raw_coefs[name]:+.4f}, "
                f"normalized_weight={optimized_weights[name]:.2f}"
            )

        # 現在のweight取得
        rules = self._registry.get_active_rules()
        current_weights = {r["rule_name"]: r.get("weight", 1.0) for r in rules}

        elapsed = time.perf_counter() - t_start
        logger.info(
            f"Weight最適化完了: accuracy={acc:.3f}, log_loss={ll:.4f}, "
            f"elapsed={elapsed:.2f}s"
        )

        return {
            "weights": optimized_weights,
            "current_weights": current_weights,
            "accuracy": acc,
            "log_loss": ll,
            "n_samples": len(y),
            "n_positive": n_positive,
            "feature_coefs": raw_coefs,
            "training_from": date_from,
            "training_to": date_to,
        }

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        """YYYYMMDD → YYYY-MM-DD に正規化する。"""
        if not date_str:
            return ""
        s = date_str.strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        return s

    def apply_weights(
        self,
        optimized_weights: dict[str, float],
        changed_by: str = "optimizer",
        training_from: str = "",
        training_to: str = "",
    ) -> int:
        """最適化結果をDBに反映する。

        Args:
            optimized_weights: rule_name -> new_weight のdict
            changed_by: 変更者名
            training_from: 訓練データ開始日 (YYYYMMDD or YYYY-MM-DD)
            training_to: 訓練データ終了日 (YYYYMMDD or YYYY-MM-DD)

        Returns:
            更新したルール数
        """
        rules = self._registry.get_active_rules()
        updated = 0

        train_from_iso = self._normalize_date(training_from)
        train_to_iso = self._normalize_date(training_to)

        for rule in rules:
            name = rule["rule_name"]
            if name not in optimized_weights:
                continue

            new_weight = optimized_weights[name]
            old_weight = rule.get("weight", 1.0)

            if abs(new_weight - old_weight) < 0.01:
                continue

            reason = f"ML最適化: {old_weight:.2f} → {new_weight:.2f}"
            if train_from_iso and train_to_iso:
                reason += f" (訓練期間: {train_from_iso}〜{train_to_iso})"

            self._registry.update_weight(
                rule["rule_id"],
                new_weight,
                reason=reason,
                changed_by=changed_by,
            )

            # 訓練期間をDBに記録
            if train_from_iso or train_to_iso:
                self._registry._db.execute_write(
                    "UPDATE factor_rules SET training_from = ?, training_to = ? WHERE rule_id = ?",
                    (train_from_iso, train_to_iso, rule["rule_id"]),
                )

            updated += 1

        logger.info(
            f"Weight適用完了: {updated}ルール更新"
            + (f" (訓練期間: {train_from_iso}〜{train_to_iso})" if train_from_iso else "")
        )
        return updated

    def _normalize_coefs(
        self,
        coefs: np.ndarray,
        factor_names: list[str],
    ) -> dict[str, float]:
        """LogisticRegression係数をWeight（0.0〜MAX_WEIGHT）に正規化する。"""
        abs_coefs = np.abs(coefs)
        max_abs = abs_coefs.max() if abs_coefs.max() > 0 else 1.0

        weights = {}
        for name, coef in zip(factor_names, coefs):
            # 正の係数 → 正のweight、負の係数 → 0に近いweight
            # 係数の絶対値で重要度を表現し、符号で方向性を維持
            normalized = abs(coef) / max_abs * self.MAX_WEIGHT
            # 負の係数は小さいweightに（ファクター式自体が正負を含むため、
            # weightは正の値で維持し、寄与度の大きさのみ反映する）
            weights[name] = round(max(0.1, normalized), 2)

        return weights
