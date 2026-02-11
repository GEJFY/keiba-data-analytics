"""ファクターライフサイクル管理モジュール。

DRAFT → TESTING → APPROVED → DEPRECATED の
ライフサイクル遷移と定期レビューを管理する。
"""

from typing import Any

from loguru import logger

from src.data.db import DatabaseManager
from src.factors.registry import FactorRegistry


class FactorLifecycleManager:
    """ファクタールールのライフサイクルを管理するクラス。"""

    # 劣化検知の閾値
    DECAY_THRESHOLD = 0.3
    # 有効性スコアの最低閾値
    MIN_VALIDATION_SCORE = 0.5

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db
        self._registry = FactorRegistry(db)

    def detect_decayed_rules(self) -> list[dict[str, Any]]:
        """有効性が劣化したルールを検出する。"""
        rules = self._registry.get_rules_by_status("APPROVED")
        decayed = []
        for rule in rules:
            if rule.get("decay_rate") and rule["decay_rate"] > self.DECAY_THRESHOLD:
                decayed.append(rule)
                logger.warning(
                    f"ルール劣化検知: {rule['rule_name']} "
                    f"(decay_rate={rule['decay_rate']:.3f})"
                )
        return decayed

    def batch_deprecate(self, min_score: float | None = None) -> int:
        """有効性スコアが閾値を下回るルールを一括DEPRECATED化する。"""
        threshold = min_score or self.MIN_VALIDATION_SCORE
        rules = self._registry.get_rules_by_status("APPROVED")
        count = 0
        for rule in rules:
            if rule.get("validation_score") is not None and rule["validation_score"] < threshold:
                self._registry.transition_status(
                    rule["rule_id"],
                    "DEPRECATED",
                    reason=f"有効性スコア {rule['validation_score']:.3f} < 閾値 {threshold}",
                    changed_by="auto_review",
                )
                count += 1
        logger.info(f"一括DEPRECATED化: {count}件のルールを無効化")
        return count
