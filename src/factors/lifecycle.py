"""ファクターライフサイクル管理モジュール。

DRAFT → TESTING → APPROVED → DEPRECATED の
ライフサイクル遷移と定期レビューを管理する。

定期レビューでは以下を検知:
- decay_rate（劣化率）が閾値を超えたルール
- validation_score（有効性スコア）が閾値を下回ったルール
"""

from typing import Any

from loguru import logger

from src.data.db import DatabaseManager
from src.factors.registry import FactorRegistry


class FactorLifecycleManager:
    """ファクタールールのライフサイクルを管理するクラス。

    定期的にAPPROVEDルールの有効性を評価し、
    基準を下回ったルールを自動的にDEPRECATED化する。
    """

    # 劣化検知の閾値（decay_rateがこの値を超えると警告）
    DECAY_THRESHOLD = 0.3
    # 有効性スコアの最低閾値（これを下回るとDEPRECATED化対象）
    MIN_VALIDATION_SCORE = 0.5

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db
        self._registry = FactorRegistry(db)

    def detect_decayed_rules(self) -> list[dict[str, Any]]:
        """有効性が劣化したルールを検出する。

        Returns:
            decay_rateがDECAY_THRESHOLDを超えたAPPROVEDルールのリスト
        """
        rules = self._registry.get_rules_by_status("APPROVED")
        decayed = []
        for rule in rules:
            decay_rate = rule.get("decay_rate")
            if decay_rate is not None and decay_rate > self.DECAY_THRESHOLD:
                decayed.append(rule)
                logger.warning(
                    f"ルール劣化検知: {rule['rule_name']} "
                    f"(decay_rate={decay_rate:.3f}, 閾値={self.DECAY_THRESHOLD})"
                )

        if decayed:
            logger.info(f"劣化ルール検出: {len(decayed)}件")
        return decayed

    def batch_deprecate(self, min_score: float | None = None) -> int:
        """有効性スコアが閾値を下回るルールを一括DEPRECATED化する。

        Args:
            min_score: カスタム閾値（未指定時はMIN_VALIDATION_SCORE）

        Returns:
            DEPRECATED化したルール件数
        """
        threshold = min_score if min_score is not None else self.MIN_VALIDATION_SCORE
        rules = self._registry.get_rules_by_status("APPROVED")
        count = 0

        for rule in rules:
            score = rule.get("validation_score")
            if score is not None and score < threshold:
                self._registry.transition_status(
                    rule["rule_id"],
                    "DEPRECATED",
                    reason=f"有効性スコア {score:.3f} < 閾値 {threshold}",
                    changed_by="auto_review",
                )
                count += 1
                logger.info(f"DEPRECATED化: {rule['rule_name']} (score={score:.3f})")

        logger.info(f"一括DEPRECATED化完了: {count}件のルールを無効化 (閾値={threshold})")
        return count
