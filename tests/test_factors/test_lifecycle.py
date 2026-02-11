"""ファクターライフサイクル管理の単体テスト。"""

import pytest

from src.data.db import DatabaseManager
from src.factors.lifecycle import FactorLifecycleManager
from src.factors.registry import FactorRegistry


@pytest.fixture
def lifecycle_db(initialized_db: DatabaseManager) -> DatabaseManager:
    """ライフサイクルテスト用のデータが投入されたDBを返す。"""
    registry = FactorRegistry(initialized_db)

    # ルール1: 正常なAPPROVEDルール
    rule_id1 = registry.create_rule({"rule_name": "speed_rule", "category": "speed"})
    registry.transition_status(rule_id1, "TESTING", reason="テスト開始")
    registry.transition_status(rule_id1, "APPROVED", reason="承認")

    # ルール2: 劣化したAPPROVEDルール（decay_rate高）
    rule_id2 = registry.create_rule({"rule_name": "decayed_rule", "category": "pace"})
    registry.transition_status(rule_id2, "TESTING", reason="テスト開始")
    registry.transition_status(rule_id2, "APPROVED", reason="承認")
    # decay_rateとvalidation_scoreを手動設定
    initialized_db.execute_write(
        "UPDATE factor_rules SET decay_rate = 0.5, validation_score = 0.3 WHERE rule_id = ?",
        (rule_id2,),
    )

    # ルール3: バリデーションスコアが低いAPPROVEDルール
    rule_id3 = registry.create_rule({"rule_name": "low_score_rule", "category": "form"})
    registry.transition_status(rule_id3, "TESTING", reason="テスト開始")
    registry.transition_status(rule_id3, "APPROVED", reason="承認")
    initialized_db.execute_write(
        "UPDATE factor_rules SET validation_score = 0.4 WHERE rule_id = ?",
        (rule_id3,),
    )

    # ルール4: バリデーションスコアが十分なAPPROVEDルール
    rule_id4 = registry.create_rule({"rule_name": "good_rule", "category": "course"})
    registry.transition_status(rule_id4, "TESTING", reason="テスト開始")
    registry.transition_status(rule_id4, "APPROVED", reason="承認")
    initialized_db.execute_write(
        "UPDATE factor_rules SET validation_score = 0.8 WHERE rule_id = ?",
        (rule_id4,),
    )

    return initialized_db


class TestFactorLifecycleManager:
    """FactorLifecycleManagerクラスのテスト。"""

    def test_detect_decayed_rules(self, lifecycle_db: DatabaseManager) -> None:
        """劣化ルールが正しく検出されること。"""
        manager = FactorLifecycleManager(lifecycle_db)
        decayed = manager.detect_decayed_rules()
        assert len(decayed) == 1
        assert decayed[0]["rule_name"] == "decayed_rule"

    def test_detect_decayed_rules_none(self, initialized_db: DatabaseManager) -> None:
        """劣化ルールがない場合、空リストを返すこと。"""
        manager = FactorLifecycleManager(initialized_db)
        decayed = manager.detect_decayed_rules()
        assert decayed == []

    def test_batch_deprecate(self, lifecycle_db: DatabaseManager) -> None:
        """バリデーションスコアが低いルールが一括DEPRECATED化されること。"""
        manager = FactorLifecycleManager(lifecycle_db)
        count = manager.batch_deprecate()
        assert count == 2  # decayed_rule(0.3)とlow_score_rule(0.4)

        # DEPRECATED化されたことを確認
        registry = FactorRegistry(lifecycle_db)
        deprecated = registry.get_rules_by_status("DEPRECATED")
        names = {r["rule_name"] for r in deprecated}
        assert "decayed_rule" in names
        assert "low_score_rule" in names

    def test_batch_deprecate_custom_threshold(self, lifecycle_db: DatabaseManager) -> None:
        """カスタム閾値で一括DEPRECATED化できること。"""
        manager = FactorLifecycleManager(lifecycle_db)
        count = manager.batch_deprecate(min_score=0.35)
        assert count == 1  # decayed_rule(0.3)のみ

    def test_batch_deprecate_no_targets(self, lifecycle_db: DatabaseManager) -> None:
        """閾値が非常に低い場合、対象なしで0を返すこと。"""
        manager = FactorLifecycleManager(lifecycle_db)
        count = manager.batch_deprecate(min_score=0.01)
        assert count == 0

    def test_decay_threshold_constant(self) -> None:
        """劣化閾値のデフォルト値が正しいこと。"""
        assert FactorLifecycleManager.DECAY_THRESHOLD == 0.3

    def test_min_validation_score_constant(self) -> None:
        """有効性スコア閾値のデフォルト値が正しいこと。"""
        assert FactorLifecycleManager.MIN_VALIDATION_SCORE == 0.5
