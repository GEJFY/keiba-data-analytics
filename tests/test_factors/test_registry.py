"""ファクターレジストリの単体テスト。"""

import pytest

from src.data.db import DatabaseManager
from src.factors.registry import FactorRegistry


@pytest.mark.unit
class TestFactorRegistry:
    """FactorRegistryクラスのテスト。"""

    def test_create_rule(self, initialized_db: DatabaseManager) -> None:
        """ルール作成が正常に動作すること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = registry.create_rule({
            "rule_name": "前走僅差2着加点",
            "category": "過去レース評価",
            "description": "前走で鼻差〜クビ差の2着馬に加点",
            "sql_expression": "CASE WHEN prev_rank = 2 AND margin < 0.2 THEN 1 ELSE 0 END",
            "weight": 1.5,
            "source": "manual",
        })
        assert rule_id > 0

    def test_create_rule_default_status_is_draft(self, initialized_db: DatabaseManager) -> None:
        """新規ルールのデフォルトステータスがDRAFTであること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = registry.create_rule({
            "rule_name": "テストルール",
            "category": "テスト",
        })
        rules = registry.get_rules_by_status("DRAFT")
        assert any(r["rule_id"] == rule_id for r in rules)

    def test_transition_draft_to_testing(self, initialized_db: DatabaseManager) -> None:
        """DRAFT → TESTINGの遷移が正常に動作すること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = registry.create_rule({"rule_name": "遷移テスト", "category": "テスト"})
        registry.transition_status(rule_id, "TESTING", reason="バックテスト検証開始")
        rules = registry.get_rules_by_status("TESTING")
        assert any(r["rule_id"] == rule_id for r in rules)

    def test_transition_testing_to_approved(self, initialized_db: DatabaseManager) -> None:
        """TESTING → APPROVEDの遷移が正常に動作すること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = registry.create_rule({"rule_name": "承認テスト", "category": "テスト"})
        registry.transition_status(rule_id, "TESTING", reason="検証開始")
        registry.transition_status(rule_id, "APPROVED", reason="検証合格")
        rules = registry.get_rules_by_status("APPROVED")
        assert any(r["rule_id"] == rule_id for r in rules)

    def test_invalid_transition_raises_error(self, initialized_db: DatabaseManager) -> None:
        """不正なステータス遷移がエラーになること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = registry.create_rule({"rule_name": "不正遷移テスト", "category": "テスト"})
        with pytest.raises(ValueError, match="許可されていません"):
            registry.transition_status(rule_id, "APPROVED", reason="直接承認は不可")

    def test_update_weight(self, initialized_db: DatabaseManager) -> None:
        """重み更新が正常に動作すること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = registry.create_rule({"rule_name": "重みテスト", "category": "テスト", "weight": 1.0})
        registry.update_weight(rule_id, 2.5, reason="バックテスト結果に基づく調整")

        rules = initialized_db.execute_query(
            "SELECT weight FROM factor_rules WHERE rule_id = ?", (rule_id,)
        )
        assert rules[0]["weight"] == 2.5

    def test_change_log_recorded(self, initialized_db: DatabaseManager) -> None:
        """変更履歴がfactor_review_logに記録されること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = registry.create_rule({"rule_name": "ログテスト", "category": "テスト"})

        logs = initialized_db.execute_query(
            "SELECT * FROM factor_review_log WHERE rule_id = ?", (rule_id,)
        )
        assert len(logs) >= 1
        assert logs[0]["action"] == "CREATED"
