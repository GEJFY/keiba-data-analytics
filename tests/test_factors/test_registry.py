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


@pytest.mark.unit
class TestFactorRegistryAsOfDate:
    """get_active_rules(as_of_date) のテスト。"""

    def _create_approved_rule(
        self,
        registry: FactorRegistry,
        db: DatabaseManager,
        rule_name: str,
        training_from: str = "",
        training_to: str = "",
    ) -> int:
        """APPROVEDルールを作成し、訓練期間を設定する。"""
        rule_id = registry.create_rule({
            "rule_name": rule_name,
            "category": "テスト",
            "weight": 1.0,
        })
        registry.transition_status(rule_id, "TESTING", reason="検証開始")
        registry.transition_status(rule_id, "APPROVED", reason="検証合格")
        if training_from or training_to:
            db.execute_write(
                "UPDATE factor_rules SET training_from = ?, training_to = ? WHERE rule_id = ?",
                (training_from, training_to, rule_id),
            )
        return rule_id

    def test_as_of_date_filters_trained_rules(self, initialized_db: DatabaseManager) -> None:
        """as_of_date指定時にtraining_to以降のルールのみ返ること。"""
        registry = FactorRegistry(initialized_db)
        # training_to=2024-06-30 のルール
        id1 = self._create_approved_rule(
            registry, initialized_db, "ルールA",
            training_from="2024-01-01", training_to="2024-06-30",
        )
        # training_to=2024-12-31 のルール
        id2 = self._create_approved_rule(
            registry, initialized_db, "ルールB",
            training_from="2024-07-01", training_to="2024-12-31",
        )

        # as_of_date=2024-07-01 → ルールA(training_to=06-30 < 07-01)は返る、
        # ルールB(training_to=12-31 >= 07-01)は返らない
        rules = registry.get_active_rules(as_of_date="2024-07-01")
        names = {r["rule_name"] for r in rules}
        assert "ルールA" in names
        assert "ルールB" not in names

    def test_as_of_date_includes_null_training(self, initialized_db: DatabaseManager) -> None:
        """training_to未設定のルールはas_of_date指定時も返ること。"""
        registry = FactorRegistry(initialized_db)
        # 訓練期間なし
        id1 = self._create_approved_rule(registry, initialized_db, "未設定ルール")
        # 訓練期間あり（未来）
        id2 = self._create_approved_rule(
            registry, initialized_db, "設定済みルール",
            training_from="2024-01-01", training_to="2025-12-31",
        )

        rules = registry.get_active_rules(as_of_date="2025-01-01")
        names = {r["rule_name"] for r in rules}
        assert "未設定ルール" in names
        assert "設定済みルール" not in names

    def test_as_of_date_none_returns_all(self, initialized_db: DatabaseManager) -> None:
        """as_of_date=Noneの場合、従来動作（全ルール返却）。"""
        registry = FactorRegistry(initialized_db)
        self._create_approved_rule(
            registry, initialized_db, "全取得テスト",
            training_from="2020-01-01", training_to="2099-12-31",
        )
        rules = registry.get_active_rules(as_of_date=None)
        names = {r["rule_name"] for r in rules}
        assert "全取得テスト" in names


@pytest.mark.unit
class TestCheckTrainingOverlap:
    """check_training_overlap() のテスト。"""

    def _create_approved_rule(
        self,
        registry: FactorRegistry,
        db: DatabaseManager,
        rule_name: str,
        training_from: str = "",
        training_to: str = "",
    ) -> int:
        rule_id = registry.create_rule({
            "rule_name": rule_name, "category": "テスト", "weight": 1.0,
        })
        registry.transition_status(rule_id, "TESTING", reason="検証開始")
        registry.transition_status(rule_id, "APPROVED", reason="検証合格")
        if training_from or training_to:
            db.execute_write(
                "UPDATE factor_rules SET training_from = ?, training_to = ? WHERE rule_id = ?",
                (training_from, training_to, rule_id),
            )
        return rule_id

    def test_overlap_detected(self, initialized_db: DatabaseManager) -> None:
        """訓練期間とバックテスト期間の重複が検出されること。"""
        registry = FactorRegistry(initialized_db)
        self._create_approved_rule(
            registry, initialized_db, "重複ルール",
            training_from="2024-01-01", training_to="2024-06-30",
        )
        result = registry.check_training_overlap("2024-03-01", "2024-12-31")
        assert result["has_overlap"] is True
        assert len(result["overlapping_rules"]) == 1
        assert result["overlapping_rules"][0]["rule_name"] == "重複ルール"

    def test_no_overlap(self, initialized_db: DatabaseManager) -> None:
        """重複なしの場合にhas_overlap=Falseになること。"""
        registry = FactorRegistry(initialized_db)
        self._create_approved_rule(
            registry, initialized_db, "安全ルール",
            training_from="2024-01-01", training_to="2024-06-30",
        )
        result = registry.check_training_overlap("2024-07-01", "2024-12-31")
        assert result["has_overlap"] is False
        assert len(result["safe_rules"]) == 1

    def test_no_training_info_classified(self, initialized_db: DatabaseManager) -> None:
        """訓練期間未記録のルールがno_training_infoに分類されること。"""
        registry = FactorRegistry(initialized_db)
        self._create_approved_rule(registry, initialized_db, "未記録ルール")
        result = registry.check_training_overlap("2024-01-01", "2024-12-31")
        assert len(result["no_training_info"]) == 1
        assert result["no_training_info"][0]["rule_name"] == "未記録ルール"
