"""バージョン管理機能の単体テスト。

スナップショット作成・復元・履歴取得、
自動アーカイブ（Weight変更/ステータス遷移時）をテストする。
"""

import pytest

from src.data.db import DatabaseManager
from src.factors.registry import FactorRegistry


def _create_approved_rule(registry: FactorRegistry, name: str = "テストルール") -> int:
    """APPROVEDステータスのルールを作成するヘルパー。"""
    rule_id = registry.create_rule({"rule_name": name, "category": "test"})
    registry.transition_status(rule_id, "TESTING", reason="検証開始")
    registry.transition_status(rule_id, "APPROVED", reason="検証合格")
    return rule_id


@pytest.mark.unit
class TestVersionControl:
    """バージョン管理のテスト。"""

    def test_create_snapshot(self, initialized_db: DatabaseManager) -> None:
        """スナップショット作成が正常に動作すること。"""
        registry = FactorRegistry(initialized_db)
        registry.create_rule({"rule_name": "ルールA", "category": "test"})
        registry.create_rule({"rule_name": "ルールB", "category": "test"})

        snap_id = registry.create_snapshot("v1.0.0", description="初期バージョン")
        assert snap_id > 0

    def test_list_snapshots(self, initialized_db: DatabaseManager) -> None:
        """スナップショット一覧がルール数付きで返ること。"""
        registry = FactorRegistry(initialized_db)
        registry.create_rule({"rule_name": "ルールA", "category": "test"})
        registry.create_rule({"rule_name": "ルールB", "category": "test"})

        registry.create_snapshot("v1.0.0")
        snapshots = registry.list_snapshots()
        assert len(snapshots) == 1
        assert snapshots[0]["version_label"] == "v1.0.0"
        assert snapshots[0]["rule_count"] == 2

    def test_list_snapshots_with_counts(self, initialized_db: DatabaseManager) -> None:
        """複数スナップショットのルール数が正しいこと。"""
        registry = FactorRegistry(initialized_db)
        registry.create_rule({"rule_name": "ルール1", "category": "test"})
        registry.create_snapshot("v1.0.0")

        registry.create_rule({"rule_name": "ルール2", "category": "test"})
        registry.create_snapshot("v2.0.0")

        snapshots = registry.list_snapshots()
        assert len(snapshots) == 2
        # 新しい順で返る
        assert snapshots[0]["version_label"] == "v2.0.0"
        assert snapshots[0]["rule_count"] == 2
        assert snapshots[1]["version_label"] == "v1.0.0"
        assert snapshots[1]["rule_count"] == 1

    def test_restore_snapshot(self, initialized_db: DatabaseManager) -> None:
        """スナップショットからルールを復元できること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = _create_approved_rule(registry, "復元テスト")

        # Weight=1.0 の状態でスナップショット作成
        snap_id = registry.create_snapshot("v1.0.0", description="初期状態")

        # Weightを変更
        registry.update_weight(rule_id, 2.5, reason="最適化結果")
        rules_after = initialized_db.execute_query(
            "SELECT weight FROM factor_rules WHERE rule_id = ?", (rule_id,)
        )
        assert rules_after[0]["weight"] == 2.5

        # スナップショットから復元
        restored = registry.restore_snapshot(snap_id)
        assert restored > 0

        # 復元後のWeightが元に戻っていること
        rules_restored = initialized_db.execute_query(
            "SELECT weight FROM factor_rules WHERE rule_id = ?", (rule_id,)
        )
        assert rules_restored[0]["weight"] == 1.0

    def test_restore_creates_pre_restore_snapshot(self, initialized_db: DatabaseManager) -> None:
        """復元前に自動バックアップスナップショットが作成されること。"""
        registry = FactorRegistry(initialized_db)
        registry.create_rule({"rule_name": "自動BKテスト", "category": "test"})
        snap_id = registry.create_snapshot("v1.0.0")

        # 復元前のスナップショット数
        before_count = len(registry.list_snapshots())

        # 復元実行（内部でpre-restoreスナップショットが作られる）
        registry.restore_snapshot(snap_id)

        after_count = len(registry.list_snapshots())
        # 元の1つ + pre-restore の1つ = +1
        assert after_count == before_count + 1

        # 最新のスナップショットがpre-restoreであること
        latest = registry.list_snapshots()[0]
        assert "pre-restore" in latest["version_label"]

    def test_restore_nonexistent_snapshot_raises(self, initialized_db: DatabaseManager) -> None:
        """存在しないスナップショットの復元がエラーになること。"""
        registry = FactorRegistry(initialized_db)
        with pytest.raises(ValueError, match="not found or empty"):
            registry.restore_snapshot(9999)

    def test_get_rule_history(self, initialized_db: DatabaseManager) -> None:
        """ルールのアーカイブ履歴を取得できること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = registry.create_rule({"rule_name": "履歴テスト", "category": "test"})

        # 2回スナップショット作成（異なるWeight）
        registry.create_snapshot("v1.0.0")
        registry.update_weight(rule_id, 2.0, reason="変更1")
        registry.create_snapshot("v2.0.0")

        history = registry.get_rule_history(rule_id)
        assert len(history) >= 2
        # バージョンラベルが含まれていること
        labels = [h.get("version_label") for h in history]
        assert "v2.0.0" in labels
        assert "v1.0.0" in labels

    def test_auto_archive_on_weight_update(self, initialized_db: DatabaseManager) -> None:
        """Weight変更時に自動アーカイブされること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = registry.create_rule({"rule_name": "自動アーカイブテスト", "category": "test"})

        # Weight変更（auto-archiveが走る）
        registry.update_weight(rule_id, 2.0, reason="テスト変更")

        # factor_rules_archiveにレコードが存在すること
        archives = initialized_db.execute_query(
            "SELECT * FROM factor_rules_archive WHERE rule_id = ?", (rule_id,)
        )
        assert len(archives) >= 1
        # アーカイブされたWeightは変更前の1.0であること
        assert archives[0]["weight"] == 1.0

    def test_auto_archive_on_status_transition(self, initialized_db: DatabaseManager) -> None:
        """ステータス遷移時に自動アーカイブされること。"""
        registry = FactorRegistry(initialized_db)
        rule_id = registry.create_rule({"rule_name": "遷移アーカイブテスト", "category": "test"})

        # DRAFT → TESTING（auto-archiveが走る）
        registry.transition_status(rule_id, "TESTING", reason="検証開始")

        archives = initialized_db.execute_query(
            "SELECT * FROM factor_rules_archive WHERE rule_id = ?", (rule_id,)
        )
        assert len(archives) >= 1
        # アーカイブされたステータスは遷移前のDRAFTであること
        assert archives[0]["review_status"] == "DRAFT"

    def test_snapshot_empty_db(self, initialized_db: DatabaseManager) -> None:
        """ルールがない状態でもスナップショット作成が成功すること。"""
        registry = FactorRegistry(initialized_db)
        snap_id = registry.create_snapshot("v0.0.0", description="空の状態")
        assert snap_id > 0

        snapshots = registry.list_snapshots()
        assert snapshots[0]["rule_count"] == 0
