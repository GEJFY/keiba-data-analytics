"""ファクタールール登録・管理・CRUDモジュール。

factor_rulesテーブルとfactor_review_logテーブルを通じて
ファクタールールのライフサイクルを管理する。

ルールの状態遷移:
    DRAFT → TESTING → APPROVED → DEPRECATED
"""

from datetime import UTC, datetime
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager

# 許可されるステータス遷移マップ
_VALID_TRANSITIONS: dict[str, list[str]] = {
    "DRAFT": ["TESTING"],
    "TESTING": ["APPROVED", "DRAFT"],
    "APPROVED": ["DEPRECATED"],
    "DEPRECATED": ["DRAFT"],  # 再検討のためDRAFTに戻せる
}

# ステータス遷移時のアクションマッピング
_STATUS_ACTION_MAP: dict[str, str] = {
    "TESTING": "ACTIVATED",
    "APPROVED": "ACTIVATED",
    "DRAFT": "DEACTIVATED",
    "DEPRECATED": "DEPRECATED",
}


class FactorRegistry:
    """ファクタールールの登録・管理クラス。

    全てのルール操作はfactor_review_logに変更履歴として記録される。
    """

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    def get_active_rules(self, as_of_date: str | None = None) -> list[dict[str, Any]]:
        """有効な（APPROVED かつ is_active = 1 かつ有効期間内の）ルールを取得する。

        Args:
            as_of_date: 時点日 (YYYY-MM-DD)。指定時は training_to < as_of_date の
                       ルールのみ返す（データリーケージ防止）。
                       training_to が NULL のルールは含める（訓練期間未記録）。
                       Noneの場合は現在時刻でフィルタ（既存動作）。

        Returns:
            有効なルールのdictリスト（category, rule_id順）
        """
        if as_of_date is None:
            now = datetime.now(UTC).isoformat()
            return self._db.execute_query(
                """
                SELECT * FROM factor_rules
                WHERE review_status = 'APPROVED'
                  AND is_active = 1
                  AND (effective_from IS NULL OR effective_from <= ?)
                  AND (effective_to IS NULL OR effective_to >= ?)
                ORDER BY category, rule_id
                """,
                (now, now),
            )

        return self._db.execute_query(
            """
            SELECT * FROM factor_rules
            WHERE review_status = 'APPROVED'
              AND is_active = 1
              AND (effective_from IS NULL OR effective_from <= ?)
              AND (effective_to IS NULL OR effective_to >= ?)
              AND (training_to IS NULL OR training_to < ?)
            ORDER BY category, rule_id
            """,
            (as_of_date, as_of_date, as_of_date),
        )

    def check_training_overlap(
        self, backtest_from: str, backtest_to: str,
    ) -> dict[str, Any]:
        """バックテスト期間と訓練期間の重複をチェックする。

        Args:
            backtest_from: バックテスト開始日 (YYYY-MM-DD)
            backtest_to: バックテスト終了日 (YYYY-MM-DD)

        Returns:
            重複情報を含むdict:
            - has_overlap: 重複があるかどうか
            - overlapping_rules: 重複ルールのリスト
            - safe_rules: 安全なルールのリスト
            - no_training_info: 訓練期間未記録のルールのリスト
        """
        all_rules = self._db.execute_query(
            """SELECT * FROM factor_rules
               WHERE review_status = 'APPROVED' AND is_active = 1
               ORDER BY rule_id""",
        )

        overlapping: list[dict[str, Any]] = []
        safe: list[dict[str, Any]] = []
        no_training: list[dict[str, Any]] = []

        for rule in all_rules:
            t_from = rule.get("training_from")
            t_to = rule.get("training_to")

            if not t_from or not t_to:
                no_training.append(rule)
                continue

            # 区間重複判定: [t_from, t_to] ∩ [bt_from, bt_to] ≠ ∅
            if t_from <= backtest_to and t_to >= backtest_from:
                overlapping.append(rule)
            else:
                safe.append(rule)

        return {
            "has_overlap": len(overlapping) > 0,
            "overlapping_rules": overlapping,
            "safe_rules": safe,
            "no_training_info": no_training,
        }

    def get_rules_by_status(self, status: str) -> list[dict[str, Any]]:
        """ステータスでルールをフィルタリングする。

        Args:
            status: DRAFT / TESTING / APPROVED / DEPRECATED

        Returns:
            該当ステータスのルールリスト
        """
        return self._db.execute_query(
            "SELECT * FROM factor_rules WHERE review_status = ? ORDER BY rule_id",
            (status,),
        )

    def create_rule(self, rule_data: dict[str, Any]) -> int:
        """新規ルールをDRAFTとして作成する。

        Args:
            rule_data: ルール情報（rule_name必須、category/description/weight等は任意）

        Returns:
            作成されたルールのrule_id

        Raises:
            KeyError: rule_nameが含まれていない場合
        """
        if "rule_name" not in rule_data:
            raise KeyError("rule_dataに'rule_name'が必須です")

        now = datetime.now(UTC).isoformat()
        insert_sql = """
            INSERT INTO factor_rules
            (rule_name, category, description, sql_expression, weight,
             is_active, created_at, updated_at, source, review_status, min_sample_size)
            VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, 'DRAFT', ?)
        """
        params = (
            rule_data["rule_name"],
            rule_data.get("category", ""),
            rule_data.get("description", ""),
            rule_data.get("sql_expression", ""),
            rule_data.get("weight", 1.0),
            now,
            now,
            rule_data.get("source", "manual"),
            rule_data.get("min_sample_size", 100),
        )

        # INSERTとlast_insert_rowid()を同一接続内で実行
        with self._db.connect() as conn:
            conn.execute(insert_sql, params)
            cursor = conn.execute("SELECT last_insert_rowid() as id")
            row = cursor.fetchone()
            rule_id = row[0] if row else 0

        self._log_change(rule_id, "CREATED", reason="新規ルール作成", changed_by=rule_data.get("changed_by", "user"))
        logger.info(f"ルール作成: {rule_data['rule_name']} (ID: {rule_id})")
        return rule_id

    def update_weight(self, rule_id: int, new_weight: float, reason: str, changed_by: str = "user") -> None:
        """ルールの重みを更新する。

        Args:
            rule_id: 対象ルールID
            new_weight: 新しい重み値
            reason: 変更理由
            changed_by: 変更者名
        """
        self._archive_rule(rule_id)  # 変更前の状態を保存

        old = self._db.execute_query("SELECT weight FROM factor_rules WHERE rule_id = ?", (rule_id,))
        old_weight = old[0]["weight"] if old else 0.0

        now = datetime.now(UTC).isoformat()
        self._db.execute_write(
            "UPDATE factor_rules SET weight = ?, updated_at = ? WHERE rule_id = ?",
            (new_weight, now, rule_id),
        )
        self._log_change(
            rule_id, "UPDATED", old_weight=old_weight, new_weight=new_weight, reason=reason, changed_by=changed_by
        )
        logger.info(f"ルール {rule_id}: weight {old_weight} → {new_weight} ({reason})")

    def transition_status(self, rule_id: int, new_status: str, reason: str, changed_by: str = "user") -> None:
        """ルールのステータスを遷移する。

        Args:
            rule_id: 対象ルールID
            new_status: 遷移先ステータス
            reason: 遷移理由
            changed_by: 変更者名

        Raises:
            ValueError: ルールが存在しない、または不正な遷移の場合
        """
        current = self._db.execute_query(
            "SELECT review_status FROM factor_rules WHERE rule_id = ?", (rule_id,)
        )
        if not current:
            raise ValueError(f"ルールID {rule_id} が見つかりません")

        current_status = current[0]["review_status"]
        if new_status not in _VALID_TRANSITIONS.get(current_status, []):
            raise ValueError(f"ステータス遷移 {current_status} → {new_status} は許可されていません")

        self._archive_rule(rule_id)  # 変更前の状態を保存

        now = datetime.now(UTC).isoformat()
        is_active = 1 if new_status == "APPROVED" else 0
        self._db.execute_write(
            """
            UPDATE factor_rules
            SET review_status = ?, is_active = ?, reviewed_at = ?, updated_at = ?
            WHERE rule_id = ?
            """,
            (new_status, is_active, now, now, rule_id),
        )

        action = _STATUS_ACTION_MAP.get(new_status, "UPDATED")
        self._log_change(rule_id, action, reason=reason, changed_by=changed_by)
        logger.info(f"ルール {rule_id}: {current_status} → {new_status} ({reason})")

    # --- バージョン管理 ---

    def _archive_rule(self, rule_id: int, snapshot_id: int | None = None) -> None:
        """1ルールの全状態をfactor_rules_archiveに保存する。"""
        if not self._db.table_exists("factor_rules_archive"):
            return
        rule = self._db.execute_query(
            "SELECT * FROM factor_rules WHERE rule_id = ?", (rule_id,)
        )
        if not rule:
            return
        r = rule[0]
        now = datetime.now(UTC).isoformat()
        self._db.execute_write(
            """INSERT INTO factor_rules_archive
            (snapshot_id, rule_id, rule_name, category, description,
             sql_expression, weight, review_status, is_active,
             validation_score, decay_rate, min_sample_size,
             source, training_from, training_to, archived_at, archived_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                snapshot_id, r["rule_id"], r["rule_name"],
                r.get("category", ""), r.get("description", ""),
                r.get("sql_expression", ""), r.get("weight", 1.0),
                r.get("review_status", "DRAFT"), r.get("is_active", 0),
                r.get("validation_score"), r.get("decay_rate"),
                r.get("min_sample_size", 100), r.get("source", "manual"),
                r.get("training_from"), r.get("training_to"),
                now, "system",
            ),
        )

    def create_snapshot(
        self,
        version_label: str,
        description: str = "",
        trigger: str = "manual",
        calibrator_path: str | None = None,
        calibrator_method: str | None = None,
        config_json: str | None = None,
    ) -> int:
        """全ルールのスナップショットを作成する。

        Args:
            version_label: バージョンラベル (例: "v1.2.0")
            description: 変更内容の説明
            trigger: トリガー種別 (manual/optimization/calibration/restore)

        Returns:
            作成されたsnapshot_id
        """
        if not self._db.table_exists("rule_set_snapshots"):
            return 0

        now = datetime.now(UTC).isoformat()
        with self._db.connect() as conn:
            conn.execute(
                """INSERT INTO rule_set_snapshots
                (version_label, description, trigger, calibrator_path,
                 calibrator_method, config_json, created_at, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (version_label, description, trigger, calibrator_path,
                 calibrator_method, config_json, now, "user"),
            )
            cursor = conn.execute("SELECT last_insert_rowid() as id")
            snapshot_id = cursor.fetchone()[0]

        all_rules = self._db.execute_query(
            "SELECT rule_id FROM factor_rules ORDER BY rule_id"
        )
        for r in all_rules:
            self._archive_rule(r["rule_id"], snapshot_id)

        logger.info(f"Snapshot created: {version_label} (ID: {snapshot_id}, {len(all_rules)} rules)")
        return snapshot_id

    def list_snapshots(self) -> list[dict[str, Any]]:
        """スナップショット一覧を返す（ルール数付き、新しい順）。"""
        if not self._db.table_exists("rule_set_snapshots"):
            return []
        return self._db.execute_query(
            """SELECT s.*, COUNT(a.archive_id) as rule_count
            FROM rule_set_snapshots s
            LEFT JOIN factor_rules_archive a ON s.snapshot_id = a.snapshot_id
            GROUP BY s.snapshot_id
            ORDER BY s.created_at DESC"""
        )

    def restore_snapshot(self, snapshot_id: int) -> int:
        """スナップショットからルールを復元する。

        復元前に現在の状態を自動スナップショット保存する。

        Args:
            snapshot_id: 復元元のスナップショットID

        Returns:
            復元したルール数
        """
        archived = self._db.execute_query(
            "SELECT * FROM factor_rules_archive WHERE snapshot_id = ?",
            (snapshot_id,),
        )
        if not archived:
            raise ValueError(f"Snapshot {snapshot_id} not found or empty")

        # 復元前に自動バックアップ
        self.create_snapshot(
            f"pre-restore (before #{snapshot_id})",
            description=f"Snapshot {snapshot_id} 復元前の自動バックアップ",
            trigger="restore",
        )

        restored = 0
        now = datetime.now(UTC).isoformat()
        for ar in archived:
            self._db.execute_write(
                """UPDATE factor_rules SET
                weight = ?, review_status = ?, is_active = ?,
                validation_score = ?, decay_rate = ?,
                min_sample_size = ?, training_from = ?,
                training_to = ?, updated_at = ?
                WHERE rule_id = ?""",
                (
                    ar["weight"], ar["review_status"], ar["is_active"],
                    ar["validation_score"], ar["decay_rate"],
                    ar["min_sample_size"], ar["training_from"],
                    ar["training_to"], now, ar["rule_id"],
                ),
            )
            self._log_change(
                ar["rule_id"], "RESTORED",
                reason=f"Snapshot {snapshot_id} から復元",
                changed_by="system",
            )
            restored += 1

        logger.info(f"Restored {restored} rules from snapshot {snapshot_id}")
        return restored

    def get_rule_history(self, rule_id: int) -> list[dict[str, Any]]:
        """1ルールの全アーカイブ履歴を返す。

        Args:
            rule_id: 対象ルールID

        Returns:
            アーカイブされたルール状態のリスト（新しい順）
        """
        if not self._db.table_exists("factor_rules_archive"):
            return []
        return self._db.execute_query(
            """SELECT a.*, s.version_label, s.trigger as snapshot_trigger
            FROM factor_rules_archive a
            LEFT JOIN rule_set_snapshots s ON a.snapshot_id = s.snapshot_id
            WHERE a.rule_id = ?
            ORDER BY a.archived_at DESC""",
            (rule_id,),
        )

    # --- 内部ヘルパー ---

    def _log_change(
        self,
        rule_id: int,
        action: str,
        old_weight: float | None = None,
        new_weight: float | None = None,
        reason: str = "",
        changed_by: str = "user",
        backtest_roi: float | None = None,
    ) -> None:
        """変更履歴をfactor_review_logに記録する。"""
        now = datetime.now(UTC).isoformat()
        self._db.execute_write(
            """
            INSERT INTO factor_review_log
            (rule_id, action, old_weight, new_weight, reason, backtest_roi, changed_at, changed_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (rule_id, action, old_weight, new_weight, reason, backtest_roi, now, changed_by),
        )
