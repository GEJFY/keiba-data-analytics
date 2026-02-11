"""ファクタールール登録・管理・CRUDモジュール。

factor_rulesテーブルとfactor_review_logテーブルを通じて
ファクタールールのライフサイクルを管理する。

ルールの状態遷移:
    DRAFT → TESTING → APPROVED → DEPRECATED
"""

from datetime import datetime, timezone
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager

# 許可されるステータス遷移マップ
_VALID_TRANSITIONS: dict[str, list[str]] = {
    "DRAFT": ["TESTING"],
    "TESTING": ["APPROVED", "DRAFT"],
    "APPROVED": ["DEPRECATED"],
    "DEPRECATED": [],
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

    def get_active_rules(self) -> list[dict[str, Any]]:
        """有効な（APPROVED かつ is_active = 1 かつ有効期間内の）ルールを取得する。

        Returns:
            有効なルールのdictリスト（category, rule_id順）
        """
        now = datetime.now(timezone.utc).isoformat()
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

        now = datetime.now(timezone.utc).isoformat()
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
        old = self._db.execute_query("SELECT weight FROM factor_rules WHERE rule_id = ?", (rule_id,))
        old_weight = old[0]["weight"] if old else 0.0

        now = datetime.now(timezone.utc).isoformat()
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

        now = datetime.now(timezone.utc).isoformat()
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
        now = datetime.now(timezone.utc).isoformat()
        self._db.execute_write(
            """
            INSERT INTO factor_review_log
            (rule_id, action, old_weight, new_weight, reason, backtest_roi, changed_at, changed_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (rule_id, action, old_weight, new_weight, reason, backtest_roi, now, changed_by),
        )
