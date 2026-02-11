"""データ品質チェックモジュール。

JVLinkToSQLiteが取り込んだデータの整合性・品質を検証する。
"""

from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager


@dataclass
class ValidationResult:
    """データ検証結果。"""

    table_name: str
    total_records: int = 0
    missing_values: dict[str, int] = field(default_factory=dict)
    anomalies: list[str] = field(default_factory=list)
    is_valid: bool = True


class DataValidator:
    """JVLink DBのデータ品質検証クラス。"""

    # 必須テーブル一覧
    REQUIRED_TABLES = ["NL_RA", "NL_SE", "NL_HR", "NL_UM", "NL_KS"]

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    def check_required_tables(self) -> list[str]:
        """必須テーブルの存在を確認し、不足テーブル名を返す。"""
        missing = [t for t in self.REQUIRED_TABLES if not self._db.table_exists(t)]
        if missing:
            logger.warning(f"不足テーブル: {missing}")
        return missing

    def validate_table(self, table_name: str, required_columns: list[str] | None = None) -> ValidationResult:
        """テーブルのレコード数と欠損値を検証する。"""
        result = ValidationResult(table_name=table_name)

        rows = self._db.execute_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        result.total_records = rows[0]["cnt"] if rows else 0

        if result.total_records == 0:
            result.is_valid = False
            result.anomalies.append("テーブルにレコードがありません")
            return result

        if required_columns:
            for col in required_columns:
                null_rows = self._db.execute_query(
                    f"SELECT COUNT(*) as cnt FROM {table_name} WHERE {col} IS NULL OR {col} = ''",
                )
                null_count = null_rows[0]["cnt"] if null_rows else 0
                if null_count > 0:
                    result.missing_values[col] = null_count

        return result

    def run_full_check(self) -> dict[str, Any]:
        """全テーブルの品質チェックを実行する。"""
        missing_tables = self.check_required_tables()
        results: dict[str, Any] = {"missing_tables": missing_tables, "table_validations": {}}

        for table in self.REQUIRED_TABLES:
            if table not in missing_tables:
                validation = self.validate_table(table)
                results["table_validations"][table] = {
                    "total_records": validation.total_records,
                    "is_valid": validation.is_valid,
                    "anomalies": validation.anomalies,
                }

        return results
