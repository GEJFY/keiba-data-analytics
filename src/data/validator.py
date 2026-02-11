"""データ品質チェックモジュール。

JVLinkToSQLiteが取り込んだデータの整合性・品質を検証する。
テーブル存在確認、レコード数チェック、欠損値検出を実施。
"""

import re
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager

# テーブル名・カラム名のバリデーション用パターン
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class ValidationResult:
    """データ検証結果。

    Attributes:
        table_name: 検証対象テーブル名
        total_records: 総レコード数
        missing_values: カラム名→欠損レコード数のマッピング
        anomalies: 検出された異常の説明リスト
        is_valid: 検証合格フラグ
    """

    table_name: str
    total_records: int = 0
    missing_values: dict[str, int] = field(default_factory=dict)
    anomalies: list[str] = field(default_factory=list)
    is_valid: bool = True


class DataValidator:
    """JVLink DBのデータ品質検証クラス。

    check_required_tables()でテーブル存在を確認し、
    validate_table()で個別テーブルの品質を検証する。
    """

    # JVLink必須テーブル一覧
    REQUIRED_TABLES = ["NL_RA", "NL_SE", "NL_HR", "NL_UM", "NL_KS"]

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    def check_required_tables(self) -> list[str]:
        """必須テーブルの存在を確認し、不足テーブル名を返す。

        Returns:
            不足しているテーブル名のリスト（全て存在する場合は空リスト）
        """
        missing = [t for t in self.REQUIRED_TABLES if not self._db.table_exists(t)]
        if missing:
            logger.warning(f"不足テーブル: {missing}")
        else:
            logger.info("全必須テーブル確認済み")
        return missing

    def validate_table(self, table_name: str, required_columns: list[str] | None = None) -> ValidationResult:
        """テーブルのレコード数と欠損値を検証する。

        Args:
            table_name: 検証対象テーブル名
            required_columns: 欠損値チェック対象のカラム名リスト

        Returns:
            ValidationResult（is_valid=Falseの場合、anomaliesに詳細あり）

        Raises:
            ValueError: テーブル名やカラム名が不正な識別子の場合
        """
        if not _IDENTIFIER_PATTERN.match(table_name):
            raise ValueError(f"不正なテーブル名: '{table_name}'")

        result = ValidationResult(table_name=table_name)

        rows = self._db.execute_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        result.total_records = rows[0]["cnt"] if rows else 0

        if result.total_records == 0:
            result.is_valid = False
            result.anomalies.append("テーブルにレコードがありません")
            logger.warning(f"{table_name}: レコード0件")
            return result

        logger.info(f"{table_name}: {result.total_records:,}件")

        if required_columns:
            for col in required_columns:
                if not _IDENTIFIER_PATTERN.match(col):
                    raise ValueError(f"不正なカラム名: '{col}'")
                null_rows = self._db.execute_query(
                    f"SELECT COUNT(*) as cnt FROM {table_name} WHERE {col} IS NULL OR {col} = ''",
                )
                null_count = null_rows[0]["cnt"] if null_rows else 0
                if null_count > 0:
                    result.missing_values[col] = null_count
                    logger.info(f"  {col}: 欠損{null_count:,}件 / {result.total_records:,}件")

        return result

    def run_full_check(self) -> dict[str, Any]:
        """全テーブルの品質チェックを実行する。

        Returns:
            {"missing_tables": [...], "table_validations": {テーブル名: {...}, ...}}
        """
        logger.info("=== データ品質チェック開始 ===")
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

        logger.info("=== データ品質チェック完了 ===")
        return results
