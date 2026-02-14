"""DataValidatorの単体テスト。

JVLink実スキーマ（NL_RA_RACE, NL_SE_RACE_UMA等）に準拠。
"""

import pytest

from src.data.db import DatabaseManager
from src.data.validator import DataValidator, ValidationResult


@pytest.fixture
def validator_db(db_manager: DatabaseManager) -> DatabaseManager:
    """バリデーション用テストDBを返す。"""
    with db_manager.connect() as conn:
        # 全必須テーブルを作成
        for table in DataValidator.REQUIRED_TABLES:
            conn.execute(f"CREATE TABLE [{table}] (id INTEGER, Name TEXT, Value TEXT)")
        # NL_RA_RACEにデータ投入
        conn.execute("INSERT INTO NL_RA_RACE VALUES (1, '中山金杯', '2000')")
        conn.execute("INSERT INTO NL_RA_RACE VALUES (2, '京都金杯', '')")
        conn.execute("INSERT INTO NL_RA_RACE VALUES (3, NULL, '1600')")
        # NL_SE_RACE_UMAにデータ投入
        conn.execute("INSERT INTO NL_SE_RACE_UMA VALUES (1, '馬A', '100')")
    return db_manager


class TestDataValidator:
    """DataValidatorクラスのテスト。"""

    def test_check_required_tables_all_present(self, validator_db: DatabaseManager) -> None:
        """全必須テーブルが存在する場合、空リストを返すこと。"""
        validator = DataValidator(validator_db)
        missing = validator.check_required_tables()
        assert missing == []

    def test_check_required_tables_some_missing(self, db_manager: DatabaseManager) -> None:
        """一部テーブルが不足している場合、不足テーブル名を返すこと。"""
        with db_manager.connect() as conn:
            conn.execute("CREATE TABLE NL_RA_RACE (id INTEGER)")
            conn.execute("CREATE TABLE NL_SE_RACE_UMA (id INTEGER)")

        validator = DataValidator(db_manager)
        missing = validator.check_required_tables()
        assert "NL_HR_PAY" in missing
        assert "NL_UM_UMA" in missing
        assert "NL_KS_KISYU" in missing
        assert "NL_RA_RACE" not in missing

    def test_check_required_tables_all_missing(self, db_manager: DatabaseManager) -> None:
        """全テーブルが不足している場合、全テーブル名を返すこと。"""
        validator = DataValidator(db_manager)
        missing = validator.check_required_tables()
        assert set(missing) == set(DataValidator.REQUIRED_TABLES)

    def test_validate_table_with_data(self, validator_db: DatabaseManager) -> None:
        """データのあるテーブルの検証結果が正しいこと。"""
        validator = DataValidator(validator_db)
        result = validator.validate_table("NL_RA_RACE")
        assert result.table_name == "NL_RA_RACE"
        assert result.total_records == 3
        assert result.is_valid is True

    def test_validate_table_empty(self, validator_db: DatabaseManager) -> None:
        """空テーブルの検証結果にアノマリーが含まれること。"""
        validator = DataValidator(validator_db)
        result = validator.validate_table("NL_HR_PAY")
        assert result.total_records == 0
        assert result.is_valid is False
        assert len(result.anomalies) > 0
        assert "レコードがありません" in result.anomalies[0]

    def test_validate_table_missing_values(self, validator_db: DatabaseManager) -> None:
        """欠損値のあるカラムが検出されること。"""
        validator = DataValidator(validator_db)
        result = validator.validate_table("NL_RA_RACE", required_columns=["Name", "Value"])
        assert result.total_records == 3
        # Nameは1件NULL、Valueは1件空文字
        assert "Name" in result.missing_values
        assert result.missing_values["Name"] == 1
        assert "Value" in result.missing_values
        assert result.missing_values["Value"] == 1

    def test_validate_table_no_missing_required(self, validator_db: DatabaseManager) -> None:
        """欠損値なしのカラムはmissing_valuesに含まれないこと。"""
        validator = DataValidator(validator_db)
        result = validator.validate_table("NL_SE_RACE_UMA", required_columns=["Name"])
        assert "Name" not in result.missing_values

    def test_run_full_check(self, validator_db: DatabaseManager) -> None:
        """全テーブルの品質チェックが正しく実行されること。"""
        validator = DataValidator(validator_db)
        report = validator.run_full_check()
        assert report["missing_tables"] == []
        assert "NL_RA_RACE" in report["table_validations"]
        assert report["table_validations"]["NL_RA_RACE"]["total_records"] == 3
        assert report["table_validations"]["NL_RA_RACE"]["is_valid"] is True
        # 空テーブル
        assert report["table_validations"]["NL_HR_PAY"]["is_valid"] is False

    def test_run_full_check_with_missing_tables(self, db_manager: DatabaseManager) -> None:
        """テーブルが不足している場合、missing_tablesに含まれること。"""
        with db_manager.connect() as conn:
            conn.execute("CREATE TABLE NL_RA_RACE (id INTEGER)")
            conn.execute("INSERT INTO NL_RA_RACE VALUES (1)")

        validator = DataValidator(db_manager)
        report = validator.run_full_check()
        assert "NL_SE_RACE_UMA" in report["missing_tables"]
        # 不足テーブルはvalidationsに含まれない
        assert "NL_SE_RACE_UMA" not in report["table_validations"]
        # 存在するテーブルはvalidationsに含まれる
        assert "NL_RA_RACE" in report["table_validations"]

    def test_validation_result_defaults(self) -> None:
        """ValidationResultのデフォルト値が正しいこと。"""
        result = ValidationResult(table_name="test")
        assert result.total_records == 0
        assert result.missing_values == {}
        assert result.anomalies == []
        assert result.is_valid is True
