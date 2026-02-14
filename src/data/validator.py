"""データ品質チェックモジュール。

JVLinkToSQLiteが取り込んだデータの整合性・品質を検証する。
テーブル存在確認、レコード数チェック、欠損値検出、
データカバレッジ確認、テーブル間整合性チェックを実施。
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


@dataclass
class CheckItem:
    """個別チェック項目の結果。

    Attributes:
        category: チェックカテゴリ (table / column / coverage / consistency)
        name: チェック名
        status: OK / WARNING / ERROR
        detail: 詳細説明
    """

    category: str
    name: str
    status: str  # "OK" / "WARNING" / "ERROR"
    detail: str


class DataValidator:
    """JVLink DBのデータ品質検証クラス。

    check_required_tables()でテーブル存在を確認し、
    validate_table()で個別テーブルの品質を検証する。
    """

    # JVLink必須テーブル一覧
    REQUIRED_TABLES = ["NL_RA_RACE", "NL_SE_RACE_UMA", "NL_HR_PAY", "NL_UM_UMA", "NL_KS_KISYU"]
    # EV計算に必要なオッズテーブル
    ODDS_TABLES = [
        "NL_O1_ODDS_TANFUKUWAKU",
        "NL_O2_ODDS_UMAREN",
        "NL_O3_ODDS_WIDE",
        "NL_O4_ODDS_UMATAN",
        "NL_O5_ODDS_SANREN",
        "NL_O6_ODDS_SANRENTAN",
    ]

    # テーブルごとの重要カラム（欠損チェック対象）
    IMPORTANT_COLUMNS: dict[str, list[str]] = {
        "NL_RA_RACE": ["idYear", "idMonthDay", "idJyoCD", "Kyori", "TrackCD", "SyussoTosu"],
        "NL_SE_RACE_UMA": ["idYear", "idMonthDay", "idJyoCD", "idRaceNum", "Umaban"],
        "NL_HR_PAY": ["idYear", "idMonthDay", "idJyoCD", "idRaceNum"],
        "NL_UM_UMA": ["KettoNum", "Bamei"],
        "NL_KS_KISYU": ["KisyuCode", "KisyuName"],
    }

    # テーブル説明
    TABLE_DESCRIPTIONS: dict[str, str] = {
        "NL_RA_RACE": "レース情報（開催日・競馬場・距離等）",
        "NL_SE_RACE_UMA": "出走馬情報（馬番・成績等）",
        "NL_HR_PAY": "払戻情報（配当金額）",
        "NL_UM_UMA": "馬マスタ（血統・馬名等）",
        "NL_KS_KISYU": "騎手マスタ（騎手名等）",
        "NL_O1_ODDS_TANFUKUWAKU": "単勝・複勝・枠連オッズ",
        "NL_O2_ODDS_UMAREN": "馬連オッズ",
        "NL_O3_ODDS_WIDE": "ワイドオッズ",
        "NL_O4_ODDS_UMATAN": "馬単オッズ",
        "NL_O5_ODDS_SANREN": "三連複オッズ",
        "NL_O6_ODDS_SANRENTAN": "三連単オッズ",
    }

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

    def _get_table_columns(self, table_name: str) -> list[str]:
        """テーブルの実カラム名一覧を取得する。"""
        rows = self._db.execute_query(f"PRAGMA table_info({table_name})")
        return [r["name"] for r in rows] if rows else []

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
            actual_columns = self._get_table_columns(table_name)
            for col in required_columns:
                if not _IDENTIFIER_PATTERN.match(col):
                    raise ValueError(f"不正なカラム名: '{col}'")
                if col not in actual_columns:
                    continue  # テーブルに存在しないカラムはスキップ
                null_rows = self._db.execute_query(
                    f"SELECT COUNT(*) as cnt FROM {table_name} WHERE {col} IS NULL OR {col} = ''",
                )
                null_count = null_rows[0]["cnt"] if null_rows else 0
                if null_count > 0:
                    result.missing_values[col] = null_count
                    logger.info(f"  {col}: 欠損{null_count:,}件 / {result.total_records:,}件")

        return result

    def check_odds_tables(self) -> list[str]:
        """オッズテーブルの存在を確認し、不足テーブル名を返す。

        Returns:
            不足しているオッズテーブル名のリスト
        """
        missing = [t for t in self.ODDS_TABLES if not self._db.table_exists(t)]
        if missing:
            logger.warning(
                f"オッズテーブル不足: {missing} — "
                "setting.xmlでO1-O6が除外されていないか確認し、"
                "sync_jvlink.batを再実行してください"
            )
        else:
            logger.info("全オッズテーブル確認済み")
        return missing

    def _has_columns(self, table_name: str, columns: list[str]) -> bool:
        """テーブルに指定カラムが全て存在するか確認する。"""
        actual = set(self._get_table_columns(table_name))
        return all(c in actual for c in columns)

    def check_data_coverage(self) -> dict[str, Any]:
        """データカバレッジ情報を取得する。

        Returns:
            日付範囲、競馬場、レース数、出走馬数等の統計情報
        """
        coverage: dict[str, Any] = {}

        if not self._db.table_exists("NL_RA_RACE"):
            return coverage

        # idYear, idMonthDay カラムが存在するか確認
        if not self._has_columns("NL_RA_RACE", ["idYear", "idMonthDay"]):
            return coverage

        # 日付範囲・開催日数・レース数
        rows = self._db.execute_query(
            "SELECT MIN(idYear || idMonthDay) AS min_date, "
            "MAX(idYear || idMonthDay) AS max_date, "
            "COUNT(DISTINCT idYear || idMonthDay) AS day_count, "
            "COUNT(*) AS race_count "
            "FROM NL_RA_RACE"
        )
        if rows and rows[0]["race_count"]:
            r = rows[0]
            coverage["min_date"] = str(r["min_date"]) if r["min_date"] else ""
            coverage["max_date"] = str(r["max_date"]) if r["max_date"] else ""
            coverage["day_count"] = r["day_count"]
            coverage["race_count"] = r["race_count"]

        # 競馬場分布
        if self._has_columns("NL_RA_RACE", ["idJyoCD"]):
            rows = self._db.execute_query(
                "SELECT idJyoCD, COUNT(*) AS cnt "
                "FROM NL_RA_RACE GROUP BY idJyoCD ORDER BY cnt DESC"
            )
            if rows:
                coverage["venues"] = {r["idJyoCD"]: r["cnt"] for r in rows}

        # 出走馬レコード数
        if self._db.table_exists("NL_SE_RACE_UMA"):
            rows = self._db.execute_query(
                "SELECT COUNT(*) AS cnt FROM NL_SE_RACE_UMA"
            )
            if rows:
                coverage["horse_entries"] = rows[0]["cnt"]

        return coverage

    def check_cross_consistency(self) -> list[CheckItem]:
        """テーブル間の整合性をチェックする。

        Returns:
            チェック結果のリスト
        """
        checks: list[CheckItem] = []
        join_cols = ["idYear", "idMonthDay", "idJyoCD", "idRaceNum"]

        # レース情報と出走馬の対応チェック
        if (self._db.table_exists("NL_RA_RACE")
                and self._db.table_exists("NL_SE_RACE_UMA")
                and self._has_columns("NL_RA_RACE", join_cols)
                and self._has_columns("NL_SE_RACE_UMA", join_cols)):

            # レースに出走馬がないケース
            rows = self._db.execute_query(
                "SELECT COUNT(*) AS cnt FROM NL_RA_RACE r "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM NL_SE_RACE_UMA s "
                "  WHERE s.idYear = r.idYear AND s.idMonthDay = r.idMonthDay "
                "  AND s.idJyoCD = r.idJyoCD AND s.idRaceNum = r.idRaceNum"
                ")"
            )
            orphan_races = rows[0]["cnt"] if rows else 0
            if orphan_races > 0:
                checks.append(CheckItem(
                    category="consistency",
                    name="レース-出走馬対応",
                    status="WARNING",
                    detail=f"出走馬データのないレースが {orphan_races} 件あります",
                ))
            else:
                checks.append(CheckItem(
                    category="consistency",
                    name="レース-出走馬対応",
                    status="OK",
                    detail="全レースに出走馬データが紐付いています",
                ))

        # レースと払戻の対応チェック
        if (self._db.table_exists("NL_RA_RACE")
                and self._db.table_exists("NL_HR_PAY")
                and self._has_columns("NL_RA_RACE", join_cols)
                and self._has_columns("NL_HR_PAY", join_cols)):

            rows = self._db.execute_query(
                "SELECT COUNT(*) AS cnt FROM NL_RA_RACE r "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM NL_HR_PAY p "
                "  WHERE p.idYear = r.idYear AND p.idMonthDay = r.idMonthDay "
                "  AND p.idJyoCD = r.idJyoCD AND p.idRaceNum = r.idRaceNum"
                ")"
            )
            orphan_pay = rows[0]["cnt"] if rows else 0
            if orphan_pay > 0:
                total_races = self._db.execute_query(
                    "SELECT COUNT(*) AS cnt FROM NL_RA_RACE"
                )
                total = total_races[0]["cnt"] if total_races else 0
                ratio = orphan_pay / total if total > 0 else 0
                status = "WARNING" if ratio < 0.5 else "ERROR"
                checks.append(CheckItem(
                    category="consistency",
                    name="レース-払戻対応",
                    status=status,
                    detail=f"払戻データのないレースが {orphan_pay}/{total} 件あります（未確定レースを含む場合あり）",
                ))
            else:
                checks.append(CheckItem(
                    category="consistency",
                    name="レース-払戻対応",
                    status="OK",
                    detail="全レースに払戻データが紐付いています",
                ))

        # オッズテーブルのレコード数チェック
        for odds_table in self.ODDS_TABLES:
            if self._db.table_exists(odds_table):
                rows = self._db.execute_query(
                    f"SELECT COUNT(*) AS cnt FROM [{odds_table}]"
                )
                cnt = rows[0]["cnt"] if rows else 0
                desc = self.TABLE_DESCRIPTIONS.get(odds_table, odds_table)
                if cnt == 0:
                    checks.append(CheckItem(
                        category="odds",
                        name=f"{odds_table}",
                        status="WARNING",
                        detail=f"{desc}: テーブルは存在するがレコード0件",
                    ))
                else:
                    checks.append(CheckItem(
                        category="odds",
                        name=f"{odds_table}",
                        status="OK",
                        detail=f"{desc}: {cnt:,} 件",
                    ))

        return checks

    def run_full_check(self) -> dict[str, Any]:
        """全テーブルの品質チェックを実行する。

        Returns:
            {"missing_tables": [...], "missing_odds_tables": [...],
             "table_validations": {テーブル名: {...}, ...},
             "check_items": [CheckItem, ...],
             "data_coverage": {...}}
        """
        logger.info("=== データ品質チェック開始 ===")
        missing_tables = self.check_required_tables()
        missing_odds = self.check_odds_tables()
        check_items: list[CheckItem] = []

        results: dict[str, Any] = {
            "missing_tables": missing_tables,
            "missing_odds_tables": missing_odds,
            "table_validations": {},
            "check_items": check_items,
            "data_coverage": {},
        }

        # --- 1. テーブル存在チェック ---
        for table in self.REQUIRED_TABLES:
            desc = self.TABLE_DESCRIPTIONS.get(table, table)
            if table in missing_tables:
                check_items.append(CheckItem(
                    category="table",
                    name=f"{table} 存在",
                    status="ERROR",
                    detail=f"{desc} — テーブルが見つかりません",
                ))
            else:
                check_items.append(CheckItem(
                    category="table",
                    name=f"{table} 存在",
                    status="OK",
                    detail=f"{desc} — テーブル検出済み",
                ))

        for table in self.ODDS_TABLES:
            desc = self.TABLE_DESCRIPTIONS.get(table, table)
            if table in missing_odds:
                check_items.append(CheckItem(
                    category="table",
                    name=f"{table} 存在",
                    status="ERROR",
                    detail=f"{desc} — テーブルが見つかりません",
                ))
            else:
                check_items.append(CheckItem(
                    category="table",
                    name=f"{table} 存在",
                    status="OK",
                    detail=f"{desc} — テーブル検出済み",
                ))

        # --- 2. レコード数・欠損値チェック ---
        for table in self.REQUIRED_TABLES:
            if table not in missing_tables:
                important_cols = self.IMPORTANT_COLUMNS.get(table, [])
                validation = self.validate_table(table, required_columns=important_cols)
                results["table_validations"][table] = {
                    "total_records": validation.total_records,
                    "is_valid": validation.is_valid,
                    "anomalies": validation.anomalies,
                    "missing_values": {
                        col: {
                            "count": cnt,
                            "ratio": cnt / validation.total_records if validation.total_records > 0 else 0,
                        }
                        for col, cnt in validation.missing_values.items()
                    },
                }

                desc = self.TABLE_DESCRIPTIONS.get(table, table)
                # レコード数チェック
                if validation.total_records == 0:
                    check_items.append(CheckItem(
                        category="record",
                        name=f"{table} レコード数",
                        status="ERROR",
                        detail=f"{desc} — レコードが0件です",
                    ))
                else:
                    check_items.append(CheckItem(
                        category="record",
                        name=f"{table} レコード数",
                        status="OK",
                        detail=f"{desc} — {validation.total_records:,} 件",
                    ))

                # 欠損値チェック
                if validation.missing_values:
                    for col, cnt in validation.missing_values.items():
                        ratio = cnt / validation.total_records
                        status = "ERROR" if ratio > 0.1 else "WARNING"
                        check_items.append(CheckItem(
                            category="column",
                            name=f"{table}.{col} 欠損",
                            status=status,
                            detail=f"{cnt:,}/{validation.total_records:,} 件が空 ({ratio:.1%})",
                        ))
                else:
                    if important_cols:
                        check_items.append(CheckItem(
                            category="column",
                            name=f"{table} 重要カラム",
                            status="OK",
                            detail=f"チェック対象: {', '.join(important_cols)} — 欠損なし",
                        ))

        # --- 3. データカバレッジ ---
        coverage = self.check_data_coverage()
        results["data_coverage"] = coverage
        if coverage and coverage.get("race_count"):
            check_items.append(CheckItem(
                category="coverage",
                name="データ範囲",
                status="OK",
                detail=(
                    f"{coverage['min_date']} 〜 {coverage['max_date']} / "
                    f"{coverage['day_count']}開催日 / {coverage['race_count']}レース"
                ),
            ))

        # --- 4. テーブル間整合性 ---
        cross_checks = self.check_cross_consistency()
        check_items.extend(cross_checks)

        logger.info("=== データ品質チェック完了 ===")
        return results
