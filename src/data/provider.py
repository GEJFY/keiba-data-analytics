"""データプロバイダー — JVLink DBラッパー。

JVLinkToSQLiteが生成するNL_*テーブルへの
アクセスを抽象化する。race_keyの解析・組立もここで行う。
"""

import re
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager

# 許可されるJVLinkオッズテーブル名のホワイトリスト
_ALLOWED_ODDS_TABLES = frozenset({"NL_O1", "NL_O2", "NL_O3", "NL_O4", "NL_O5", "NL_O6"})

# race_keyの正規表現パターン（16桁の数字文字列）
_RACE_KEY_PATTERN = re.compile(r"^\d{16}$")


class JVLinkDataProvider:
    """JVLinkToSQLite DBからのデータ取得を提供するクラス。

    race_keyの構成:
        Year(4桁) + MonthDay(4桁) + JyoCD(2桁) + Kaiji(2桁) + Nichiji(2桁) + RaceNum(2桁)
        例: "2025010106010101" → 2025年1月1日 中山(06) 1回(01) 1日目(01) 1R(01)
    """

    RACE_KEY_COLUMNS = ["Year", "MonthDay", "JyoCD", "Kaiji", "Nichiji", "RaceNum"]

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    @staticmethod
    def build_race_key(row: dict[str, Any]) -> str:
        """行データからrace_keyを組み立てる。

        Args:
            row: RACE_KEY_COLUMNSの各キーを含むdict

        Returns:
            16桁のrace_key文字列

        Raises:
            KeyError: 必須キーが不足している場合
        """
        return "".join(str(row[col]) for col in JVLinkDataProvider.RACE_KEY_COLUMNS)

    @staticmethod
    def _parse_race_key(race_key: str) -> tuple[str, str, str, str, str, str] | None:
        """race_keyを構成要素に分解する。

        Returns:
            (year, month_day, jyo_cd, kaiji, nichiji, race_num) または不正キーの場合None
        """
        if not _RACE_KEY_PATTERN.match(race_key):
            logger.debug(f"不正なrace_keyフォーマット: '{race_key}'")
            return None
        return (
            race_key[0:4],   # Year
            race_key[4:8],   # MonthDay
            race_key[8:10],  # JyoCD
            race_key[10:12], # Kaiji
            race_key[12:14], # Nichiji
            race_key[14:16], # RaceNum
        )

    def get_race_info(self, race_key: str) -> dict[str, Any] | None:
        """レース情報（NL_RA）を取得する。

        Args:
            race_key: 16桁のrace_key

        Returns:
            レース情報のdict。該当なしまたは不正キーの場合None。
        """
        parts = self._parse_race_key(race_key)
        if parts is None:
            return None

        results = self._db.execute_query(
            """
            SELECT * FROM NL_RA
            WHERE Year = ? AND MonthDay = ? AND JyoCD = ?
              AND Kaiji = ? AND Nichiji = ? AND RaceNum = ?
            """,
            parts,
        )
        return results[0] if results else None

    def get_race_entries(self, race_key: str) -> list[dict[str, Any]]:
        """出走馬情報（NL_SE）を取得する。

        Args:
            race_key: 16桁のrace_key

        Returns:
            出走馬のdictリスト（Umaban昇順）。該当なしの場合空リスト。
        """
        parts = self._parse_race_key(race_key)
        if parts is None:
            return []

        return self._db.execute_query(
            """
            SELECT * FROM NL_SE
            WHERE Year = ? AND MonthDay = ? AND JyoCD = ?
              AND Kaiji = ? AND Nichiji = ? AND RaceNum = ?
            ORDER BY Umaban
            """,
            parts,
        )

    def get_odds(self, race_key: str, odds_table: str = "NL_O1") -> list[dict[str, Any]]:
        """オッズ情報を取得する。

        Args:
            race_key: 16桁のrace_key
            odds_table: オッズテーブル名（NL_O1〜NL_O6のみ許可）

        Returns:
            オッズ情報のdictリスト。該当なしの場合空リスト。

        Raises:
            ValueError: 許可されていないテーブル名が指定された場合
        """
        if odds_table not in _ALLOWED_ODDS_TABLES:
            raise ValueError(
                f"許可されていないオッズテーブル名: '{odds_table}' "
                f"(許可: {sorted(_ALLOWED_ODDS_TABLES)})"
            )

        parts = self._parse_race_key(race_key)
        if parts is None:
            return []

        return self._db.execute_query(
            f"""
            SELECT * FROM {odds_table}
            WHERE Year = ? AND MonthDay = ? AND JyoCD = ?
              AND Kaiji = ? AND Nichiji = ? AND RaceNum = ?
            """,
            parts,
        )
