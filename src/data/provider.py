"""データプロバイダー — JVLink DBラッパー。

JVLinkToSQLiteが生成するNL_*テーブルへの
アクセスを抽象化する。
"""

from typing import Any

from src.data.db import DatabaseManager


class JVLinkDataProvider:
    """JVLinkToSQLite DBからのデータ取得を提供するクラス。"""

    # race_keyの構成要素
    RACE_KEY_COLUMNS = ["Year", "MonthDay", "JyoCD", "Kaiji", "Nichiji", "RaceNum"]

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    @staticmethod
    def build_race_key(row: dict[str, Any]) -> str:
        """行データからrace_keyを組み立てる。"""
        return "".join(str(row[col]) for col in JVLinkDataProvider.RACE_KEY_COLUMNS)

    def get_race_info(self, race_key: str) -> dict[str, Any] | None:
        """レース情報（NL_RA）を取得する。"""
        # race_keyの分解: Year(4) + MonthDay(4) + JyoCD(2) + Kaiji(2) + Nichiji(2) + RaceNum(2)
        if len(race_key) != 16:
            return None
        year = race_key[0:4]
        month_day = race_key[4:8]
        jyo_cd = race_key[8:10]
        kaiji = race_key[10:12]
        nichiji = race_key[12:14]
        race_num = race_key[14:16]

        results = self._db.execute_query(
            """
            SELECT * FROM NL_RA
            WHERE Year = ? AND MonthDay = ? AND JyoCD = ?
              AND Kaiji = ? AND Nichiji = ? AND RaceNum = ?
            """,
            (year, month_day, jyo_cd, kaiji, nichiji, race_num),
        )
        return results[0] if results else None

    def get_race_entries(self, race_key: str) -> list[dict[str, Any]]:
        """出走馬情報（NL_SE）を取得する。"""
        if len(race_key) != 16:
            return []
        year = race_key[0:4]
        month_day = race_key[4:8]
        jyo_cd = race_key[8:10]
        kaiji = race_key[10:12]
        nichiji = race_key[12:14]
        race_num = race_key[14:16]

        return self._db.execute_query(
            """
            SELECT * FROM NL_SE
            WHERE Year = ? AND MonthDay = ? AND JyoCD = ?
              AND Kaiji = ? AND Nichiji = ? AND RaceNum = ?
            ORDER BY Umaban
            """,
            (year, month_day, jyo_cd, kaiji, nichiji, race_num),
        )

    def get_odds(self, race_key: str, odds_table: str = "NL_O1") -> list[dict[str, Any]]:
        """オッズ情報を取得する。"""
        if len(race_key) != 16:
            return []
        year = race_key[0:4]
        month_day = race_key[4:8]
        jyo_cd = race_key[8:10]
        kaiji = race_key[10:12]
        nichiji = race_key[12:14]
        race_num = race_key[14:16]

        return self._db.execute_query(
            f"""
            SELECT * FROM {odds_table}
            WHERE Year = ? AND MonthDay = ? AND JyoCD = ?
              AND Kaiji = ? AND Nichiji = ? AND RaceNum = ?
            """,
            (year, month_day, jyo_cd, kaiji, nichiji, race_num),
        )
