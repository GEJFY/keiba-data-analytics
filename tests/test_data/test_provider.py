"""JVLinkDataProviderの単体テスト。"""

import pytest

from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider


@pytest.fixture
def jvlink_db(db_manager: DatabaseManager) -> DatabaseManager:
    """JVLink形式のテストテーブルを持つDBを返す。"""
    with db_manager.connect() as conn:
        # NL_RA（レース情報）テーブル
        conn.execute("""
            CREATE TABLE NL_RA (
                Year TEXT, MonthDay TEXT, JyoCD TEXT, Kaiji TEXT,
                Nichiji TEXT, RaceNum TEXT, RaceName TEXT, Kyori TEXT
            )
        """)
        conn.execute(
            "INSERT INTO NL_RA VALUES ('2025', '0101', '06', '01', '01', '01', '中山金杯', '2000')"
        )
        conn.execute(
            "INSERT INTO NL_RA VALUES ('2025', '0101', '06', '01', '01', '02', '2R', '1800')"
        )

        # NL_SE（出走馬情報）テーブル
        conn.execute("""
            CREATE TABLE NL_SE (
                Year TEXT, MonthDay TEXT, JyoCD TEXT, Kaiji TEXT,
                Nichiji TEXT, RaceNum TEXT, Umaban TEXT, KettoNum TEXT, Bamei TEXT
            )
        """)
        conn.execute(
            "INSERT INTO NL_SE VALUES ('2025', '0101', '06', '01', '01', '01', '01', '0001', '馬A')"
        )
        conn.execute(
            "INSERT INTO NL_SE VALUES ('2025', '0101', '06', '01', '01', '01', '03', '0002', '馬B')"
        )
        conn.execute(
            "INSERT INTO NL_SE VALUES ('2025', '0101', '06', '01', '01', '01', '02', '0003', '馬C')"
        )

        # NL_O1（単勝オッズ）テーブル
        conn.execute("""
            CREATE TABLE NL_O1 (
                Year TEXT, MonthDay TEXT, JyoCD TEXT, Kaiji TEXT,
                Nichiji TEXT, RaceNum TEXT, Umaban TEXT, Odds TEXT
            )
        """)
        conn.execute(
            "INSERT INTO NL_O1 VALUES ('2025', '0101', '06', '01', '01', '01', '01', '3.5')"
        )
        conn.execute(
            "INSERT INTO NL_O1 VALUES ('2025', '0101', '06', '01', '01', '01', '02', '8.2')"
        )

    return db_manager


class TestJVLinkDataProvider:
    """JVLinkDataProviderクラスのテスト。"""

    def test_build_race_key(self) -> None:
        """race_keyが正しく組み立てられること。"""
        row = {
            "Year": "2025", "MonthDay": "0101", "JyoCD": "06",
            "Kaiji": "01", "Nichiji": "01", "RaceNum": "01",
        }
        key = JVLinkDataProvider.build_race_key(row)
        assert key == "2025010106010101"

    def test_get_race_info_found(self, jvlink_db: DatabaseManager) -> None:
        """存在するレースの情報を取得できること。"""
        provider = JVLinkDataProvider(jvlink_db)
        result = provider.get_race_info("2025010106010101")
        assert result is not None
        assert result["RaceName"] == "中山金杯"
        assert result["Kyori"] == "2000"

    def test_get_race_info_not_found(self, jvlink_db: DatabaseManager) -> None:
        """存在しないレースの場合Noneを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db)
        result = provider.get_race_info("2099010106010101")
        assert result is None

    def test_get_race_info_invalid_key_length(self, jvlink_db: DatabaseManager) -> None:
        """race_keyの長さが不正な場合Noneを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db)
        assert provider.get_race_info("short") is None
        assert provider.get_race_info("") is None
        assert provider.get_race_info("12345678901234567890") is None

    def test_get_race_entries(self, jvlink_db: DatabaseManager) -> None:
        """出走馬リストを取得できること。"""
        provider = JVLinkDataProvider(jvlink_db)
        entries = provider.get_race_entries("2025010106010101")
        assert len(entries) == 3
        # Umaban順にソートされていること
        assert entries[0]["Umaban"] == "01"
        assert entries[1]["Umaban"] == "02"
        assert entries[2]["Umaban"] == "03"

    def test_get_race_entries_empty(self, jvlink_db: DatabaseManager) -> None:
        """出走馬が存在しないレースの場合空リストを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db)
        entries = provider.get_race_entries("2099010106010101")
        assert entries == []

    def test_get_race_entries_invalid_key(self, jvlink_db: DatabaseManager) -> None:
        """race_keyが不正な場合空リストを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db)
        assert provider.get_race_entries("short") == []

    def test_get_odds(self, jvlink_db: DatabaseManager) -> None:
        """オッズ情報を取得できること。"""
        provider = JVLinkDataProvider(jvlink_db)
        odds = provider.get_odds("2025010106010101")
        assert len(odds) == 2
        assert odds[0]["Odds"] == "3.5"

    def test_get_odds_custom_table(self, jvlink_db: DatabaseManager) -> None:
        """カスタムオッズテーブルを指定できること。"""
        provider = JVLinkDataProvider(jvlink_db)
        # NL_O1のみ存在するのでNL_O1を指定
        odds = provider.get_odds("2025010106010101", odds_table="NL_O1")
        assert len(odds) == 2

    def test_get_odds_invalid_key(self, jvlink_db: DatabaseManager) -> None:
        """race_keyが不正な場合空リストを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db)
        assert provider.get_odds("short") == []
