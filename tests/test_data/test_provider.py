"""JVLinkDataProviderの単体テスト。

実スキーマ（NL_RA_RACE, NL_SE_RACE_UMA, NL_O1_ODDS_TANFUKUWAKU, NL_HR_PAY）に準拠。
"""

import pytest

from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider


@pytest.fixture
def jvlink_db(db_manager: DatabaseManager) -> DatabaseManager:
    """JVLink実スキーマ形式のテストテーブルを持つDBを返す。"""
    with db_manager.connect() as conn:
        # NL_RA_RACE（レース情報）
        conn.execute("""
            CREATE TABLE NL_RA_RACE (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT, idKaiji TEXT,
                idNichiji TEXT, idRaceNum TEXT,
                RaceInfoHondai TEXT, Kyori TEXT, TrackCD TEXT,
                TenkoBabaTenkoCD TEXT, TenkoBabaSibaBabaCD TEXT, TenkoBabaDirtBabaCD TEXT,
                JyokenInfoSyubetuCD TEXT, GradeCD TEXT, HassoTime TEXT,
                TorokuTosu TEXT, SyussoTosu TEXT, NyusenTosu TEXT,
                HaronTimeL3 TEXT, HaronTimeL4 TEXT
            )
        """)
        conn.execute(
            "INSERT INTO NL_RA_RACE VALUES "
            "('2025','0101','06','01','01','01','中山金杯','2000','10','1','1','0','11','','1530','16','16','16','348','472')"
        )
        conn.execute(
            "INSERT INTO NL_RA_RACE VALUES "
            "('2025','0101','06','01','01','02','2R','1800','22','1','0','1','','','1400','12','12','12','370','500')"
        )

        # NL_SE_RACE_UMA（出走馬情報）
        conn.execute("""
            CREATE TABLE NL_SE_RACE_UMA (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT, idKaiji TEXT,
                idNichiji TEXT, idRaceNum TEXT,
                Wakuban TEXT, Umaban TEXT, KettoNum TEXT, Bamei TEXT,
                SexCD TEXT, Barei TEXT, Futan TEXT,
                KisyuRyakusyo TEXT, ChokyosiRyakusyo TEXT,
                BaTaijyu TEXT, ZogenFugo TEXT, ZogenSa TEXT,
                KakuteiJyuni TEXT, Ninki TEXT, Odds TEXT, Time TEXT,
                HaronTimeL3 TEXT, HaronTimeL4 TEXT,
                DMJyuni TEXT, KyakusituKubun TEXT,
                Jyuni1c TEXT, Jyuni2c TEXT, Jyuni3c TEXT, Jyuni4c TEXT
            )
        """)
        conn.execute(
            "INSERT INTO NL_SE_RACE_UMA VALUES "
            "('2025','0101','06','01','01','01',"
            "'1','01','0001','馬A','1','4','540','騎手A','調教師A',"
            "'480','+','4','3','2','35','11820','345','470','2','1','5','4','3','2')"
        )
        conn.execute(
            "INSERT INTO NL_SE_RACE_UMA VALUES "
            "('2025','0101','06','01','01','01',"
            "'2','03','0002','馬B','2','3','530','騎手B','調教師B',"
            "'460','-','2','1','5','120','12010','352','480','1','3','3','2','1','1')"
        )
        conn.execute(
            "INSERT INTO NL_SE_RACE_UMA VALUES "
            "('2025','0101','06','01','01','01',"
            "'1','02','0003','馬C','1','5','560','騎手C','調教師C',"
            "'500','+','8','5','1','15','11950','340','465','3','2','8','6','5','4')"
        )

        # NL_O1_ODDS_TANFUKUWAKU（単複枠オッズ、1行=1レース、横持ち）
        conn.execute("""
            CREATE TABLE NL_O1_ODDS_TANFUKUWAKU (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT, idKaiji TEXT,
                idNichiji TEXT, idRaceNum TEXT,
                OddsTansyoInfo0Umaban TEXT, OddsTansyoInfo0Odds TEXT,
                OddsTansyoInfo1Umaban TEXT, OddsTansyoInfo1Odds TEXT,
                OddsTansyoInfo2Umaban TEXT, OddsTansyoInfo2Odds TEXT
            )
        """)
        conn.execute(
            "INSERT INTO NL_O1_ODDS_TANFUKUWAKU VALUES "
            "('2025','0101','06','01','01','01',"
            "'01','0035','02','0082','03','0150')"
        )

        # NL_HR_PAY（払戻情報）
        conn.execute("""
            CREATE TABLE NL_HR_PAY (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT, idKaiji TEXT,
                idNichiji TEXT, idRaceNum TEXT,
                PayTansyo0Umaban TEXT, PayTansyo0Pay TEXT, PayTansyo0Ninki TEXT,
                PayFukusyo0Umaban TEXT, PayFukusyo0Pay TEXT, PayFukusyo0Ninki TEXT,
                PayFukusyo1Umaban TEXT, PayFukusyo1Pay TEXT, PayFukusyo1Ninki TEXT,
                PayUmaren0Kumi TEXT, PayUmaren0Pay TEXT, PayUmaren0Ninki TEXT
            )
        """)
        conn.execute(
            "INSERT INTO NL_HR_PAY VALUES "
            "('2025','0101','06','01','01','01',"
            "'03','1250','5','03','350','3','02','200','1','0203','2400','8')"
        )

    return db_manager


class TestJVLinkDataProvider:
    """JVLinkDataProviderクラスのテスト。"""

    def test_build_race_key(self) -> None:
        """race_keyが正しく組み立てられること（正規化キー）。"""
        row = {
            "Year": "2025", "MonthDay": "0101", "JyoCD": "06",
            "Kaiji": "01", "Nichiji": "01", "RaceNum": "01",
        }
        key = JVLinkDataProvider.build_race_key(row)
        assert key == "2025010106010101"

    def test_build_race_key_with_id_prefix(self) -> None:
        """idプレフィックス付きキーからもrace_keyが組み立てられること。"""
        row = {
            "idYear": "2025", "idMonthDay": "0101", "idJyoCD": "06",
            "idKaiji": "01", "idNichiji": "01", "idRaceNum": "01",
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
        assert result["TrackCD"] == "10"
        assert result["Year"] == "2025"

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
        # 正規化キーで返されること
        assert entries[0]["Year"] == "2025"
        assert entries[0]["Bamei"] == "馬A"

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
        """オッズ情報をdict[str, float]として取得できること。"""
        provider = JVLinkDataProvider(jvlink_db)
        odds = provider.get_odds("2025010106010101")
        assert isinstance(odds, dict)
        assert len(odds) == 3
        # OddsTansyoInfo0: Umaban=01, Odds=0035 → 3.5
        assert odds["01"] == pytest.approx(3.5)
        # OddsTansyoInfo1: Umaban=02, Odds=0082 → 8.2
        assert odds["02"] == pytest.approx(8.2)
        # OddsTansyoInfo2: Umaban=03, Odds=0150 → 15.0
        assert odds["03"] == pytest.approx(15.0)

    def test_get_odds_invalid_key(self, jvlink_db: DatabaseManager) -> None:
        """race_keyが不正な場合空dictを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db)
        assert provider.get_odds("short") == {}

    def test_get_odds_invalid_table(self, jvlink_db: DatabaseManager) -> None:
        """許可されていないテーブル名でValueErrorを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db)
        with pytest.raises(ValueError, match="許可されていないオッズテーブル名"):
            provider.get_odds("2025010106010101", odds_table="INVALID_TABLE")

    def test_get_payouts(self, jvlink_db: DatabaseManager) -> None:
        """払戻情報を取得できること。"""
        provider = JVLinkDataProvider(jvlink_db)
        payouts = provider.get_payouts("2025010106010101")
        assert "tansyo" in payouts
        assert len(payouts["tansyo"]) == 1
        assert payouts["tansyo"][0]["selection"] == "03"
        assert payouts["tansyo"][0]["pay"] == 1250
        assert "fukusyo" in payouts
        assert len(payouts["fukusyo"]) == 2
        assert "umaren" in payouts
        assert len(payouts["umaren"]) == 1
        assert payouts["umaren"][0]["pay"] == 2400

    def test_get_payouts_not_found(self, jvlink_db: DatabaseManager) -> None:
        """払戻情報が存在しない場合空dictを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db)
        assert provider.get_payouts("2099010106010101") == {}

    def test_get_race_list(self, jvlink_db: DatabaseManager) -> None:
        """レース一覧を取得できること。"""
        provider = JVLinkDataProvider(jvlink_db)
        races = provider.get_race_list()
        assert len(races) == 2
        assert races[0]["RaceName"] == "中山金杯"

    def test_get_race_list_with_year(self, jvlink_db: DatabaseManager) -> None:
        """年指定でレース一覧をフィルタリングできること。"""
        provider = JVLinkDataProvider(jvlink_db)
        races = provider.get_race_list(year="2025")
        assert len(races) == 2
        races_empty = provider.get_race_list(year="2099")
        assert len(races_empty) == 0


class TestGetPreviousRaceEntry:
    """get_previous_race_entry のテスト。"""

    @pytest.fixture
    def jvlink_db_with_history(self, db_manager: DatabaseManager) -> DatabaseManager:
        """同一馬が2レースに出走するテストDB。"""
        with db_manager.connect() as conn:
            conn.execute("""
                CREATE TABLE NL_SE_RACE_UMA (
                    idYear TEXT, idMonthDay TEXT, idJyoCD TEXT, idKaiji TEXT,
                    idNichiji TEXT, idRaceNum TEXT,
                    Wakuban TEXT, Umaban TEXT, KettoNum TEXT, Bamei TEXT,
                    SexCD TEXT, Barei TEXT, Futan TEXT,
                    KisyuRyakusyo TEXT, ChokyosiRyakusyo TEXT,
                    BaTaijyu TEXT, ZogenFugo TEXT, ZogenSa TEXT,
                    KakuteiJyuni TEXT, Ninki TEXT, Odds TEXT, Time TEXT,
                    HaronTimeL3 TEXT, HaronTimeL4 TEXT,
                    DMJyuni TEXT, KyakusituKubun TEXT,
                    Jyuni1c TEXT, Jyuni2c TEXT, Jyuni3c TEXT, Jyuni4c TEXT
                )
            """)
            # 馬A: 1月1日に出走（KettoNum=0001）
            conn.execute(
                "INSERT INTO NL_SE_RACE_UMA VALUES "
                "('2025','0101','06','01','01','01',"
                "'1','01','0001','馬A','1','4','540','騎手A','調教師A',"
                "'480','+','4','3','2','35','11820','345','470','2','1','5','4','3','2')"
            )
            # 馬A: 1月5日に再出走（同じKettoNum=0001）
            conn.execute(
                "INSERT INTO NL_SE_RACE_UMA VALUES "
                "('2025','0105','06','01','02','03',"
                "'2','05','0001','馬A','1','4','540','騎手A','調教師A',"
                "'482','+','2','1','3','40','11750','338','465','1','2','3','2','1','1')"
            )
            # 馬B: 1月5日のみ出走（KettoNum=0002、前走なし）
            conn.execute(
                "INSERT INTO NL_SE_RACE_UMA VALUES "
                "('2025','0105','06','01','02','03',"
                "'1','01','0002','馬B','2','3','530','騎手B','調教師B',"
                "'460','-','2','2','5','120','12010','352','480','3','3','3','2','1','1')"
            )
        return db_manager

    def test_get_previous_race_entry(self, jvlink_db_with_history: DatabaseManager) -> None:
        """前走データを正しく取得できること。"""
        provider = JVLinkDataProvider(jvlink_db_with_history)
        # 馬A（KettoNum=0001）の1月5日レース(0105)から見た前走
        prev = provider.get_previous_race_entry("0001", "2025010506010203")
        assert prev is not None
        assert prev["KakuteiJyuni"] == "3"  # 1月1日の着順
        assert prev["HaronTimeL3"] == "345"

    def test_get_previous_race_entry_no_prev(self, jvlink_db_with_history: DatabaseManager) -> None:
        """初出走馬はNoneを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db_with_history)
        # 馬B（KettoNum=0002）は1月5日が初出走
        prev = provider.get_previous_race_entry("0002", "2025010506010203")
        assert prev is None

    def test_get_previous_race_entry_first_race(
        self, jvlink_db_with_history: DatabaseManager,
    ) -> None:
        """最初のレース時点では前走がないためNoneを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db_with_history)
        # 馬A の1月1日レース(0101)時点では前走なし
        prev = provider.get_previous_race_entry("0001", "2025010106010101")
        assert prev is None

    def test_get_previous_race_entry_empty_ketto(
        self, jvlink_db_with_history: DatabaseManager,
    ) -> None:
        """空のKettoNumでNoneを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db_with_history)
        assert provider.get_previous_race_entry("", "2025010506010203") is None

    def test_get_previous_race_entry_invalid_key(
        self, jvlink_db_with_history: DatabaseManager,
    ) -> None:
        """不正なrace_keyでNoneを返すこと。"""
        provider = JVLinkDataProvider(jvlink_db_with_history)
        assert provider.get_previous_race_entry("0001", "short") is None


class TestFetchRacesBatch:
    """fetch_races_batchのテスト。"""

    def test_batch_returns_races(self, jvlink_db: DatabaseManager) -> None:
        """バッチ取得でレースが返されること。"""
        provider = JVLinkDataProvider(jvlink_db)
        with jvlink_db.session():
            result = provider.fetch_races_batch()
        # jvlink_db fixtureにはレース01のみentriesがあるので1件
        assert len(result) >= 1
        # 必須キーの存在確認
        race = result[0]
        assert "race_key" in race
        assert "race_info" in race
        assert "entries" in race
        assert "odds" in race
        assert "payouts" in race

    def test_batch_format_matches_per_race(self, jvlink_db: DatabaseManager) -> None:
        """バッチ結果が個別取得と同一フォーマットであること。"""
        provider = JVLinkDataProvider(jvlink_db)
        race_key = "2025010106010101"

        # 個別取得
        single_info = provider.get_race_info(race_key)
        single_entries = provider.get_race_entries(race_key)
        single_odds = provider.get_odds(race_key)
        single_payouts = provider.get_payouts(race_key)

        # バッチ取得
        with jvlink_db.session():
            batch = provider.fetch_races_batch()
        race_data = [r for r in batch if r["race_key"] == race_key]
        assert len(race_data) == 1
        race_data = race_data[0]

        assert race_data["race_info"] == single_info
        assert race_data["entries"] == single_entries
        assert race_data["odds"] == single_odds
        assert race_data["payouts"] == single_payouts

    def test_batch_date_filter(self, jvlink_db: DatabaseManager) -> None:
        """日付フィルタが機能すること。"""
        provider = JVLinkDataProvider(jvlink_db)
        with jvlink_db.session():
            result = provider.fetch_races_batch(date_from="20250101", date_to="20250101")
        assert len(result) >= 1

        # 範囲外の日付ではレースなし
        with jvlink_db.session():
            result_empty = provider.fetch_races_batch(date_from="20990101", date_to="20990101")
        assert len(result_empty) == 0

    def test_batch_max_races(self, jvlink_db: DatabaseManager) -> None:
        """max_racesで取得数を制限できること。"""
        provider = JVLinkDataProvider(jvlink_db)
        with jvlink_db.session():
            result = provider.fetch_races_batch(max_races=1)
        assert len(result) <= 1

    def test_batch_without_payouts(self, jvlink_db: DatabaseManager) -> None:
        """include_payouts=Falseで払戻なしのレースが返されること。"""
        provider = JVLinkDataProvider(jvlink_db)
        with jvlink_db.session():
            result = provider.fetch_races_batch(include_payouts=False)
        for r in result:
            assert r["payouts"] == {}

    def test_batch_without_session(self, jvlink_db: DatabaseManager) -> None:
        """session()なしでもfetch_races_batchが動作すること。"""
        provider = JVLinkDataProvider(jvlink_db)
        result = provider.fetch_races_batch()
        assert len(result) >= 1


class TestDatabaseManagerSession:
    """DatabaseManager.session()のテスト。"""

    def test_session_reuses_connection(self, db_manager: DatabaseManager) -> None:
        """session中のexecute_queryが接続を再利用すること。"""
        with db_manager.session():
            result1 = db_manager.execute_query("SELECT 1 AS v")
            result2 = db_manager.execute_query("SELECT 2 AS v")
        assert result1[0]["v"] == 1
        assert result2[0]["v"] == 2

    def test_session_commits_on_exit(self, db_manager: DatabaseManager) -> None:
        """session終了時に正常commitされること。"""
        with db_manager.session() as conn:
            conn.execute("CREATE TABLE _test_session (x INTEGER)")
            conn.execute("INSERT INTO _test_session VALUES (42)")
        # session外で確認
        result = db_manager.execute_query("SELECT x FROM _test_session")
        assert result[0]["x"] == 42

    def test_without_session_still_works(self, db_manager: DatabaseManager) -> None:
        """session未使用時も従来通り動作すること。"""
        result = db_manager.execute_query("SELECT 123 AS v")
        assert result[0]["v"] == 123
