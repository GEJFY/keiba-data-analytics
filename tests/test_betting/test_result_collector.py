"""レース結果収集・ベット照合のテスト。"""

import pytest

from src.betting.result_collector import ResultCollector
from src.data.db import DatabaseManager


def _init_jvlink(db: DatabaseManager) -> None:
    """JVLink DBのテストデータ。"""
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE NL_SE_RACE_UMA (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Umaban TEXT, Bamei TEXT, KakuteiJyuni TEXT,
                Wakuban TEXT, SexCD TEXT, Barei TEXT, Futan TEXT,
                KisyuRyakusyo TEXT, ChokyosiRyakusyo TEXT,
                BaTaijyu TEXT, ZogenFugo TEXT,
                ZogenSa TEXT, Ninki TEXT, Odds TEXT, Time TEXT,
                HaronTimeL3 TEXT, HaronTimeL4 TEXT,
                DMJyuni TEXT, KyakusituKubun TEXT,
                Jyuni1c TEXT, Jyuni2c TEXT, Jyuni3c TEXT, Jyuni4c TEXT,
                KettoNum TEXT
            )
        """)
        # 3頭のテストデータ（1着=03, 2着=01, 3着=07）
        for uma, jyuni, ninki in [("01", "2", "3"), ("03", "1", "1"), ("07", "3", "2")]:
            conn.execute(
                """INSERT INTO NL_SE_RACE_UMA VALUES
                   ('2025','0105','06','01','01','01',?,?||'号馬',?,
                    '1','1','4','55.0','テスト騎手','テスト調教師',
                    '480','+','4',?,
                    '0500','1350','345','345',
                    '1','1','1','1','1','1','000000000')""",
                (uma, uma, jyuni, ninki),
            )

        # NL_HR_PAY: 0-indexed format (provider._extract_pay_entriesが使うカラム名)
        conn.execute("""
            CREATE TABLE NL_HR_PAY (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                PayTansyo0Umaban TEXT, PayTansyo0Pay TEXT, PayTansyo0Ninki TEXT,
                PayFukusyo0Umaban TEXT, PayFukusyo0Pay TEXT, PayFukusyo0Ninki TEXT,
                PayFukusyo1Umaban TEXT, PayFukusyo1Pay TEXT, PayFukusyo1Ninki TEXT,
                PayFukusyo2Umaban TEXT, PayFukusyo2Pay TEXT, PayFukusyo2Ninki TEXT
            )
        """)
        conn.execute("""
            INSERT INTO NL_HR_PAY VALUES
            ('2025','0105','06','01','01','01',
             '03','500','01',
             '03','200','01','01','350','02','07','800','03')
        """)


def _init_ext(db: DatabaseManager) -> None:
    """拡張DBのテストデータ。"""
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE bets (
                bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
                race_key TEXT, bet_type TEXT, selection TEXT,
                stake_yen INTEGER, odds_at_bet REAL, est_ev REAL,
                status TEXT, result TEXT, payout_yen INTEGER DEFAULT 0,
                settled_at TEXT
            )
        """)
        # 単勝03（的中）
        conn.execute("""
            INSERT INTO bets (race_key, bet_type, selection, stake_yen, odds_at_bet, est_ev, status)
            VALUES ('2025010506010101', 'WIN', '03', 1000, 5.0, 1.15, 'EXECUTED')
        """)
        # 単勝01（不的中 — 2着）
        conn.execute("""
            INSERT INTO bets (race_key, bet_type, selection, stake_yen, odds_at_bet, est_ev, status)
            VALUES ('2025010506010101', 'WIN', '01', 500, 8.0, 1.05, 'EXECUTED')
        """)
        # 複勝07（的中 — 3着以内）
        conn.execute("""
            INSERT INTO bets (race_key, bet_type, selection, stake_yen, odds_at_bet, est_ev, status)
            VALUES ('2025010506010101', 'PLACE', '07', 1000, 3.0, 1.10, 'DRYRUN')
        """)


@pytest.fixture
def jvlink_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "jvlink.db"), wal_mode=False)
    _init_jvlink(db)
    return db


@pytest.fixture
def ext_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
    _init_ext(db)
    return db


class TestResultCollector:
    """ResultCollectorのテスト。"""

    def test_collect_results(self, jvlink_db, ext_db) -> None:
        """レース結果の収集。"""
        collector = ResultCollector(jvlink_db, ext_db)
        result = collector.collect_results("2025010506010101")
        assert result["race_key"] == "2025010506010101"
        assert "payouts" in result
        assert "kakutei_jyuni" in result
        # 馬番03が1着
        assert result["kakutei_jyuni"]["03"] == 1
        assert result["kakutei_jyuni"]["01"] == 2

    def test_reconcile_bets_win(self, jvlink_db, ext_db) -> None:
        """単勝の的中照合。"""
        collector = ResultCollector(jvlink_db, ext_db)
        updated = collector.reconcile_bets("2025010506010101")
        # 3件のベットが照合される
        assert len(updated) == 3

        # 馬番03は単勝的中
        win_bet = next(b for b in updated if b["selection"] == "03")
        assert win_bet["result"] == "WIN"
        assert win_bet["payout_yen"] > 0  # 500 * (1000/100) = 5000

        # 馬番01は単勝不的中
        lose_bet = next(b for b in updated if b["selection"] == "01")
        assert lose_bet["result"] == "LOSE"
        assert lose_bet["payout_yen"] == 0

    def test_reconcile_bets_place(self, jvlink_db, ext_db) -> None:
        """複勝の的中照合。"""
        collector = ResultCollector(jvlink_db, ext_db)
        updated = collector.reconcile_bets("2025010506010101")

        # 馬番07は複勝的中（3着以内）
        place_bet = next(b for b in updated if b["selection"] == "07")
        assert place_bet["result"] == "WIN"
        assert place_bet["payout_yen"] > 0

    def test_reconcile_all_pending(self, jvlink_db, ext_db) -> None:
        """一括照合。"""
        collector = ResultCollector(jvlink_db, ext_db)
        count = collector.reconcile_all_pending()
        assert count == 3

    def test_reconcile_no_bets_table(self, jvlink_db, tmp_path) -> None:
        """betsテーブルなしで空リスト。"""
        bare_db = DatabaseManager(str(tmp_path / "bare.db"), wal_mode=False)
        collector = ResultCollector(jvlink_db, bare_db)
        assert collector.reconcile_bets("2025010506010101") == []
        assert collector.reconcile_all_pending() == 0

    def test_reconcile_no_pending(self, jvlink_db, ext_db) -> None:
        """照合済みベットのみの場合。"""
        collector = ResultCollector(jvlink_db, ext_db)
        collector.reconcile_all_pending()
        # 2回目は照合対象なし
        count = collector.reconcile_all_pending()
        assert count == 0


class TestCalculatePayout:
    """_calculate_payoutのテスト。

    provider.get_payouts()が返す形式:
    {"tansyo": [{"selection": "03", "pay": 500, "ninki": 1}], ...}
    """

    def test_win_hit(self) -> None:
        """単勝的中。"""
        payout = ResultCollector._calculate_payout(
            "WIN", "03", 1000,
            {"tansyo": [{"selection": "03", "pay": 500, "ninki": 1}]},
            {"03": 1},
        )
        assert payout == 5000  # 500 * (1000/100)

    def test_win_miss(self) -> None:
        """単勝不的中。"""
        payout = ResultCollector._calculate_payout(
            "WIN", "01", 1000,
            {"tansyo": [{"selection": "03", "pay": 500, "ninki": 1}]},
            {"01": 3},
        )
        assert payout == 0

    def test_place_hit(self) -> None:
        """複勝的中。"""
        payout = ResultCollector._calculate_payout(
            "PLACE", "07", 1000,
            {"fukusyo": [{"selection": "07", "pay": 300, "ninki": 3}]},
            {"07": 3},
        )
        assert payout == 3000

    def test_place_miss(self) -> None:
        """複勝不的中（4着以下）。"""
        payout = ResultCollector._calculate_payout(
            "PLACE", "05", 1000,
            {"fukusyo": []},
            {"05": 5},
        )
        assert payout == 0

    def test_unknown_bet_type(self) -> None:
        """未対応の券種は0。"""
        payout = ResultCollector._calculate_payout(
            "EXACTA", "01-03", 100,
            {}, {"01": 1, "03": 2},
        )
        assert payout == 0
