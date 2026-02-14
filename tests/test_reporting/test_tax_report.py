"""税務レポートのテスト。"""

import pytest

from src.data.db import DatabaseManager
from src.reporting.tax_report import TaxReport, TaxReportGenerator


@pytest.fixture
def ext_db(tmp_path) -> DatabaseManager:
    """テスト用DB。"""
    db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE bets (
                bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
                race_key TEXT, bet_type TEXT, selection TEXT,
                stake_yen INTEGER, odds_at_bet REAL, est_ev REAL,
                est_prob REAL, status TEXT, result TEXT,
                payout_yen INTEGER DEFAULT 0, settled_at TEXT,
                created_at TEXT
            )
        """)
        # 2025年の的中ベット
        conn.execute("""
            INSERT INTO bets (race_key, bet_type, selection, stake_yen,
                odds_at_bet, est_ev, status, result, payout_yen, settled_at)
            VALUES ('R001', 'WIN', '03', 1000, 5.0, 1.15, 'SETTLED', 'WIN', 5000, '2025-03-15T12:00:00')
        """)
        # 2025年のハズレベット
        conn.execute("""
            INSERT INTO bets (race_key, bet_type, selection, stake_yen,
                odds_at_bet, est_ev, status, result, payout_yen, settled_at)
            VALUES ('R002', 'WIN', '01', 1000, 8.0, 1.05, 'SETTLED', 'LOSE', 0, '2025-03-15T14:00:00')
        """)
        # もう1件的中
        conn.execute("""
            INSERT INTO bets (race_key, bet_type, selection, stake_yen,
                odds_at_bet, est_ev, status, result, payout_yen, settled_at)
            VALUES ('R003', 'WIN', '05', 2000, 3.0, 1.10, 'SETTLED', 'WIN', 6000, '2025-06-20T10:00:00')
        """)
    return db


class TestTaxReportGenerator:
    """TaxReportGeneratorのテスト。"""

    def test_generate_basic(self, ext_db: DatabaseManager) -> None:
        """基本的なレポートが生成されること。"""
        gen = TaxReportGenerator(ext_db)
        report = gen.generate(2025)
        assert isinstance(report, TaxReport)
        assert report.year == 2025
        assert report.n_bets == 3
        assert report.n_wins == 2

    def test_total_amounts(self, ext_db: DatabaseManager) -> None:
        """金額が正しく集計されること。"""
        gen = TaxReportGenerator(ext_db)
        report = gen.generate(2025)
        assert report.total_stake == 4000  # 1000 + 1000 + 2000
        assert report.total_payout == 11000  # 5000 + 0 + 6000
        assert report.winning_stake == 3000  # 的中分のみ: 1000 + 2000

    def test_ichiji_shotoku_calculation(self, ext_db: DatabaseManager) -> None:
        """一時所得が正しく計算されること。"""
        gen = TaxReportGenerator(ext_db)
        report = gen.generate(2025)
        # 総収入 = 11000
        # 控除経費 = 3000（的中分の購入費）
        # 特別控除 = 500000
        # 一時所得 = max(0, 11000 - 3000 - 500000) = 0
        assert report.ichiji_shotoku == 0
        assert report.taxable_amount == 0

    def test_high_payout_ichiji_shotoku(self, tmp_path) -> None:
        """高額払戻で一時所得が発生すること。"""
        db = DatabaseManager(str(tmp_path / "high.db"), wal_mode=False)
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
            conn.execute("""
                INSERT INTO bets (race_key, bet_type, selection, stake_yen,
                    odds_at_bet, est_ev, status, result, payout_yen, settled_at)
                VALUES ('R001', 'WIN', '01', 10000, 100.0, 2.0, 'SETTLED', 'WIN', 1000000, '2025-01-01T12:00:00')
            """)
        gen = TaxReportGenerator(db)
        report = gen.generate(2025)
        # 総収入 = 1,000,000
        # 控除経費 = 10,000（的中分の購入費）
        # 特別控除 = 500,000
        # 一時所得 = 1,000,000 - 10,000 - 500,000 = 490,000
        assert report.ichiji_shotoku == 490_000
        # 課税対象 = 490,000 / 2 = 245,000
        assert report.taxable_amount == 245_000

    def test_monthly_breakdown(self, ext_db: DatabaseManager) -> None:
        """月次内訳が正しいこと。"""
        gen = TaxReportGenerator(ext_db)
        report = gen.generate(2025)
        assert len(report.monthly_breakdown) == 2
        # 3月: 2件（的中1件、ハズレ1件）
        mar = report.monthly_breakdown[0]
        assert mar.month == "2025-03"
        assert mar.n_bets == 2
        assert mar.n_wins == 1
        # 6月: 1件（的中）
        jun = report.monthly_breakdown[1]
        assert jun.month == "2025-06"
        assert jun.n_bets == 1
        assert jun.n_wins == 1

    def test_top_payouts(self, ext_db: DatabaseManager) -> None:
        """高額払戻が降順でリストされること。"""
        gen = TaxReportGenerator(ext_db)
        report = gen.generate(2025)
        assert len(report.top_payouts) == 2  # 的中2件
        assert report.top_payouts[0]["payout"] >= report.top_payouts[1]["payout"]

    def test_empty_year(self, ext_db: DatabaseManager) -> None:
        """データのない年で空レポート。"""
        gen = TaxReportGenerator(ext_db)
        report = gen.generate(2020)
        assert report.n_bets == 0
        assert report.total_payout == 0
        assert report.ichiji_shotoku == 0

    def test_no_bets_table(self, tmp_path) -> None:
        """betsテーブルなしで空レポート。"""
        db = DatabaseManager(str(tmp_path / "empty.db"), wal_mode=False)
        gen = TaxReportGenerator(db)
        report = gen.generate(2025)
        assert report.n_bets == 0

    def test_format_summary(self, ext_db: DatabaseManager) -> None:
        """テキストサマリーが生成されること。"""
        gen = TaxReportGenerator(ext_db)
        report = gen.generate(2025)
        summary = gen.format_summary(report)
        assert "2025年" in summary
        assert "一時所得" in summary
        assert "課税対象額" in summary
        assert "月次内訳" in summary
