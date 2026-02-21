"""ベットエグゼキュータのテスト。"""

import os

import pytest

from src.betting.executor import BetExecutionResult, BetExecutor
from src.data.db import DatabaseManager
from src.strategy.base import Bet


def _init_bets_table(db: DatabaseManager) -> None:
    """betsテーブルを初期化する。"""
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bets (
                bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
                race_key TEXT NOT NULL,
                created_at TEXT NOT NULL,
                bet_type TEXT NOT NULL,
                selection TEXT NOT NULL,
                stake_yen INTEGER NOT NULL,
                est_prob REAL,
                odds_at_bet REAL,
                est_ev REAL,
                status TEXT DEFAULT 'PENDING',
                executed_at TEXT,
                result TEXT,
                payout_yen INTEGER DEFAULT 0,
                settled_at TEXT,
                factor_details TEXT DEFAULT '{}'
            )
        """)


@pytest.fixture
def ext_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
    _init_bets_table(db)
    return db


@pytest.fixture
def sample_bets() -> list[Bet]:
    return [
        Bet(
            race_key="2025010506010101",
            bet_type="WIN",
            selection="03",
            stake_yen=1000,
            est_prob=0.23,
            odds_at_bet=5.0,
            est_ev=1.15,
            factor_details={"DM予想上位": 1.5},
        ),
        Bet(
            race_key="2025010506010101",
            bet_type="WIN",
            selection="07",
            stake_yen=500,
            est_prob=0.09,
            odds_at_bet=12.0,
            est_ev=1.08,
            factor_details={},
        ),
    ]


class TestBetExecutor:
    """BetExecutorのテスト。"""

    def test_dryrun_mode(self, ext_db, sample_bets) -> None:
        """DRYRUNモードで全ベットがDRYRUNステータスになること。"""
        executor = BetExecutor(ext_db, method="dryrun")
        results = executor.execute_bets(sample_bets)
        assert len(results) == 2
        assert all(r.status == "DRYRUN" for r in results)

    def test_dryrun_records_to_db(self, ext_db, sample_bets) -> None:
        """DRYRUNでもDBに記録されること。"""
        executor = BetExecutor(ext_db, method="dryrun")
        executor.execute_bets(sample_bets)
        rows = ext_db.execute_query("SELECT * FROM bets")
        assert len(rows) == 2
        assert rows[0]["status"] == "DRYRUN"

    def test_ipatgo_mode(self, ext_db, sample_bets, tmp_path) -> None:
        """IPATGOモードでCSVが生成されること。"""
        csv_dir = str(tmp_path / "ipatgo")
        executor = BetExecutor(ext_db, method="ipatgo", csv_output_dir=csv_dir)
        results = executor.execute_bets(sample_bets, race_date="20250105")
        assert len(results) == 2
        assert all(r.status == "EXECUTED" for r in results)
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]
        assert len(csv_files) == 1

    def test_selenium_unavailable_returns_failed(
        self, ext_db, sample_bets, monkeypatch
    ) -> None:
        """Selenium未インストール時はFAILEDが返ること。"""
        from src.betting import selenium_executor

        monkeypatch.setattr(
            selenium_executor.SeleniumIPATExecutor,
            "is_available",
            lambda self: False,
        )
        executor = BetExecutor(ext_db, method="selenium")
        results = executor.execute_bets(sample_bets)
        assert len(results) == 2
        assert all(r.status == "FAILED" for r in results)
        assert all("インストール" in r.error_message for r in results)

    def test_selenium_login_fails_returns_failed(
        self, ext_db, sample_bets, monkeypatch
    ) -> None:
        """Seleniumログイン失敗時もFAILEDが返ること。"""
        from src.betting import selenium_executor

        monkeypatch.setattr(
            selenium_executor.SeleniumIPATExecutor,
            "is_available",
            lambda self: True,
        )
        # login()はFalseを返す（スタブの現状動作）
        executor = BetExecutor(ext_db, method="selenium")
        results = executor.execute_bets(sample_bets)
        assert len(results) == 2
        assert all(r.status == "FAILED" for r in results)

    def test_selenium_success_returns_executed(
        self, ext_db, sample_bets, monkeypatch
    ) -> None:
        """Selenium成功時にEXECUTEDが返ること。"""
        from src.betting import selenium_executor

        monkeypatch.setattr(
            selenium_executor.SeleniumIPATExecutor,
            "is_available",
            lambda self: True,
        )
        monkeypatch.setattr(
            selenium_executor.SeleniumIPATExecutor,
            "execute_bets",
            lambda self, bets: [
                {"success": True, "message": "投票成功", "screenshot": None}
                for _ in bets
            ],
        )
        executor = BetExecutor(ext_db, method="selenium")
        results = executor.execute_bets(sample_bets)
        assert len(results) == 2
        assert all(r.status == "EXECUTED" for r in results)

    def test_invalid_method(self, ext_db) -> None:
        """無効なメソッドでValueErrorになること。"""
        with pytest.raises(ValueError, match="無効な投票方式"):
            BetExecutor(ext_db, method="invalid")

    def test_empty_bets(self, ext_db) -> None:
        """空のベットリストで空結果が返ること。"""
        executor = BetExecutor(ext_db, method="dryrun")
        results = executor.execute_bets([])
        assert results == []


class TestBetDataclass:
    """Betデータクラスのテスト。"""

    def test_bet_fields(self) -> None:
        """全フィールドの確認。"""
        bet = Bet(
            race_key="2025010506010101",
            bet_type="WIN",
            selection="01",
            stake_yen=100,
            est_prob=0.15,
            odds_at_bet=5.0,
            est_ev=1.1,
            factor_details={"test": 0.5},
        )
        assert bet.race_key == "2025010506010101"
        assert bet.est_prob == 0.15
        assert bet.factor_details == {"test": 0.5}

    def test_execution_result_defaults(self) -> None:
        """BetExecutionResultのデフォルト値。"""
        result = BetExecutionResult(
            race_key="test",
            selection="01",
            bet_type="WIN",
            stake_yen=100,
            odds_at_bet=5.0,
            est_ev=1.1,
            status="DRYRUN",
        )
        assert result.executed_at == ""
        assert result.error_message == ""
