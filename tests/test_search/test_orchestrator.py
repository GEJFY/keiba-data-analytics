"""ModelSearchOrchestratorのテスト。"""

import sqlite3

import pytest

from src.data.db import DatabaseManager
from src.search.config import SearchConfig
from src.search.orchestrator import ModelSearchOrchestrator
from src.search.reporter import SearchSummary


def _init_jvlink(db: DatabaseManager) -> None:
    """テスト用JVLink DBを構築する。"""
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE NL_RA_RACE (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                RaceInfoHondai TEXT, Kyori TEXT, TrackCD TEXT,
                TenkoBabaTenkoCD TEXT, TenkoBabaSibaBabaCD TEXT,
                TenkoBabaDirtBabaCD TEXT,
                JyokenInfoSyubetuCD TEXT, GradeCD TEXT,
                HassoTime TEXT, TorokuTosu TEXT, SyussoTosu TEXT,
                NyusenTosu TEXT, HaronTimeL3 TEXT, HaronTimeL4 TEXT
            )
        """)
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
        cols = ["idYear TEXT", "idMonthDay TEXT", "idJyoCD TEXT",
                "idKaiji TEXT", "idNichiji TEXT", "idRaceNum TEXT"]
        for i in range(28):
            cols.append(f"OddsTansyoInfo{i}Umaban TEXT")
            cols.append(f"OddsTansyoInfo{i}Odds TEXT")
        conn.execute(f"CREATE TABLE NL_O1_ODDS_TANFUKUWAKU ({', '.join(cols)})")
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


def _init_ext(db: DatabaseManager) -> None:
    """テスト用拡張DBを構築する。"""
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE factor_rules (
                rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_name TEXT NOT NULL, category TEXT DEFAULT '',
                description TEXT DEFAULT '', sql_expression TEXT DEFAULT '',
                weight REAL DEFAULT 1.0, validation_score REAL,
                is_active INTEGER DEFAULT 0,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                source TEXT DEFAULT 'manual',
                effective_from TEXT, effective_to TEXT,
                decay_rate REAL, min_sample_size INTEGER DEFAULT 100,
                review_status TEXT DEFAULT 'DRAFT', reviewed_at TEXT,
                training_from TEXT, training_to TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE factor_review_log (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id INTEGER NOT NULL, action TEXT NOT NULL,
                old_weight REAL, new_weight REAL,
                reason TEXT DEFAULT '', backtest_roi REAL,
                changed_at TEXT NOT NULL, changed_by TEXT DEFAULT 'user'
            )
        """)


@pytest.fixture()
def jvlink_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "jvlink.db"), wal_mode=False)
    _init_jvlink(db)
    return db


@pytest.fixture()
def ext_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
    _init_ext(db)
    return db


class TestModelSearchOrchestrator:
    """ModelSearchOrchestratorのテスト。"""

    def test_init(self, jvlink_db, ext_db) -> None:
        """正常に初期化できること。"""
        config = SearchConfig(
            date_from="20240101", date_to="20240601",
            n_trials=2, mc_simulations=100,
        )
        orch = ModelSearchOrchestrator(jvlink_db, ext_db, config)
        assert orch._config == config
        assert orch._store is not None
        assert orch._runner is not None
        assert orch._reporter is not None

    def test_run_empty_db(self, jvlink_db, ext_db) -> None:
        """空DBで探索実行しても正常完了すること。"""
        config = SearchConfig(
            date_from="20240101", date_to="20240601",
            n_trials=2, mc_simulations=100,
        )
        orch = ModelSearchOrchestrator(jvlink_db, ext_db, config)
        summary = orch.run()

        assert isinstance(summary, SearchSummary)
        assert summary.session_id == config.session_id
        # レースデータなし → トライアル0件
        assert summary.total_trials == 0

    def test_run_creates_session(self, jvlink_db, ext_db) -> None:
        """探索実行でセッションが作成されること。"""
        config = SearchConfig(
            date_from="20240101", date_to="20240601",
            n_trials=1, mc_simulations=100,
        )
        orch = ModelSearchOrchestrator(jvlink_db, ext_db, config)
        orch.run()

        session = orch._store.get_session(config.session_id)
        assert session is not None
        assert session["status"] == "COMPLETED"

    def test_resume_nonexistent_session(self, jvlink_db, ext_db) -> None:
        """存在しないセッションIDでresumeするとValueErrorになること。"""
        config = SearchConfig(
            date_from="20240101", date_to="20240601",
            n_trials=1, mc_simulations=100,
        )
        orch = ModelSearchOrchestrator(jvlink_db, ext_db, config)
        orch._store.init_tables()

        with pytest.raises(ValueError, match="見つかりません"):
            orch.resume("nonexistent_session")

    def test_resume_completed_session(self, jvlink_db, ext_db) -> None:
        """全トライアル完了済みセッションのresumeでサマリーが返ること。"""
        config = SearchConfig(
            date_from="20240101", date_to="20240601",
            n_trials=1, mc_simulations=100,
        )
        orch = ModelSearchOrchestrator(jvlink_db, ext_db, config)

        # まず通常実行
        orch.run()

        # resumeしても正常完了
        summary = orch.resume(config.session_id)
        assert isinstance(summary, SearchSummary)

    def test_search_tables_initialized(self, jvlink_db, ext_db) -> None:
        """run()でsearch_sessionsとsearch_trialsテーブルが作成されること。"""
        config = SearchConfig(
            date_from="20240101", date_to="20240601",
            n_trials=1, mc_simulations=100,
        )
        orch = ModelSearchOrchestrator(jvlink_db, ext_db, config)
        orch.run()

        assert ext_db.table_exists("search_sessions")
        assert ext_db.table_exists("search_trials")
