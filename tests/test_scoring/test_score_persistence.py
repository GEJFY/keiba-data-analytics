"""スコア永続化のテスト。"""

import json

import pytest

from src.data.db import DatabaseManager
from src.scoring.engine import ScoringEngine


def _init_ext_db(db: DatabaseManager) -> None:
    """horse_scoresテーブルを初期化する。"""
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE horse_scores (
                score_id INTEGER PRIMARY KEY AUTOINCREMENT,
                race_key TEXT NOT NULL,
                umaban TEXT NOT NULL,
                total_score REAL NOT NULL,
                factor_details TEXT DEFAULT '{}',
                estimated_prob REAL,
                fair_odds REAL,
                actual_odds REAL,
                expected_value REAL,
                strategy_version TEXT DEFAULT '',
                calculated_at TEXT NOT NULL
            )
        """)


def _init_factor_db(db: DatabaseManager) -> None:
    """FactorRegistry用テーブルを初期化する。"""
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE factor_rules (
                rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_name TEXT, category TEXT, description TEXT,
                sql_expression TEXT, weight REAL DEFAULT 1.0,
                validation_score REAL, is_active INTEGER DEFAULT 0,
                created_at TEXT, updated_at TEXT, source TEXT,
                effective_from TEXT, effective_to TEXT, decay_rate REAL,
                min_sample_size INTEGER, review_status TEXT DEFAULT 'DRAFT',
                reviewed_at TEXT
            )
        """)


@pytest.fixture
def ext_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
    _init_ext_db(db)
    return db


@pytest.fixture
def factor_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "factor.db"), wal_mode=False)
    _init_factor_db(db)
    return db


class TestSaveScores:
    """ScoringEngine.save_scores()のテスト。"""

    def test_save_scored_results(self, factor_db, ext_db) -> None:
        """スコア結果を保存できること。"""
        engine = ScoringEngine(factor_db)
        scored = [
            {
                "umaban": "01",
                "total_score": 105.5,
                "factor_details": {"DM予想上位": 1.5, "内枠有利": 0.8},
                "estimated_prob": 0.12,
                "fair_odds": 8.3,
                "actual_odds": 10.0,
                "expected_value": 1.2,
            },
            {
                "umaban": "03",
                "total_score": 98.0,
                "factor_details": {"人気先行": -2.0},
                "estimated_prob": 0.08,
                "fair_odds": 12.5,
                "actual_odds": 5.0,
                "expected_value": 0.4,
            },
        ]
        saved = engine.save_scores("2025010506010101", scored, ext_db, "GY_VALUE v1.0.0")
        assert saved == 2

        # DB確認
        rows = ext_db.execute_query("SELECT * FROM horse_scores ORDER BY umaban")
        assert len(rows) == 2
        assert rows[0]["umaban"] == "01"
        assert rows[0]["total_score"] == 105.5
        assert rows[0]["strategy_version"] == "GY_VALUE v1.0.0"
        assert rows[0]["calculated_at"] != ""

        # factor_detailsがJSON
        details = json.loads(rows[0]["factor_details"])
        assert details["DM予想上位"] == 1.5

    def test_save_empty_results(self, factor_db, ext_db) -> None:
        """空のスコア結果で0件。"""
        engine = ScoringEngine(factor_db)
        saved = engine.save_scores("2025010506010101", [], ext_db)
        assert saved == 0

    def test_save_no_table(self, factor_db, tmp_path) -> None:
        """テーブルなしで0件（エラーなし）。"""
        bare_db = DatabaseManager(str(tmp_path / "bare.db"), wal_mode=False)
        engine = ScoringEngine(factor_db)
        saved = engine.save_scores("test", [{"umaban": "01", "total_score": 100}], bare_db)
        assert saved == 0
