"""拡張テーブル初期化スクリプト。

JVLinkToSQLiteが生成するDBに、本システム固有の拡張テーブルを作成する。
"""

import sqlite3
import sys
from pathlib import Path

# 拡張テーブルのCREATE文
EXTENSION_TABLES = [
    """
    CREATE TABLE IF NOT EXISTS factor_rules (
        rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_name TEXT NOT NULL,
        category TEXT DEFAULT '',
        description TEXT DEFAULT '',
        sql_expression TEXT DEFAULT '',
        weight REAL DEFAULT 1.0,
        validation_score REAL,
        is_active INTEGER DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        source TEXT DEFAULT 'manual',
        effective_from TEXT,
        effective_to TEXT,
        decay_rate REAL,
        min_sample_size INTEGER DEFAULT 100,
        review_status TEXT DEFAULT 'DRAFT',
        reviewed_at TEXT,
        training_from TEXT,
        training_to TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS factor_review_log (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        old_weight REAL,
        new_weight REAL,
        reason TEXT DEFAULT '',
        backtest_roi REAL,
        changed_at TEXT NOT NULL,
        changed_by TEXT DEFAULT 'user',
        FOREIGN KEY (rule_id) REFERENCES factor_rules(rule_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS horse_scores (
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
    """,
    """
    CREATE TABLE IF NOT EXISTS bets (
        bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
        race_key TEXT NOT NULL,
        created_at TEXT NOT NULL,
        strategy TEXT DEFAULT '',
        strategy_version TEXT DEFAULT '',
        bet_type TEXT NOT NULL,
        selection TEXT NOT NULL,
        stake_yen INTEGER NOT NULL,
        est_prob REAL,
        odds_at_bet REAL,
        odds_at_close REAL,
        est_ev REAL,
        status TEXT DEFAULT 'PENDING',
        cancel_reason TEXT,
        executed_at TEXT,
        result TEXT,
        payout_yen INTEGER DEFAULT 0,
        settled_at TEXT,
        factor_details TEXT DEFAULT '{}',
        note TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bankroll_log (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        opening_balance INTEGER NOT NULL,
        total_stake INTEGER DEFAULT 0,
        total_payout INTEGER DEFAULT 0,
        closing_balance INTEGER NOT NULL,
        pnl INTEGER DEFAULT 0,
        roi REAL DEFAULT 0.0,
        note TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS backtest_results (
        bt_id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_version TEXT DEFAULT '',
        date_from TEXT NOT NULL,
        date_to TEXT NOT NULL,
        total_races INTEGER DEFAULT 0,
        total_bets INTEGER DEFAULT 0,
        total_stake INTEGER DEFAULT 0,
        total_payout INTEGER DEFAULT 0,
        pnl INTEGER DEFAULT 0,
        roi REAL DEFAULT 0.0,
        win_rate REAL DEFAULT 0.0,
        max_drawdown REAL DEFAULT 0.0,
        sharpe_ratio REAL DEFAULT 0.0,
        params_json TEXT DEFAULT '{}',
        executed_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS data_sync_log (
        sync_id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        exit_code INTEGER,
        records_added INTEGER DEFAULT 0,
        status TEXT DEFAULT 'RUNNING',
        error_message TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date TEXT NOT NULL,
        status TEXT DEFAULT 'RUNNING',
        sync_status TEXT,
        sync_records_added INTEGER DEFAULT 0,
        races_found INTEGER DEFAULT 0,
        races_scored INTEGER DEFAULT 0,
        total_bets INTEGER DEFAULT 0,
        total_stake INTEGER DEFAULT 0,
        reconciled INTEGER DEFAULT 0,
        errors TEXT DEFAULT '[]',
        started_at TEXT NOT NULL,
        completed_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rule_set_snapshots (
        snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
        version_label TEXT NOT NULL,
        description TEXT DEFAULT '',
        trigger TEXT DEFAULT 'manual',
        calibrator_path TEXT,
        calibrator_method TEXT,
        config_json TEXT,
        created_at TEXT NOT NULL,
        created_by TEXT DEFAULT 'user'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS factor_rules_archive (
        archive_id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_id INTEGER,
        rule_id INTEGER NOT NULL,
        rule_name TEXT NOT NULL,
        category TEXT DEFAULT '',
        description TEXT DEFAULT '',
        sql_expression TEXT DEFAULT '',
        weight REAL DEFAULT 1.0,
        review_status TEXT DEFAULT 'DRAFT',
        is_active INTEGER DEFAULT 0,
        validation_score REAL,
        decay_rate REAL,
        min_sample_size INTEGER DEFAULT 100,
        source TEXT DEFAULT 'manual',
        training_from TEXT,
        training_to TEXT,
        archived_at TEXT NOT NULL,
        archived_by TEXT DEFAULT 'system',
        FOREIGN KEY (rule_id) REFERENCES factor_rules(rule_id),
        FOREIGN KEY (snapshot_id) REFERENCES rule_set_snapshots(snapshot_id)
    )
    """,
]

# インデックス
INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_factor_rules_status ON factor_rules(review_status)",
    "CREATE INDEX IF NOT EXISTS idx_factor_rules_active ON factor_rules(is_active, review_status)",
    "CREATE INDEX IF NOT EXISTS idx_horse_scores_race ON horse_scores(race_key)",
    "CREATE INDEX IF NOT EXISTS idx_bets_race ON bets(race_key)",
    "CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status)",
    "CREATE INDEX IF NOT EXISTS idx_bankroll_date ON bankroll_log(date)",
    "CREATE INDEX IF NOT EXISTS idx_data_sync_status ON data_sync_log(status)",
    "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date ON pipeline_runs(run_date)",
    "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status)",
    "CREATE INDEX IF NOT EXISTS idx_factor_rules_training ON factor_rules(training_from, training_to)",
    "CREATE INDEX IF NOT EXISTS idx_archive_snapshot ON factor_rules_archive(snapshot_id)",
    "CREATE INDEX IF NOT EXISTS idx_archive_rule ON factor_rules_archive(rule_id)",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_created ON rule_set_snapshots(created_at)",
]


def init_extension_tables(db_path: str) -> None:
    """拡張テーブルを初期化する。"""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        for ddl in EXTENSION_TABLES:
            conn.execute(ddl)
        for idx in INDEXES:
            conn.execute(idx)
        _migrate_training_columns(conn)
        _migrate_search_tables(conn)
        _migrate_version_tables(conn)
        conn.commit()
        print(f"拡張テーブル初期化完了: {path}")
    except Exception as e:
        conn.rollback()
        print(f"エラー: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


def _migrate_search_tables(conn: sqlite3.Connection) -> None:
    """search_sessions / search_trials テーブルを追加する。"""
    from src.search.result_store import SEARCH_TABLES_DDL

    for ddl in SEARCH_TABLES_DDL:
        conn.execute(ddl)


def _migrate_training_columns(conn: sqlite3.Connection) -> None:
    """既存DBにtraining_from/training_toカラムを追加する。"""
    cols = [row[1] for row in conn.execute("PRAGMA table_info(factor_rules)").fetchall()]
    if "training_from" not in cols:
        conn.execute("ALTER TABLE factor_rules ADD COLUMN training_from TEXT")
    if "training_to" not in cols:
        conn.execute("ALTER TABLE factor_rules ADD COLUMN training_to TEXT")


def _migrate_version_tables(conn: sqlite3.Connection) -> None:
    """バージョン管理テーブルを追加する（冪等）。"""
    tables = [row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    if "rule_set_snapshots" not in tables:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rule_set_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_label TEXT NOT NULL,
                description TEXT DEFAULT '',
                trigger TEXT DEFAULT 'manual',
                calibrator_path TEXT,
                calibrator_method TEXT,
                config_json TEXT,
                created_at TEXT NOT NULL,
                created_by TEXT DEFAULT 'user'
            )
        """)
    if "factor_rules_archive" not in tables:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS factor_rules_archive (
                archive_id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER,
                rule_id INTEGER NOT NULL,
                rule_name TEXT NOT NULL,
                category TEXT DEFAULT '',
                description TEXT DEFAULT '',
                sql_expression TEXT DEFAULT '',
                weight REAL DEFAULT 1.0,
                review_status TEXT DEFAULT 'DRAFT',
                is_active INTEGER DEFAULT 0,
                validation_score REAL,
                decay_rate REAL,
                min_sample_size INTEGER DEFAULT 100,
                source TEXT DEFAULT 'manual',
                training_from TEXT,
                training_to TEXT,
                archived_at TEXT NOT NULL,
                archived_by TEXT DEFAULT 'system',
                FOREIGN KEY (rule_id) REFERENCES factor_rules(rule_id),
                FOREIGN KEY (snapshot_id) REFERENCES rule_set_snapshots(snapshot_id)
            )
        """)


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else "./data/extension.db"
    init_extension_tables(db)
