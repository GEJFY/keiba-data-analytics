"""探索結果のDB永続化。"""

from datetime import UTC, datetime
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager
from src.search.config import SearchConfig, TrialResult

SEARCH_TABLES_DDL = [
    """
    CREATE TABLE IF NOT EXISTS search_sessions (
        session_id TEXT PRIMARY KEY,
        date_from TEXT NOT NULL,
        date_to TEXT NOT NULL,
        n_trials INTEGER NOT NULL,
        initial_bankroll INTEGER NOT NULL,
        random_seed INTEGER,
        status TEXT DEFAULT 'RUNNING',
        best_trial_id TEXT,
        started_at TEXT NOT NULL,
        completed_at TEXT,
        total_elapsed_seconds REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS search_trials (
        trial_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        train_window_months INTEGER,
        ev_threshold REAL,
        regularization REAL,
        target_jyuni INTEGER,
        calibration_method TEXT,
        betting_method TEXT,
        wf_n_windows INTEGER,
        max_bets_per_race INTEGER,
        factor_selection TEXT,
        wf_avg_test_roi REAL,
        wf_avg_train_roi REAL,
        wf_overfitting_ratio REAL,
        total_bets INTEGER,
        roi REAL,
        sharpe_ratio REAL,
        max_drawdown REAL,
        win_rate REAL,
        profit_factor REAL,
        calmar_ratio REAL,
        edge REAL,
        mc_roi_5th REAL,
        mc_roi_95th REAL,
        mc_ruin_probability REAL,
        composite_score REAL,
        n_factors_used INTEGER,
        elapsed_seconds REAL,
        error TEXT DEFAULT '',
        completed_at TEXT NOT NULL,
        FOREIGN KEY (session_id) REFERENCES search_sessions(session_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_search_trials_session ON search_trials(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_search_trials_score ON search_trials(composite_score DESC)",
]


class ResultStore:
    """探索結果のDB永続化。"""

    def __init__(self, ext_db: DatabaseManager) -> None:
        self._db = ext_db

    def init_tables(self) -> None:
        """search_sessions, search_trials テーブルを作成する。"""
        for ddl in SEARCH_TABLES_DDL:
            self._db.execute_write(ddl)

    def create_session(self, config: SearchConfig) -> str:
        """セッション開始を記録する。"""
        now = datetime.now(UTC).isoformat()
        self._db.execute_write(
            """INSERT INTO search_sessions
            (session_id, date_from, date_to, n_trials,
             initial_bankroll, random_seed, status, started_at)
            VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', ?)""",
            (
                config.session_id, config.date_from, config.date_to,
                config.n_trials, config.initial_bankroll,
                config.random_seed, now,
            ),
        )
        logger.info(f"探索セッション開始: {config.session_id}")
        return config.session_id

    def save_trial(self, session_id: str, result: TrialResult) -> None:
        """トライアル結果を保存する。"""
        now = datetime.now(UTC).isoformat()
        c = result.config
        self._db.execute_write(
            """INSERT INTO search_trials
            (trial_id, session_id,
             train_window_months, ev_threshold, regularization,
             target_jyuni, calibration_method, betting_method,
             wf_n_windows, max_bets_per_race, factor_selection,
             wf_avg_test_roi, wf_avg_train_roi, wf_overfitting_ratio,
             total_bets, roi, sharpe_ratio, max_drawdown,
             win_rate, profit_factor, calmar_ratio, edge,
             mc_roi_5th, mc_roi_95th, mc_ruin_probability,
             composite_score, n_factors_used, elapsed_seconds,
             error, completed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                c.trial_id, session_id,
                c.train_window_months, c.ev_threshold, c.regularization,
                c.target_jyuni, c.calibration_method, c.betting_method,
                c.wf_n_windows, c.max_bets_per_race, c.factor_selection,
                result.wf_avg_test_roi, result.wf_avg_train_roi,
                result.wf_overfitting_ratio,
                result.total_bets, result.roi, result.sharpe_ratio,
                result.max_drawdown, result.win_rate, result.profit_factor,
                result.calmar_ratio, result.edge,
                result.mc_roi_5th, result.mc_roi_95th,
                result.mc_ruin_probability,
                result.composite_score, result.n_factors_used,
                result.elapsed_seconds, result.error, now,
            ),
        )

    def get_completed_count(self, session_id: str) -> int:
        """完了済みトライアル数。"""
        rows = self._db.execute_query(
            "SELECT COUNT(*) AS cnt FROM search_trials WHERE session_id = ?",
            (session_id,),
        )
        return rows[0]["cnt"] if rows else 0

    def get_top_trials(
        self, session_id: str, limit: int = 10,
    ) -> list[dict[str, Any]]:
        """composite_score上位のトライアルを取得する。"""
        return self._db.execute_query(
            """SELECT * FROM search_trials
            WHERE session_id = ? AND error = ''
            ORDER BY composite_score DESC LIMIT ?""",
            (session_id, limit),
        )

    def get_all_trials(self, session_id: str) -> list[dict[str, Any]]:
        """セッション内の全トライアルを取得する。"""
        return self._db.execute_query(
            "SELECT * FROM search_trials WHERE session_id = ? ORDER BY composite_score DESC",
            (session_id,),
        )

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        """セッション情報を取得する。"""
        rows = self._db.execute_query(
            "SELECT * FROM search_sessions WHERE session_id = ?",
            (session_id,),
        )
        return rows[0] if rows else None

    def get_sessions(self) -> list[dict[str, Any]]:
        """全セッション一覧を取得する。"""
        if not self._db.table_exists("search_sessions"):
            return []
        return self._db.execute_query(
            "SELECT * FROM search_sessions ORDER BY started_at DESC"
        )

    def update_session_status(
        self,
        session_id: str,
        status: str,
        best_trial_id: str = "",
        elapsed: float = 0.0,
    ) -> None:
        """セッション完了を記録する。"""
        now = datetime.now(UTC).isoformat()
        self._db.execute_write(
            """UPDATE search_sessions
            SET status = ?, best_trial_id = ?,
                completed_at = ?, total_elapsed_seconds = ?
            WHERE session_id = ?""",
            (status, best_trial_id, now, elapsed, session_id),
        )

    def get_median_score(self, session_id: str) -> float:
        """現在の中央値composite_scoreを返す。"""
        rows = self._db.execute_query(
            """SELECT composite_score FROM search_trials
            WHERE session_id = ? AND error = ''
            ORDER BY composite_score""",
            (session_id,),
        )
        if not rows:
            return 0.0
        scores = [r["composite_score"] for r in rows]
        mid = len(scores) // 2
        return float(scores[mid]) if len(scores) % 2 == 1 else float(scores[mid - 1] + scores[mid]) / 2
