"""探索結果ストアのテスト。"""

import sqlite3

import pytest

from src.search.config import SearchConfig, TrialConfig, TrialResult
from src.search.result_store import ResultStore, SEARCH_TABLES_DDL


def _make_trial_config(trial_id: str = "t001", **kwargs) -> TrialConfig:
    """テスト用TrialConfigを生成する。"""
    defaults = dict(
        trial_id=trial_id,
        train_window_months=6,
        ev_threshold=1.15,
        regularization=1.0,
        target_jyuni=1,
        calibration_method="platt",
        betting_method="quarter_kelly",
        wf_n_windows=5,
        max_bets_per_race=3,
        factor_selection="all",
    )
    defaults.update(kwargs)
    return TrialConfig(**defaults)


def _make_trial_result(trial_id: str = "t001", **kwargs) -> TrialResult:
    """テスト用TrialResultを生成する。"""
    config = _make_trial_config(trial_id=trial_id)
    defaults = dict(
        config=config,
        roi=0.05,
        sharpe_ratio=0.5,
        max_drawdown=0.10,
        total_bets=50,
        composite_score=55.0,
    )
    defaults.update(kwargs)
    return TrialResult(**defaults)


class _InMemoryDB:
    """テスト用インメモリSQLite DatabaseManagerモック。"""

    def __init__(self):
        self._conn = sqlite3.connect(":memory:")
        self._conn.row_factory = sqlite3.Row

    def execute_write(self, sql, params=()):
        self._conn.execute(sql, params)
        self._conn.commit()

    def execute_query(self, sql, params=()):
        cur = self._conn.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]

    def table_exists(self, table_name: str) -> bool:
        rows = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return len(rows) > 0


@pytest.fixture()
def memory_db():
    """インメモリSQLiteを使ったDatabaseManagerモック。"""
    return _InMemoryDB()


@pytest.fixture()
def store(memory_db) -> ResultStore:
    """初期化済みResultStore。"""
    s = ResultStore(memory_db)
    s.init_tables()
    return s


class TestResultStoreInitTables:
    """テーブル初期化のテスト。"""

    def test_tables_created(self, store, memory_db) -> None:
        assert memory_db.table_exists("search_sessions")
        assert memory_db.table_exists("search_trials")

    def test_idempotent(self, store) -> None:
        """2回呼んでもエラーにならないこと。"""
        store.init_tables()


class TestCreateSession:
    """セッション作成のテスト。"""

    def test_create_session(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)
        assert sid == config.session_id

    def test_session_retrievable(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)
        session = store.get_session(sid)
        assert session is not None
        assert session["status"] == "RUNNING"
        assert session["n_trials"] == config.n_trials


class TestSaveTrial:
    """トライアル保存のテスト。"""

    def test_save_and_retrieve(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        result = _make_trial_result("trial_001", composite_score=75.0)
        store.save_trial(sid, result)

        count = store.get_completed_count(sid)
        assert count == 1

    def test_multiple_trials(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        for i in range(5):
            result = _make_trial_result(f"trial_{i:03d}", composite_score=float(i * 10))
            store.save_trial(sid, result)

        assert store.get_completed_count(sid) == 5


class TestGetTopTrials:
    """上位トライアル取得のテスト。"""

    def test_ordered_by_score_desc(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        scores = [30.0, 90.0, 60.0, 10.0, 80.0]
        for i, sc in enumerate(scores):
            result = _make_trial_result(f"t{i:03d}", composite_score=sc)
            store.save_trial(sid, result)

        top = store.get_top_trials(sid, limit=3)
        assert len(top) == 3
        assert top[0]["composite_score"] == 90.0
        assert top[1]["composite_score"] == 80.0
        assert top[2]["composite_score"] == 60.0

    def test_excludes_error_trials(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        ok_result = _make_trial_result("ok1", composite_score=50.0)
        store.save_trial(sid, ok_result)

        err_result = _make_trial_result("err1", composite_score=99.0, error="something broke")
        store.save_trial(sid, err_result)

        top = store.get_top_trials(sid, limit=10)
        assert len(top) == 1
        assert top[0]["trial_id"] == "ok1"


class TestGetAllTrials:
    """全トライアル取得のテスト。"""

    def test_returns_all(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        for i in range(3):
            store.save_trial(sid, _make_trial_result(f"t{i}"))

        all_trials = store.get_all_trials(sid)
        assert len(all_trials) == 3


class TestUpdateSessionStatus:
    """セッション状態更新のテスト。"""

    def test_mark_completed(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        store.update_session_status(sid, "COMPLETED", best_trial_id="best1", elapsed=120.5)

        session = store.get_session(sid)
        assert session["status"] == "COMPLETED"
        assert session["best_trial_id"] == "best1"
        assert session["total_elapsed_seconds"] == 120.5


class TestGetSessions:
    """セッション一覧のテスト。"""

    def test_returns_all_sessions(self, store) -> None:
        for _ in range(3):
            store.create_session(SearchConfig(date_from="2024-01-01", date_to="2024-12-31"))

        sessions = store.get_sessions()
        assert len(sessions) == 3

    def test_empty_when_no_table(self, memory_db) -> None:
        """テーブル未作成時は空リストを返すこと。"""
        s = ResultStore(memory_db)
        assert s.get_sessions() == []


class TestGetMedianScore:
    """中央値スコアのテスト。"""

    def test_odd_count(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        for i, sc in enumerate([10.0, 30.0, 50.0]):
            store.save_trial(sid, _make_trial_result(f"t{i}", composite_score=sc))

        median = store.get_median_score(sid)
        assert median == 30.0

    def test_even_count(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        for i, sc in enumerate([10.0, 20.0, 30.0, 40.0]):
            store.save_trial(sid, _make_trial_result(f"t{i}", composite_score=sc))

        median = store.get_median_score(sid)
        assert median == 25.0

    def test_empty(self, store) -> None:
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)
        assert store.get_median_score(sid) == 0.0