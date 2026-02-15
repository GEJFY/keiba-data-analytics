"""SearchReporterのテスト。"""

import sqlite3

import pytest

from src.search.config import SearchConfig, TrialConfig, TrialResult
from src.search.reporter import SearchReporter, SearchSummary
from src.search.result_store import ResultStore


class _InMemoryDB:
    """テスト用インメモリDBモック。"""

    def __init__(self):
        self._conn = sqlite3.connect(":memory:")
        self._conn.row_factory = sqlite3.Row

    def execute_write(self, sql, params=()):
        self._conn.execute(sql, params)
        self._conn.commit()

    def execute_query(self, sql, params=()):
        cur = self._conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    def table_exists(self, table_name: str) -> bool:
        rows = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return len(rows) > 0


def _make_trial_result(trial_id: str = "t001", **kwargs) -> TrialResult:
    config = TrialConfig(
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


@pytest.fixture()
def store():
    db = _InMemoryDB()
    s = ResultStore(db)
    s.init_tables()
    return s


@pytest.fixture()
def reporter(store):
    return SearchReporter(store)


class TestSearchSummary:
    """SearchSummaryデータクラスのテスト。"""

    def test_fields(self) -> None:
        summary = SearchSummary(
            session_id="s1",
            total_trials=10,
            completed_trials=8,
            error_trials=2,
            best_trial=None,
            top_10_trials=[],
            parameter_trends={},
            elapsed_total_seconds=60.0,
            recommendation="テスト",
        )
        assert summary.session_id == "s1"
        assert summary.total_trials == 10
        assert summary.error_trials == 2


class TestSearchReporterGenerate:
    """SearchReporter.generate()のテスト。"""

    def test_empty_session(self, reporter, store) -> None:
        """トライアルなしのセッションでサマリーを返すこと。"""
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        store.create_session(config)

        summary = reporter.generate(config.session_id)
        assert isinstance(summary, SearchSummary)
        assert summary.total_trials == 0
        assert summary.completed_trials == 0
        assert summary.best_trial is None
        assert "有効なトライアルがありません" in summary.recommendation

    def test_with_trials(self, reporter, store) -> None:
        """複数トライアルからサマリーを正しく生成すること。"""
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        for i in range(5):
            store.save_trial(sid, _make_trial_result(
                f"t{i}", composite_score=float((i + 1) * 10), roi=0.01 * i,
            ))

        summary = reporter.generate(sid)
        assert summary.total_trials == 5
        assert summary.completed_trials == 5
        assert summary.error_trials == 0
        assert summary.best_trial is not None
        assert summary.best_trial["composite_score"] == 50.0
        assert len(summary.top_10_trials) == 5

    def test_error_trials_counted(self, reporter, store) -> None:
        """エラートライアルが正しくカウントされること。"""
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        store.save_trial(sid, _make_trial_result("ok1", composite_score=50.0))
        store.save_trial(sid, _make_trial_result("err1", error="broken"))

        summary = reporter.generate(sid)
        assert summary.total_trials == 2
        assert summary.completed_trials == 1
        assert summary.error_trials == 1

    def test_parameter_trends_generated(self, reporter, store) -> None:
        """パラメータ傾向が生成されること。"""
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        for i in range(3):
            store.save_trial(sid, _make_trial_result(
                f"t{i}", composite_score=float(i * 20),
            ))

        summary = reporter.generate(sid)
        assert isinstance(summary.parameter_trends, dict)
        assert "ev_threshold" in summary.parameter_trends


class TestSearchReporterFormatReport:
    """SearchReporter.format_report()のテスト。"""

    def test_format_empty(self, reporter, store) -> None:
        """空サマリーでもフォーマットできること。"""
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)
        store.update_session_status(sid, "COMPLETED", elapsed=0.0)

        summary = reporter.generate(sid)
        report = reporter.format_report(summary)
        assert isinstance(report, str)
        assert "完了:" in report

    def test_format_with_top10(self, reporter, store) -> None:
        """上位10構成がレポートに含まれること。"""
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        for i in range(3):
            store.save_trial(sid, _make_trial_result(
                f"t{i}", composite_score=float((i + 1) * 20),
            ))
        store.update_session_status(sid, "COMPLETED", elapsed=30.0)

        summary = reporter.generate(sid)
        report = reporter.format_report(summary)
        assert "上位10構成" in report
        assert "score=" in report


class TestParameterTrends:
    """パラメータ傾向分析のテスト。"""

    def test_trends_sorted_by_avg_score(self, reporter, store) -> None:
        """傾向がavg_scoreの降順でソートされること。"""
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        for i in range(5):
            store.save_trial(sid, _make_trial_result(
                f"t{i}", composite_score=float(i * 10),
            ))

        summary = reporter.generate(sid)
        for _param, values in summary.parameter_trends.items():
            if len(values) > 1:
                for j in range(len(values) - 1):
                    assert values[j]["avg_score"] >= values[j + 1]["avg_score"]


class TestBuildRecommendation:
    """推薦テキスト生成のテスト。"""

    def test_recommendation_contains_best_config(self, reporter, store) -> None:
        """推薦に最優秀構成の情報が含まれること。"""
        config = SearchConfig(date_from="2024-01-01", date_to="2024-12-31")
        sid = store.create_session(config)

        store.save_trial(sid, _make_trial_result("best1", composite_score=80.0))

        summary = reporter.generate(sid)
        assert "最優秀構成" in summary.recommendation
        assert "EV閾値" in summary.recommendation
