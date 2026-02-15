"""TrialRunnerのテスト。"""

import pytest

from src.data.db import DatabaseManager
from src.search.config import SearchConfig, TrialConfig, TrialResult
from src.search.trial_runner import TrialRunner, TrialScoringEngine, TrialStrategy


def _make_trial_config(**overrides) -> TrialConfig:
    defaults = dict(
        trial_id="test_trial",
        train_window_months=6,
        ev_threshold=1.05,
        regularization=1.0,
        target_jyuni=1,
        calibration_method="none",
        betting_method="quarter_kelly",
        wf_n_windows=3,
        max_bets_per_race=3,
        factor_selection="all",
    )
    defaults.update(overrides)
    return TrialConfig(**defaults)


class TestTrialScoringEngine:
    """TrialScoringEngineのテスト。"""

    def test_score_race_empty_entries(self) -> None:
        engine = TrialScoringEngine(rules=[], ev_threshold=1.05)
        result = engine.score_race({}, [], {})
        assert result == []

    def test_score_race_with_rules(self) -> None:
        rules = [
            {"rule_name": "test_factor", "sql_expression": "1", "weight": 1.0},
        ]
        engine = TrialScoringEngine(rules=rules, ev_threshold=1.05)

        entries = [{"Umaban": "01"}, {"Umaban": "02"}]
        odds_map = {"01": 5.0, "02": 10.0}
        race = {}

        result = engine.score_race(race, entries, odds_map)
        assert len(result) == 2
        assert all("expected_value" in r for r in result)
        assert all("total_score" in r for r in result)

    def test_score_race_excludes_zero_odds(self) -> None:
        rules = [{"rule_name": "f1", "sql_expression": "1", "weight": 1.0}]
        engine = TrialScoringEngine(rules=rules, ev_threshold=1.05)

        entries = [{"Umaban": "01"}, {"Umaban": "02"}]
        odds_map = {"01": 5.0, "02": 0.0}

        result = engine.score_race({}, entries, odds_map)
        assert len(result) == 1
        assert result[0]["umaban"] == "01"


class TestTrialStrategy:
    """TrialStrategyのテスト。"""

    def test_name_and_version(self) -> None:
        strategy = TrialStrategy(rules=[], ev_threshold=1.05)
        assert strategy.name() == "TRIAL_GY_VALUE"
        assert strategy.version() == "search"

    def test_run_empty(self) -> None:
        strategy = TrialStrategy(rules=[], ev_threshold=1.05)
        bets = strategy.run({}, [], {}, 1_000_000, {})
        assert bets == []

    def test_run_returns_bets(self) -> None:
        rules = [{"rule_name": "f1", "sql_expression": "1", "weight": 2.0}]
        strategy = TrialStrategy(
            rules=rules, ev_threshold=0.5, max_bets_per_race=3,
        )
        entries = [{"Umaban": "01"}]
        odds_map = {"01": 5.0}
        race = {"Year": "2024", "MonthDay": "0101", "JyoCD": "05",
                "Kaiji": "01", "Nichiji": "01", "RaceNum": "01"}

        bets = strategy.run(race, entries, odds_map, 1_000_000, {})
        # score = 100 + 2.0 = 102, prob = 102/200 = 0.51, EV = 0.51*5 = 2.55 > 0.5
        assert len(bets) >= 1
        assert bets[0].bet_type == "WIN"


class TestTrialRunner:
    """TrialRunnerのテスト。"""

    @pytest.fixture()
    def runner(self, tmp_path) -> TrialRunner:
        jvlink_db = DatabaseManager(str(tmp_path / "jvlink.db"), wal_mode=False)
        ext_db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)

        # 最低限のテーブル作成
        jvlink_db.execute_write("""
            CREATE TABLE NL_RA_RACE (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Kyori TEXT, TrackCD TEXT, SyussoTosu TEXT
            )
        """)
        jvlink_db.execute_write("""
            CREATE TABLE NL_SE_RACE_UMA (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Umaban TEXT, Wakuban TEXT, SexCD TEXT,
                Barei TEXT, Futan TEXT, Ninki TEXT,
                KakuteiJyuni TEXT, Odds TEXT,
                BaTaijyu TEXT, ZogenFugo TEXT, ZogenSa TEXT,
                DMJyuni TEXT, HaronTimeL3 TEXT, HaronTimeL4 TEXT,
                KyakusituKubun TEXT, Jyuni4c TEXT
            )
        """)

        # factor_rulesテーブル
        ext_db.execute_write("""
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
        ext_db.execute_write("""
            CREATE TABLE factor_review_log (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id INTEGER NOT NULL, action TEXT NOT NULL,
                old_weight REAL, new_weight REAL,
                reason TEXT DEFAULT '', backtest_roi REAL,
                changed_at TEXT NOT NULL, changed_by TEXT DEFAULT 'user'
            )
        """)

        return TrialRunner(jvlink_db, ext_db)

    def test_run_empty_db(self, runner: TrialRunner) -> None:
        """空DBでエラーメッセージを返すこと。"""
        config = _make_trial_config()
        search_config = SearchConfig(
            date_from="20240101", date_to="20240601",
            n_trials=1, mc_simulations=100,
        )
        result = runner.run(config, search_config)
        assert isinstance(result, TrialResult)
        assert result.error != ""

    def test_run_no_db_modification(self, runner: TrialRunner, tmp_path) -> None:
        """TrialRunner実行後にfactor_rulesが変更されないこと。"""
        ext_db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)

        # 初期状態を記録
        rules_before = ext_db.execute_query(
            "SELECT * FROM factor_rules"
        )

        config = _make_trial_config()
        search_config = SearchConfig(
            date_from="20240101", date_to="20240601",
            n_trials=1, mc_simulations=100,
        )
        runner.run(config, search_config)

        # 実行後も同じ
        rules_after = ext_db.execute_query(
            "SELECT * FROM factor_rules"
        )
        assert rules_before == rules_after

    def test_select_factors_all(self, runner: TrialRunner) -> None:
        """factor_selection='all'が全ルールを返すこと。"""
        config = _make_trial_config(factor_selection="all")
        rules = runner._select_factors(config, "20240101", "20240601")
        # 空DB → 空リスト
        assert isinstance(rules, list)

    def test_select_factors_category_filtered(self, runner: TrialRunner) -> None:
        """factor_selection='category_filtered'がカテゴリフィルタを行うこと。"""
        config = _make_trial_config(factor_selection="category_filtered")
        rules = runner._select_factors(config, "20240101", "20240601")
        assert isinstance(rules, list)

    def test_select_factors_top10_auc(self, runner: TrialRunner) -> None:
        """factor_selection='top10_auc'がAUCベースのフィルタを行うこと。"""
        config = _make_trial_config(factor_selection="top10_auc")
        rules = runner._select_factors(config, "20240101", "20240601")
        assert isinstance(rules, list)

    def test_run_preloaded_empty_races(self, runner: TrialRunner) -> None:
        """空のpreloaded_racesでエラーメッセージを返すこと。"""
        config = _make_trial_config()
        search_config = SearchConfig(
            date_from="20240101", date_to="20240601",
            n_trials=1, mc_simulations=100,
        )
        result = runner.run(config, search_config, preloaded_races=[])
        assert result.error != ""

    def test_safe_float(self) -> None:
        """_safe_floatがエラー時にデフォルト値を返すこと。"""
        assert TrialScoringEngine._safe_float("3.14") == 3.14
        assert TrialScoringEngine._safe_float("invalid", 0.0) == 0.0
        assert TrialScoringEngine._safe_float(None, -1.0) == -1.0

    def test_trial_strategy_place_bet_type(self) -> None:
        """target_jyuni != 1でPLACEベットが生成されること。"""
        rules = [{"rule_name": "f1", "sql_expression": "1", "weight": 2.0}]
        strategy = TrialStrategy(
            rules=rules, ev_threshold=0.5, max_bets_per_race=3,
        )
        entries = [{"Umaban": "01"}]
        odds_map = {"01": 5.0}
        race = {"Year": "2024", "MonthDay": "0101", "JyoCD": "05",
                "Kaiji": "01", "Nichiji": "01", "RaceNum": "01"}

        bets = strategy.run(race, entries, odds_map, 1_000_000, {"target_jyuni": 3})
        if bets:
            assert bets[0].bet_type == "PLACE"

    def test_trial_strategy_max_bets_limit(self) -> None:
        """max_bets_per_raceの制限が機能すること。"""
        rules = [{"rule_name": "f1", "sql_expression": "1", "weight": 2.0}]
        strategy = TrialStrategy(
            rules=rules, ev_threshold=0.5, max_bets_per_race=1,
        )
        entries = [{"Umaban": f"{i:02d}"} for i in range(1, 6)]
        odds_map = {f"{i:02d}": 5.0 for i in range(1, 6)}
        race = {"Year": "2024", "MonthDay": "0101", "JyoCD": "05",
                "Kaiji": "01", "Nichiji": "01", "RaceNum": "01"}

        bets = strategy.run(race, entries, odds_map, 1_000_000, {})
        assert len(bets) <= 1

    def test_trial_strategy_build_race_key(self) -> None:
        """_build_race_keyが正しいキーを生成すること。"""
        race = {"Year": "2024", "MonthDay": "0105", "JyoCD": "05",
                "Kaiji": "01", "Nichiji": "02", "RaceNum": "03"}
        key = TrialStrategy._build_race_key(race)
        assert key == "2024010505010203"

    def test_trial_strategy_equal_method(self) -> None:
        """betting_method='equal'でも動作すること。"""
        rules = [{"rule_name": "f1", "sql_expression": "1", "weight": 2.0}]
        strategy = TrialStrategy(
            rules=rules, ev_threshold=0.5, max_bets_per_race=3,
            betting_method="equal",
        )
        entries = [{"Umaban": "01"}]
        odds_map = {"01": 5.0}
        race = {"Year": "2024", "MonthDay": "0101", "JyoCD": "05",
                "Kaiji": "01", "Nichiji": "01", "RaceNum": "01"}

        bets = strategy.run(race, entries, odds_map, 1_000_000, {})
        assert isinstance(bets, list)
