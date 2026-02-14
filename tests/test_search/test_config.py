"""探索設定のテスト。"""

import numpy as np
import pytest

from src.search.config import (
    BETTING_METHODS,
    CALIBRATION_METHODS,
    EV_THRESHOLDS,
    FACTOR_SELECTIONS,
    MAX_BETS_PER_RACE_OPTIONS,
    REGULARIZATIONS,
    TARGET_JYUNI_OPTIONS,
    TRAIN_WINDOW_MONTHS,
    WF_N_WINDOWS_OPTIONS,
    SearchConfig,
    SearchSpace,
    TrialConfig,
    TrialResult,
    calculate_composite_score,
)


class TestSearchSpace:
    """SearchSpaceのテスト。"""

    def test_total_combinations(self) -> None:
        space = SearchSpace()
        expected = (
            len(TRAIN_WINDOW_MONTHS)
            * len(EV_THRESHOLDS)
            * len(REGULARIZATIONS)
            * len(TARGET_JYUNI_OPTIONS)
            * len(CALIBRATION_METHODS)
            * len(BETTING_METHODS)
            * len(WF_N_WINDOWS_OPTIONS)
            * len(MAX_BETS_PER_RACE_OPTIONS)
            * len(FACTOR_SELECTIONS)
        )
        assert space.total_combinations == expected

    def test_sample_returns_trial_config(self) -> None:
        space = SearchSpace()
        rng = np.random.default_rng(42)
        config = space.sample(rng)
        assert isinstance(config, TrialConfig)

    def test_sample_values_in_range(self) -> None:
        space = SearchSpace()
        rng = np.random.default_rng(42)
        for _ in range(50):
            config = space.sample(rng)
            assert config.train_window_months in TRAIN_WINDOW_MONTHS
            assert config.ev_threshold in EV_THRESHOLDS
            assert config.regularization in REGULARIZATIONS
            assert config.target_jyuni in TARGET_JYUNI_OPTIONS
            assert config.calibration_method in CALIBRATION_METHODS
            assert config.betting_method in BETTING_METHODS
            assert config.wf_n_windows in WF_N_WINDOWS_OPTIONS
            assert config.max_bets_per_race in MAX_BETS_PER_RACE_OPTIONS
            assert config.factor_selection in FACTOR_SELECTIONS

    def test_sample_unique_ids(self) -> None:
        space = SearchSpace()
        rng = np.random.default_rng(42)
        ids = {space.sample(rng).trial_id for _ in range(100)}
        assert len(ids) == 100

    def test_deterministic_seed(self) -> None:
        space = SearchSpace()
        rng1 = np.random.default_rng(42)
        rng2 = np.random.default_rng(42)
        c1 = space.sample(rng1)
        c2 = space.sample(rng2)
        assert c1.train_window_months == c2.train_window_months
        assert c1.ev_threshold == c2.ev_threshold
        assert c1.regularization == c2.regularization

    def test_get_dimensions(self) -> None:
        space = SearchSpace()
        dims = space.get_dimensions()
        assert "train_window_months" in dims
        assert "ev_threshold" in dims
        assert len(dims) == 9


class TestTrialConfig:
    """TrialConfigのテスト。"""

    def test_to_dict(self) -> None:
        config = TrialConfig(
            trial_id="abc123",
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
        d = config.to_dict()
        assert d["trial_id"] == "abc123"
        assert d["ev_threshold"] == 1.15
        assert len(d) == 10


class TestSearchConfig:
    """SearchConfigのテスト。"""

    def test_defaults(self) -> None:
        config = SearchConfig()
        assert config.n_trials == 500
        assert config.initial_bankroll == 1_000_000
        assert config.random_seed == 42

    def test_session_id_auto_generated(self) -> None:
        c1 = SearchConfig()
        c2 = SearchConfig()
        assert c1.session_id != c2.session_id
        assert len(c1.session_id) == 12


class TestCompositeScore:
    """複合スコアのテスト。"""

    def test_perfect_score(self) -> None:
        """最高条件でスコアが高いこと。"""
        config = TrialConfig(
            trial_id="test", train_window_months=6,
            ev_threshold=1.15, regularization=1.0,
            target_jyuni=1, calibration_method="platt",
            betting_method="quarter_kelly", wf_n_windows=5,
            max_bets_per_race=3, factor_selection="all",
        )
        result = TrialResult(
            config=config,
            sharpe_ratio=1.0,
            roi=0.10,
            max_drawdown=0.0,
            wf_overfitting_ratio=1.0,
            mc_ruin_probability=0.0,
            total_bets=200,
        )
        score = calculate_composite_score(result)
        assert score >= 90.0

    def test_worst_score(self) -> None:
        """最悪条件でスコアが低いこと。"""
        config = TrialConfig(
            trial_id="test", train_window_months=6,
            ev_threshold=1.15, regularization=1.0,
            target_jyuni=1, calibration_method="platt",
            betting_method="quarter_kelly", wf_n_windows=5,
            max_bets_per_race=3, factor_selection="all",
        )
        result = TrialResult(
            config=config,
            sharpe_ratio=-1.0,
            roi=-0.50,
            max_drawdown=0.50,
            wf_overfitting_ratio=5.0,
            mc_ruin_probability=0.50,
            total_bets=0,
        )
        score = calculate_composite_score(result)
        assert score <= 10.0

    def test_score_range(self) -> None:
        """スコアが0-100の範囲に収まること。"""
        config = TrialConfig(
            trial_id="test", train_window_months=6,
            ev_threshold=1.15, regularization=1.0,
            target_jyuni=1, calibration_method="platt",
            betting_method="quarter_kelly", wf_n_windows=5,
            max_bets_per_race=3, factor_selection="all",
        )
        result = TrialResult(config=config)
        score = calculate_composite_score(result)
        assert 0 <= score <= 100
