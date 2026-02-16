"""Walk-Forwardバックテストのテスト。"""

from typing import Any

import pytest

from src.backtest.walk_forward import (
    WalkForwardEngine,
    WalkForwardResult,
    WalkForwardWindow,
    _filter_races,
    _parse_date,
)
from src.strategy.base import Bet, Strategy


class MockWFStrategy(Strategy):
    """Walk-Forwardテスト用の戦略。"""

    def name(self) -> str:
        return "mock_wf"

    def version(self) -> str:
        return "1.0.0"

    def run(
        self,
        race_data: dict[str, Any],
        entries: list[dict[str, Any]],
        odds: dict[str, float],
        bankroll: int,
        params: dict[str, Any],
    ) -> list[Bet]:
        if not entries:
            return []
        return [Bet(
            race_key=race_data.get("race_key", ""),
            bet_type="WIN",
            selection="01",
            stake_yen=1000,
            est_prob=0.2,
            odds_at_bet=5.0,
            est_ev=1.0,
            factor_details={},
        )]


def _make_race(year: str, monthday: str, race_key: str = "") -> dict:
    return {
        "race_info": {
            "race_key": race_key or f"{year}{monthday}06010101",
            "Year": year,
            "MonthDay": monthday,
            "RaceName": "テスト",
        },
        "entries": [
            {"Umaban": "01", "Bamei": "A", "KakuteiJyuni": "1"},
            {"Umaban": "02", "Bamei": "B", "KakuteiJyuni": "2"},
        ],
        "odds": {"01": 5.0, "02": 8.0},
    }


class TestWalkForwardWindow:
    def test_create_window(self) -> None:
        w = WalkForwardWindow(
            window_id=1,
            train_from="20240101", train_to="20240630",
            test_from="20240701", test_to="20240930",
        )
        assert w.train_roi == 0.0
        assert w.test_roi == 0.0

    def test_overfitting_ratio_no_results(self) -> None:
        w = WalkForwardWindow(
            window_id=1,
            train_from="20240101", train_to="20240630",
            test_from="20240701", test_to="20240930",
        )
        assert w.overfitting_ratio == 0.0


class TestGenerateWindows:
    def test_basic_generation(self) -> None:
        windows = WalkForwardEngine.generate_windows(
            "20240101", "20241231", n_windows=3,
        )
        assert len(windows) > 0
        for w in windows:
            assert w.train_from < w.train_to
            assert w.train_to < w.test_from
            assert w.test_from <= w.test_to

    def test_too_short_period_raises(self) -> None:
        with pytest.raises(ValueError, match="期間が短すぎます"):
            WalkForwardEngine.generate_windows("20240101", "20240115", n_windows=5)

    def test_windows_sorted_by_date(self) -> None:
        windows = WalkForwardEngine.generate_windows(
            "20240101", "20241231", n_windows=4,
        )
        dates = [w.test_from for w in windows]
        assert dates == sorted(dates)


class TestWalkForwardEngine:
    def test_run_with_races(self) -> None:
        strategy = MockWFStrategy()
        engine = WalkForwardEngine(strategy)

        # 1年分のレースデータ（月2回）
        races = []
        for m in range(1, 13):
            for d in [5, 20]:
                md = f"{m:02d}{d:02d}"
                races.append(_make_race("2024", md))

        windows = WalkForwardEngine.generate_windows(
            "20240101", "20241231", n_windows=3,
        )
        result = engine.run(races, windows)
        assert isinstance(result, WalkForwardResult)
        assert len(result.windows) == len(windows)

    def test_run_empty_races(self) -> None:
        strategy = MockWFStrategy()
        engine = WalkForwardEngine(strategy)
        windows = [WalkForwardWindow(
            window_id=1,
            train_from="20240101", train_to="20240630",
            test_from="20240701", test_to="20240930",
        )]
        result = engine.run([], windows)
        assert result.total_test_bets == 0

    def test_overfitting_detection(self) -> None:
        result = WalkForwardResult(
            windows=[],
            avg_train_roi=0.5,
            avg_test_roi=0.1,
            avg_overfitting_ratio=5.0,
        )
        assert result.is_overfitting is True

    def test_no_overfitting(self) -> None:
        result = WalkForwardResult(
            windows=[],
            avg_train_roi=0.1,
            avg_test_roi=0.08,
            avg_overfitting_ratio=1.25,
        )
        assert result.is_overfitting is False


class TestRunDynamic:
    """run_dynamic()のテスト。"""

    def test_run_dynamic_returns_result(self) -> None:
        """run_dynamic()がWalkForwardResultを返すこと。"""
        # run_dynamicはDB依存のため、ここではインターフェースの存在確認
        strategy = MockWFStrategy()
        engine = WalkForwardEngine(strategy)
        assert hasattr(engine, "run_dynamic")

    def test_run_dynamic_empty_windows(self) -> None:
        """空ウィンドウリストではベットなしの結果を返すこと。"""
        strategy = MockWFStrategy()
        engine = WalkForwardEngine(strategy)

        # DB不要のケース: ウィンドウリストが空
        from unittest.mock import MagicMock

        mock_jvlink = MagicMock()
        mock_ext = MagicMock()

        result = engine.run_dynamic(
            races=[],
            windows=[],
            jvlink_db=mock_jvlink,
            ext_db=mock_ext,
        )
        assert isinstance(result, WalkForwardResult)
        assert result.total_test_bets == 0
        assert result.total_train_bets == 0

    def test_run_dynamic_signature(self) -> None:
        """run_dynamic()が必要なパラメータを受け付けること。"""
        import inspect

        sig = inspect.signature(WalkForwardEngine.run_dynamic)
        params = list(sig.parameters.keys())
        assert "jvlink_db" in params
        assert "ext_db" in params
        assert "target_jyuni" in params
        assert "regularization" in params
        assert "calibration_method" in params


class TestHelpers:
    def test_parse_date_yyyymmdd(self) -> None:
        d = _parse_date("20240315")
        assert d.year == 2024
        assert d.month == 3
        assert d.day == 15

    def test_parse_date_iso(self) -> None:
        d = _parse_date("2024-03-15")
        assert d.year == 2024

    def test_filter_races(self) -> None:
        races = [
            _make_race("2024", "0301"),
            _make_race("2024", "0601"),
            _make_race("2024", "0901"),
        ]
        filtered = _filter_races(races, "20240401", "20240831")
        assert len(filtered) == 1
        assert filtered[0]["race_info"]["MonthDay"] == "0601"
