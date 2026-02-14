"""バックテストエンジンの単体テスト。"""

from typing import Any

from src.backtest.engine import (
    BacktestConfig,
    BacktestEngine,
    BacktestResult,
    DailySnapshot,
)
from src.strategy.base import Bet, Strategy


class MockStrategy(Strategy):
    """テスト用の戦略実装。"""

    def __init__(self, bets_per_race: int = 1, stake: int = 1000) -> None:
        self._bets_per_race = bets_per_race
        self._stake = stake

    def name(self) -> str:
        return "mock_strategy"

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
        bets = []
        for i in range(min(self._bets_per_race, len(entries))):
            entry = entries[i] if entries else {}
            umaban = entry.get("Umaban", str(i + 1))
            bets.append(
                Bet(
                    race_key=race_data.get("race_key", "unknown"),
                    bet_type="WIN",
                    selection=umaban,
                    stake_yen=self._stake,
                    est_prob=0.2,
                    odds_at_bet=5.0,
                    est_ev=1.0,
                    factor_details={"speed": 1.0},
                )
            )
        return bets


class EmptyStrategy(Strategy):
    """投票対象なしの戦略。"""

    def name(self) -> str:
        return "empty_strategy"

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
        return []


def _make_race(
    race_key: str = "2025010106010101",
    kakutei: dict[str, str] | None = None,
    payouts: dict | None = None,
    year: str = "2025",
    monthday: str = "0101",
) -> dict[str, Any]:
    """テスト用レースデータを生成する。"""
    entries = [
        {"Umaban": "01", "Bamei": "馬A", "KakuteiJyuni": "2"},
        {"Umaban": "02", "Bamei": "馬B", "KakuteiJyuni": "1"},
    ]
    # kakutei上書き
    if kakutei:
        for e in entries:
            uma = e["Umaban"]
            if uma in kakutei:
                e["KakuteiJyuni"] = kakutei[uma]

    result: dict[str, Any] = {
        "race_info": {
            "race_key": race_key,
            "RaceName": "テストレース",
            "Year": year,
            "MonthDay": monthday,
        },
        "entries": entries,
        "odds": {"01": 5.0, "02": 8.0},
    }
    if payouts is not None:
        result["payouts"] = payouts
    return result


class TestBacktestConfig:
    """BacktestConfigデータクラスのテスト。"""

    def test_create_config(self) -> None:
        """設定が正しく生成されること。"""
        config = BacktestConfig(
            date_from="2024-01-01",
            date_to="2024-12-31",
            initial_bankroll=500_000,
            strategy_version="2.0",
        )
        assert config.date_from == "2024-01-01"
        assert config.initial_bankroll == 500_000

    def test_default_values(self) -> None:
        """デフォルト値が正しいこと。"""
        config = BacktestConfig(date_from="2024-01-01", date_to="2024-12-31")
        assert config.initial_bankroll == 1_000_000
        assert config.strategy_version == ""


class TestBacktestEngine:
    """BacktestEngineクラスのテスト。"""

    def test_run_with_bets(self) -> None:
        """ベットありの戦略でバックテストが正しく実行されること。"""
        strategy = MockStrategy(bets_per_race=1, stake=1000)
        engine = BacktestEngine(strategy)
        config = BacktestConfig(date_from="2025-01-01", date_to="2025-01-31")
        races = [_make_race("2025010106010101"), _make_race("2025010106010102")]

        result = engine.run(races, config)
        assert isinstance(result, BacktestResult)
        assert result.total_races == 2
        assert result.total_bets == 2
        assert len(result.bets) == 2

    def test_run_no_bets(self) -> None:
        """ベットなしの戦略でバックテストが正しく実行されること。"""
        strategy = EmptyStrategy()
        engine = BacktestEngine(strategy)
        config = BacktestConfig(date_from="2025-01-01", date_to="2025-01-31")
        races = [_make_race()]

        result = engine.run(races, config)
        assert result.total_races == 1
        assert result.total_bets == 0
        assert result.metrics.total_stake == 0

    def test_run_empty_races(self) -> None:
        """空のレースリストで正しく実行されること。"""
        strategy = MockStrategy()
        engine = BacktestEngine(strategy)
        config = BacktestConfig(date_from="2025-01-01", date_to="2025-01-31")

        result = engine.run([], config)
        assert result.total_races == 0
        assert result.total_bets == 0

    def test_bankroll_decreases_on_bet(self) -> None:
        """ベット分だけbankrollが減ること。"""
        strategy = MockStrategy(bets_per_race=2, stake=5000)
        engine = BacktestEngine(strategy)
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-01-31",
            initial_bankroll=100_000,
        )
        races = [_make_race()]

        result = engine.run(races, config)
        # 2ベット × 5000円 = 10000円分の投票
        assert result.metrics.total_stake == 10000

    def test_config_preserved_in_result(self) -> None:
        """結果にconfigが保存されること。"""
        strategy = MockStrategy()
        engine = BacktestEngine(strategy)
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-06-30",
            initial_bankroll=500_000,
        )
        result = engine.run([], config)
        assert result.config is config
        assert result.config.initial_bankroll == 500_000

    def test_multiple_bets_per_race(self) -> None:
        """1レースで複数ベットが正しく処理されること。"""
        strategy = MockStrategy(bets_per_race=2, stake=1000)
        engine = BacktestEngine(strategy)
        config = BacktestConfig(date_from="2025-01-01", date_to="2025-01-31")
        races = [_make_race()]

        result = engine.run(races, config)
        assert result.total_bets == 2

    def test_bankroll_increases_on_win(self) -> None:
        """的中時にbankrollが増加すること（pnl > 0）。"""
        # MockStrategyは馬番01にWINベットする
        # 馬番01を1着にして、単勝500円の払戻を設定
        strategy = MockStrategy(bets_per_race=1, stake=1000)
        engine = BacktestEngine(strategy)
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-01-31",
            initial_bankroll=100_000,
        )
        payouts = {
            "tansyo": [{"selection": "01", "pay": "500"}],
        }
        races = [_make_race(
            kakutei={"01": "1", "02": "2"},
            payouts=payouts,
            year="2025",
            monthday="0101",
        )]

        result = engine.run(races, config)
        # stake=1000, payout=500*(1000//100)=5000, pnl=+4000
        assert result.metrics.pnl > 0
        assert result.metrics.total_payout == 5000

    def test_daily_snapshots_generated(self) -> None:
        """日次スナップショットが正しく生成されること。"""
        strategy = MockStrategy(bets_per_race=1, stake=1000)
        engine = BacktestEngine(strategy)
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-01-31",
            initial_bankroll=100_000,
        )
        payouts = {"tansyo": [{"selection": "01", "pay": "500"}]}

        races = [
            _make_race("R001", kakutei={"01": "1", "02": "2"},
                       payouts=payouts, year="2025", monthday="0101"),
            _make_race("R002", kakutei={"01": "3", "02": "1"},
                       payouts={}, year="2025", monthday="0102"),
        ]

        result = engine.run(races, config)
        assert len(result.daily_snapshots) == 2
        assert isinstance(result.daily_snapshots[0], DailySnapshot)

        # 1日目: stake=1000, payout=5000 → pnl=+4000
        snap1 = result.daily_snapshots[0]
        assert snap1.date == "20250101"
        assert snap1.opening_balance == 100_000
        assert snap1.total_stake == 1000
        assert snap1.total_payout == 5000
        assert snap1.pnl == 4000

        # 2日目: stake=1000, payout=0 (01は3着、WIN不的中)
        snap2 = result.daily_snapshots[1]
        assert snap2.date == "20250102"
        assert snap2.total_stake == 1000
        assert snap2.total_payout == 0
        assert snap2.pnl == -1000

    def test_backward_compatible_without_payouts(self) -> None:
        """payoutsなしで従来の推定ベース動作を維持すること。"""
        strategy = MockStrategy(bets_per_race=1, stake=1000)
        engine = BacktestEngine(strategy)
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-01-31",
            initial_bankroll=100_000,
        )
        # payoutsキーなし（従来のレースデータ）
        races = [_make_race()]

        result = engine.run(races, config)
        assert result.total_races == 1
        assert result.total_bets == 1
        # 推定ベースのメトリクスが返る（エラーなし）
        assert result.metrics.total_stake == 1000

    def test_snapshots_sorted_by_date(self) -> None:
        """スナップショットが日付順にソートされること。"""
        strategy = MockStrategy(bets_per_race=1, stake=1000)
        engine = BacktestEngine(strategy)
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-01-31",
            initial_bankroll=100_000,
        )

        # 逆順で渡しても日付でソートされる
        races = [
            _make_race("R002", year="2025", monthday="0105"),
            _make_race("R001", year="2025", monthday="0101"),
        ]
        result = engine.run(races, config)
        dates = [s.date for s in result.daily_snapshots]
        assert dates == sorted(dates)

    def test_exclude_overlapping_factors_config(self) -> None:
        """exclude_overlapping_factorsフラグがconfigに設定できること。"""
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-01-31",
            exclude_overlapping_factors=True,
        )
        assert config.exclude_overlapping_factors is True

    def test_exclude_overlapping_factors_default(self) -> None:
        """exclude_overlapping_factorsのデフォルトがFalseであること。"""
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-01-31",
        )
        assert config.exclude_overlapping_factors is False

    def test_params_passed_to_strategy(self) -> None:
        """exclude_overlapping_factors=Trueでas_of_dateがparamsに渡ること。"""

        class ParamCapture(Strategy):
            """paramsをキャプチャする戦略。"""
            captured_params: dict[str, Any] = {}

            def name(self) -> str:
                return "param_capture"

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
                ParamCapture.captured_params = params.copy()
                return []

        strategy = ParamCapture()
        engine = BacktestEngine(strategy)
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-01-31",
            exclude_overlapping_factors=True,
        )
        races = [_make_race(year="2025", monthday="0115")]
        engine.run(races, config)

        assert "as_of_date" in ParamCapture.captured_params
        assert ParamCapture.captured_params["as_of_date"] == "2025-01-15"

    def test_params_empty_without_exclude_flag(self) -> None:
        """exclude_overlapping_factors=Falseではas_of_dateが渡らないこと。"""

        class ParamCapture2(Strategy):
            captured_params: dict[str, Any] = {}

            def name(self) -> str:
                return "param_capture2"

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
                ParamCapture2.captured_params = params.copy()
                return []

        strategy = ParamCapture2()
        engine = BacktestEngine(strategy)
        config = BacktestConfig(
            date_from="2025-01-01",
            date_to="2025-01-31",
            exclude_overlapping_factors=False,
        )
        races = [_make_race(year="2025", monthday="0115")]
        engine.run(races, config)

        assert "as_of_date" not in ParamCapture2.captured_params
