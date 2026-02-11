"""戦略基底クラスの単体テスト。"""

from typing import Any

import pytest

from src.strategy.base import Bet, Strategy


class ConcreteStrategy(Strategy):
    """テスト用の具象戦略クラス。"""

    def name(self) -> str:
        return "value_bet_v1"

    def version(self) -> str:
        return "1.2.0"

    def run(
        self,
        race_data: dict[str, Any],
        entries: list[dict[str, Any]],
        odds: dict[str, float],
        bankroll: int,
        params: dict[str, Any],
    ) -> list[Bet]:
        bets = []
        threshold = params.get("ev_threshold", 1.05)
        for entry in entries:
            umaban = entry.get("Umaban", "")
            entry_odds = odds.get(umaban, 0.0)
            est_prob = entry.get("est_prob", 0.1)
            ev = est_prob * entry_odds
            if ev > threshold and entry_odds > 0:
                bets.append(
                    Bet(
                        race_key=race_data.get("race_key", ""),
                        bet_type="WIN",
                        selection=umaban,
                        stake_yen=min(bankroll // 20, 10000),
                        est_prob=est_prob,
                        odds_at_bet=entry_odds,
                        est_ev=ev,
                        factor_details={"speed": 1.0, "pace": 0.5},
                    )
                )
        return bets


class TestBet:
    """Betデータクラスのテスト。"""

    def test_create_bet(self) -> None:
        """Betが正しく生成されること。"""
        bet = Bet(
            race_key="2025010106010101",
            bet_type="WIN",
            selection="05",
            stake_yen=5000,
            est_prob=0.25,
            odds_at_bet=5.0,
            est_ev=1.25,
            factor_details={"speed": 2.0, "form": -0.5},
        )
        assert bet.race_key == "2025010106010101"
        assert bet.bet_type == "WIN"
        assert bet.selection == "05"
        assert bet.stake_yen == 5000
        assert bet.est_prob == 0.25
        assert bet.odds_at_bet == 5.0
        assert bet.est_ev == 1.25
        assert bet.factor_details == {"speed": 2.0, "form": -0.5}

    def test_bet_equality(self) -> None:
        """同値のBetが等しいこと。"""
        kwargs: dict[str, Any] = {
            "race_key": "2025010106010101",
            "bet_type": "WIN",
            "selection": "01",
            "stake_yen": 1000,
            "est_prob": 0.2,
            "odds_at_bet": 5.0,
            "est_ev": 1.0,
            "factor_details": {},
        }
        assert Bet(**kwargs) == Bet(**kwargs)


class TestStrategy:
    """Strategy ABCのテスト。"""

    def test_cannot_instantiate_abstract(self) -> None:
        """抽象クラスを直接インスタンス化できないこと。"""
        with pytest.raises(TypeError):
            Strategy()  # type: ignore[abstract]

    def test_concrete_strategy_name(self) -> None:
        """具象クラスがname()を返すこと。"""
        strategy = ConcreteStrategy()
        assert strategy.name() == "value_bet_v1"

    def test_concrete_strategy_version(self) -> None:
        """具象クラスがversion()を返すこと。"""
        strategy = ConcreteStrategy()
        assert strategy.version() == "1.2.0"

    def test_run_returns_bets(self) -> None:
        """run()が条件を満たす馬のBetリストを返すこと。"""
        strategy = ConcreteStrategy()
        race_data = {"race_key": "2025010106010101"}
        entries = [
            {"Umaban": "01", "est_prob": 0.3},
            {"Umaban": "02", "est_prob": 0.1},
        ]
        odds = {"01": 5.0, "02": 3.0}

        bets = strategy.run(race_data, entries, odds, bankroll=100_000, params={})
        # 馬01: EV = 0.3 * 5.0 = 1.5 > 1.05 → 投票
        # 馬02: EV = 0.1 * 3.0 = 0.3 < 1.05 → スキップ
        assert len(bets) == 1
        assert bets[0].selection == "01"
        assert bets[0].est_ev == pytest.approx(1.5)

    def test_run_returns_empty_on_no_value(self) -> None:
        """条件を満たす馬がない場合、空リストを返すこと。"""
        strategy = ConcreteStrategy()
        race_data = {"race_key": "2025010106010101"}
        entries = [{"Umaban": "01", "est_prob": 0.05}]
        odds = {"01": 3.0}

        bets = strategy.run(race_data, entries, odds, bankroll=100_000, params={})
        assert bets == []

    def test_run_respects_params(self) -> None:
        """paramsのev_thresholdが反映されること。"""
        strategy = ConcreteStrategy()
        race_data = {"race_key": "2025010106010101"}
        entries = [{"Umaban": "01", "est_prob": 0.3}]
        odds = {"01": 5.0}

        # 閾値を上げて条件を厳しくする
        bets = strategy.run(race_data, entries, odds, bankroll=100_000, params={"ev_threshold": 2.0})
        assert bets == []  # EV = 1.5 < 2.0
