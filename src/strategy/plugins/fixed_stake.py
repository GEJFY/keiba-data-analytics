"""固定金額戦略プラグイン。

GY指数スコアリング結果に対して、固定金額で投票する戦略。
Quarter Kellyではなく一定額を賭けるシンプルな方式で、
GY_VALUE戦略との比較ベンチマークに使用する。
"""

from typing import Any

from loguru import logger

from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider
from src.scoring.engine import ScoringEngine
from src.strategy.base import Bet, Strategy


class FixedStakeStrategy(Strategy):
    """固定金額ベット戦略。

    EV閾値を超える馬に対して、一定金額で単勝ベットを行う。
    投票額はbankrollに依存しない。

    Args:
        ext_db: 拡張DB（factor_rules格納先）
        jvlink_db: JVLink DB（前走データ取得用）
        ev_threshold: EV閾値
        stake_yen: 1ベットあたりの固定金額（円）
        max_bets_per_race: 1レースあたり最大ベット数
    """

    def __init__(
        self,
        ext_db: DatabaseManager,
        jvlink_db: DatabaseManager | None = None,
        ev_threshold: float = 1.05,
        stake_yen: int = 1000,
        max_bets_per_race: int = 3,
    ) -> None:
        self._ext_db = ext_db
        self._ev_threshold = ev_threshold
        self._stake_yen = stake_yen
        self._max_bets_per_race = max_bets_per_race
        provider = JVLinkDataProvider(jvlink_db) if jvlink_db else None
        self._engine = ScoringEngine(
            ext_db, ev_threshold=ev_threshold, jvlink_provider=provider,
        )

    def name(self) -> str:
        return "FIXED_STAKE"

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
        """固定金額戦略を実行する。

        Args:
            race_data: レース情報
            entries: 出走馬リスト
            odds: {馬番: オッズ} のdict
            bankroll: 現在の残高（使用しない）
            params: 追加パラメータ
                - ev_threshold: EV閾値の上書き
                - stake_yen: 固定金額の上書き
                - max_bets_per_race: 最大ベット数の上書き
                - as_of_date: 時点日フィルタ

        Returns:
            Betオブジェクトのリスト（EV降順）
        """
        if not entries or not odds:
            return []

        ev_threshold = params.get("ev_threshold", self._ev_threshold)
        stake = params.get("stake_yen", self._stake_yen)
        max_bets = params.get("max_bets_per_race", self._max_bets_per_race)
        as_of_date = params.get("as_of_date")

        race_key = self._build_race_key(race_data)

        # GY指数スコアリング
        scored = self._engine.score_race(
            race_data, entries, odds, race_key=race_key, as_of_date=as_of_date,
        )
        if not scored:
            return []

        # バリューベット抽出
        value_bets = [r for r in scored if r.get("expected_value", 0) > ev_threshold]
        if not value_bets:
            return []

        # 上位N件に絞る
        value_bets = value_bets[:max_bets]

        # bankroll上限チェック（残高不足なら打ち切り）
        result_bets: list[Bet] = []
        for r in value_bets:
            if bankroll < stake:
                break

            result_bets.append(Bet(
                race_key=race_key,
                bet_type="WIN",
                selection=r["umaban"],
                stake_yen=stake,
                est_prob=r.get("win_prob", 0.0),
                odds_at_bet=r.get("odds", 0.0),
                est_ev=r.get("expected_value", 0.0),
                factor_details=r.get("factor_scores", {}),
            ))

        return result_bets

    @staticmethod
    def _build_race_key(race_data: dict[str, Any]) -> str:
        """race_dataからrace_keyを構築する。"""
        rk = race_data.get("race_key", "")
        if rk:
            return rk
        return (
            race_data.get("Year", "")
            + race_data.get("MonthDay", "")
            + race_data.get("JyoCD", "")
            + race_data.get("Kaiji", "")
            + race_data.get("Nichiji", "")
            + race_data.get("RaceNum", "")
        )
