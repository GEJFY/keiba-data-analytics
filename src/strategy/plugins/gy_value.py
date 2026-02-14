"""GYバリュー戦略プラグイン。

GY指数方式に基づくバリューベット戦略の具体実装。
ScoringEngineでスコアを計算し、EV > 閾値の馬に対して
Quarter Kelly方式で投票額を決定する。

フロー:
    1. ScoringEngine.score_race() でGY指数・EV算出
    2. EV閾値を超える馬を抽出（バリューベット）
    3. BankrollManager.calculate_stake() で投票額決定
    4. Betオブジェクトとして返却
"""

from typing import Any

from loguru import logger

from src.betting.bankroll import BankrollManager, BettingMethod
from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider
from src.scoring.engine import ScoringEngine
from src.strategy.base import Bet, Strategy


class GYValueStrategy(Strategy):
    """GY指数バリューベット戦略。

    APPROVEDファクタールールでスコアリングし、
    EV閾値を超える馬に単勝ベットを行う。

    Args:
        ext_db: 拡張DB（factor_rules格納先）
        jvlink_db: JVLink DB（前走データ取得用、Noneで前走なし）
        ev_threshold: バリューベット判定閾値（デフォルト1.05）
        method: 投票金額決定方式（デフォルトQuarter Kelly）
        max_per_race_rate: レースあたり投票上限率
    """

    def __init__(
        self,
        ext_db: DatabaseManager,
        jvlink_db: DatabaseManager | None = None,
        ev_threshold: float = 1.05,
        method: BettingMethod = BettingMethod.QUARTER_KELLY,
        max_per_race_rate: float = 0.05,
    ) -> None:
        self._ext_db = ext_db
        self._ev_threshold = ev_threshold
        self._method = method
        self._max_per_race_rate = max_per_race_rate
        provider = JVLinkDataProvider(jvlink_db) if jvlink_db else None
        self._engine = ScoringEngine(
            ext_db, ev_threshold=ev_threshold, jvlink_provider=provider,
        )

    def name(self) -> str:
        return "GY_VALUE"

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
        """GYバリュー戦略を実行する。

        Args:
            race_data: レース情報
            entries: 出走馬リスト
            odds: {馬番: オッズ} のdict
            bankroll: 現在の残高（円）
            params: 追加パラメータ
                - ev_threshold: EV閾値の上書き
                - max_bets_per_race: 1レースあたり最大ベット数

        Returns:
            Betオブジェクトのリスト（EV降順）
        """
        if not entries or not odds:
            return []

        ev_threshold = params.get("ev_threshold", self._ev_threshold)
        max_bets = params.get("max_bets_per_race", 3)
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

        # 投票額計算
        bm = BankrollManager(
            initial_balance=bankroll,
            method=self._method,
            max_per_race_rate=self._max_per_race_rate,
        )

        bets: list[Bet] = []

        for vb in value_bets:
            stake = bm.calculate_stake(
                estimated_prob=vb["estimated_prob"],
                odds=vb["actual_odds"],
            )
            if stake <= 0:
                continue

            bets.append(Bet(
                race_key=race_key,
                bet_type="WIN",
                selection=str(vb["umaban"]),
                stake_yen=stake,
                est_prob=vb["estimated_prob"],
                odds_at_bet=vb["actual_odds"],
                est_ev=vb["expected_value"],
                factor_details=vb.get("factor_details", {}),
            ))
            bm.record_bet(stake)

        if bets:
            logger.info(
                f"GY_VALUE: {race_key} → {len(bets)}ベット "
                f"(合計 {sum(b.stake_yen for b in bets):,}円)"
            )

        return bets

    @staticmethod
    def _build_race_key(race_data: dict[str, Any]) -> str:
        """レースデータからrace_keyを組み立てる。"""
        parts = [
            str(race_data.get("Year", "")),
            str(race_data.get("MonthDay", "")),
            str(race_data.get("JyoCD", "")),
            str(race_data.get("Kaiji", "")),
            str(race_data.get("Nichiji", "")),
            str(race_data.get("RaceNum", "")),
        ]
        return "".join(parts)
