"""バックテストエンジン本体。

過去データを用いた戦略検証を実行する。
ライブ実行と同一コードパスを使用し、Look-Ahead Biasを防止する。

バックテストフロー:
    1. 対象期間のレースリストを受け取る
    2. 各レースに対して戦略のrun()を実行（ライブと同一パス）
    3. ベット分だけ仮想bankrollを減算し、実績払戻を加算
    4. 日次スナップショットを記録
    5. 実績ベースでKPIメトリクスを算出
"""

from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from src.backtest.metrics import BacktestMetrics, calculate_metrics, calculate_payout
from src.strategy.base import Bet, Strategy


@dataclass
class BacktestConfig:
    """バックテスト実行設定。

    Attributes:
        date_from: 検証期間開始日（ISO 8601形式）
        date_to: 検証期間終了日（ISO 8601形式）
        initial_bankroll: 初期資金（円）
        strategy_version: 戦略バージョン（記録用）
    """

    date_from: str
    date_to: str
    initial_bankroll: int = 1_000_000
    strategy_version: str = ""
    exclude_overlapping_factors: bool = False


@dataclass
class DailySnapshot:
    """日次の資金スナップショット。

    Attributes:
        date: 日付（YYYYMMDD）
        opening_balance: 開始時残高
        total_stake: 当日投票額合計
        total_payout: 当日払戻額合計
        closing_balance: 終了時残高
        pnl: 当日損益
    """

    date: str
    opening_balance: int
    total_stake: int
    total_payout: int
    closing_balance: int
    pnl: int


@dataclass
class BacktestResult:
    """バックテスト実行結果。

    Attributes:
        config: 実行設定
        total_races: 対象レース数
        total_bets: 総ベット数
        bets: 全ベットのリスト
        metrics: KPIメトリクス
        daily_snapshots: 日次資金スナップショット
    """

    config: BacktestConfig
    total_races: int
    total_bets: int
    bets: list[Bet]
    metrics: BacktestMetrics
    daily_snapshots: list[DailySnapshot] = field(default_factory=list)


def _build_kakutei(entries: list[dict[str, Any]]) -> dict[str, int]:
    """出走馬リストから確定着順マップを構築する。"""
    kakutei: dict[str, int] = {}
    for e in entries:
        uma = str(e.get("Umaban", "")).strip()
        jyuni_str = str(e.get("KakuteiJyuni", "0")).strip()
        jyuni = int(jyuni_str) if jyuni_str.isdigit() else 0
        if uma and jyuni > 0:
            kakutei[uma] = jyuni
    return kakutei


def _build_race_key(race_data: dict[str, Any]) -> str:
    """race_dataからrace_keyを構築する。"""
    race_key = race_data.get("race_key", "")
    if race_key:
        return race_key
    return (
        race_data.get("Year", "")
        + race_data.get("MonthDay", "")
        + race_data.get("JyoCD", "")
        + race_data.get("Kaiji", "")
        + race_data.get("Nichiji", "")
        + race_data.get("RaceNum", "")
    )


class BacktestEngine:
    """バックテストエンジン。

    戦略プラグインを注入し、過去データでの検証を実行する。
    payoutsデータが渡された場合は実績ベース、なければ推定ベースで動作する。
    """

    def __init__(self, strategy: Strategy) -> None:
        """
        Args:
            strategy: 検証対象の戦略プラグイン
        """
        self._strategy = strategy

    def run(
        self,
        races: list[dict[str, Any]],
        config: BacktestConfig,
        progress_callback: Any = None,
    ) -> BacktestResult:
        """バックテストを実行する。

        各レースの形式:
            {"race_info": {...}, "entries": [...], "odds": {...}, "payouts": {...}}

        payoutsキーが存在する場合は実績ベースで結果を判定し、
        bankrollに払戻を加算する。存在しない場合は従来の推定ベース。

        Args:
            races: バックテスト対象レースのリスト
            config: バックテスト設定

        Returns:
            バックテスト結果（メトリクス + 日次スナップショット含む）
        """
        all_bets: list[Bet] = []
        bankroll = config.initial_bankroll
        race_results: dict[str, dict] = {}
        daily_data: dict[str, dict] = {}
        has_actual_results = False

        logger.info(
            f"バックテスト開始: {config.date_from}〜{config.date_to}, "
            f"初期資金={config.initial_bankroll:,}円, レース数={len(races)}"
        )

        for race_idx, race in enumerate(races):
            race_data = race.get("race_info", {})
            entries = race.get("entries", [])
            odds = race.get("odds", {})
            payouts = race.get("payouts", {})

            # 日付取得
            race_date = f"{race_data.get('Year', '')}{race_data.get('MonthDay', '')}"

            # 日次データ初期化
            if race_date and race_date not in daily_data:
                daily_data[race_date] = {
                    "opening": bankroll,
                    "stake": 0,
                    "payout": 0,
                }

            # 戦略実行パラメータ構築
            strategy_params: dict[str, Any] = {}
            if config.exclude_overlapping_factors and race_date:
                as_of = f"{race_date[:4]}-{race_date[4:6]}-{race_date[6:8]}"
                strategy_params["as_of_date"] = as_of

            # 戦略実行（ライブと同一パス）
            bets = self._strategy.run(
                race_data=race_data,
                entries=entries,
                odds=odds,
                bankroll=bankroll,
                params=strategy_params,
            )
            all_bets.extend(bets)

            # 確定着順を構築
            kakutei = _build_kakutei(entries)
            race_key = _build_race_key(race_data)

            if kakutei and payouts:
                has_actual_results = True
                race_results[race_key] = {
                    "kakutei": kakutei,
                    "payouts": payouts,
                }

            # bankroll更新: 賭金減算 + 実績払戻加算
            for bet in bets:
                bankroll -= bet.stake_yen
                if race_date:
                    daily_data[race_date]["stake"] += bet.stake_yen

                if kakutei and payouts:
                    payout = calculate_payout(
                        bet.bet_type, bet.selection,
                        bet.stake_yen, payouts, kakutei,
                    )
                    bankroll += payout
                    if race_date:
                        daily_data[race_date]["payout"] += payout

            if progress_callback:
                progress_callback(
                    race_idx + 1, len(races),
                    f"バックテスト {race_idx + 1}/{len(races)} レース"
                )

        # 日次スナップショット生成
        snapshots: list[DailySnapshot] = []
        for date in sorted(daily_data.keys()):
            d = daily_data[date]
            closing = d["opening"] + d["payout"] - d["stake"]
            snapshots.append(DailySnapshot(
                date=date,
                opening_balance=d["opening"],
                total_stake=d["stake"],
                total_payout=d["payout"],
                closing_balance=closing,
                pnl=d["payout"] - d["stake"],
            ))

        # メトリクス算出（実績データがあれば使用）
        metrics = calculate_metrics(
            all_bets,
            config.initial_bankroll,
            race_results=race_results if has_actual_results else None,
        )

        logger.info(
            f"バックテスト完了: {len(races)}レース, "
            f"{len(all_bets)}ベット, ROI={metrics.roi:.2%}"
            f"{' (実績ベース)' if has_actual_results else ' (推定ベース)'}"
        )

        return BacktestResult(
            config=config,
            total_races=len(races),
            total_bets=len(all_bets),
            bets=all_bets,
            metrics=metrics,
            daily_snapshots=snapshots,
        )
