"""バックテストエンジン本体。

過去データを用いた戦略検証を実行する。
ライブ実行と同一コードパスを使用し、Look-Ahead Biasを防止する。

バックテストフロー:
    1. 対象期間のレースリストを受け取る
    2. 各レースに対して戦略のrun()を実行（ライブと同一パス）
    3. ベット分だけ仮想bankrollを減算
    4. 全ベットからKPIメトリクスを算出
"""

from dataclasses import dataclass
from typing import Any

from loguru import logger

from src.backtest.metrics import BacktestMetrics, calculate_metrics
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


@dataclass
class BacktestResult:
    """バックテスト実行結果。

    Attributes:
        config: 実行設定
        total_races: 対象レース数
        total_bets: 総ベット数
        bets: 全ベットのリスト
        metrics: KPIメトリクス
    """

    config: BacktestConfig
    total_races: int
    total_bets: int
    bets: list[Bet]
    metrics: BacktestMetrics


class BacktestEngine:
    """バックテストエンジン。

    戦略プラグインを注入し、過去データでの検証を実行する。
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
    ) -> BacktestResult:
        """バックテストを実行する。

        各レースの形式:
            {"race_info": {...}, "entries": [...], "odds": {...}}

        Args:
            races: バックテスト対象レースのリスト
            config: バックテスト設定

        Returns:
            バックテスト結果（メトリクス含む）
        """
        all_bets: list[Bet] = []
        bankroll = config.initial_bankroll

        logger.info(
            f"バックテスト開始: {config.date_from}〜{config.date_to}, "
            f"初期資金={config.initial_bankroll:,}円, レース数={len(races)}"
        )

        for race in races:
            race_data = race.get("race_info", {})
            entries = race.get("entries", [])
            odds = race.get("odds", {})

            bets = self._strategy.run(
                race_data=race_data,
                entries=entries,
                odds=odds,
                bankroll=bankroll,
                params={},
            )
            all_bets.extend(bets)

            # 資金更新（バックテスト用の擬似処理）
            for bet in bets:
                bankroll -= bet.stake_yen

        metrics = calculate_metrics(all_bets, config.initial_bankroll)

        logger.info(
            f"バックテスト完了: {len(races)}レース, "
            f"{len(all_bets)}ベット, ROI={metrics.roi:.2%}"
        )

        return BacktestResult(
            config=config,
            total_races=len(races),
            total_bets=len(all_bets),
            bets=all_bets,
            metrics=metrics,
        )
