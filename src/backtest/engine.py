"""バックテストエンジン本体。

過去データを用いた戦略検証を実行する。
ライブ実行と同一コードパスを使用し、Look-Ahead Biasを防止する。
"""

from dataclasses import dataclass
from typing import Any

from loguru import logger

from src.backtest.metrics import BacktestMetrics, calculate_metrics
from src.strategy.base import Bet, Strategy


@dataclass
class BacktestConfig:
    """バックテスト実行設定。"""

    date_from: str
    date_to: str
    initial_bankroll: int = 1_000_000
    strategy_version: str = ""


@dataclass
class BacktestResult:
    """バックテスト実行結果。"""

    config: BacktestConfig
    total_races: int
    total_bets: int
    bets: list[Bet]
    metrics: BacktestMetrics


class BacktestEngine:
    """バックテストエンジン。"""

    def __init__(self, strategy: Strategy) -> None:
        self._strategy = strategy

    def run(
        self,
        races: list[dict[str, Any]],
        config: BacktestConfig,
    ) -> BacktestResult:
        """
        バックテストを実行する。

        Args:
            races: バックテスト対象レースのリスト
            config: バックテスト設定

        Returns:
            バックテスト結果
        """
        all_bets: list[Bet] = []
        bankroll = config.initial_bankroll

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
                # 結果の反映はメトリクス計算時に行う

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
