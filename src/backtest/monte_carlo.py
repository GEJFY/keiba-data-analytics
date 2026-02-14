"""モンテカルロシミュレーション。

ベット履歴をリサンプリングし、戦略の確率的な収益分布を推定する。
技術仕様書 Section 9.2 に基づく。
"""

from dataclasses import dataclass

import numpy as np
from loguru import logger


@dataclass
class MonteCarloResult:
    """モンテカルロシミュレーション結果。"""

    n_simulations: int
    n_bets: int
    initial_bankroll: int
    # P&L分布
    pnl_mean: float
    pnl_median: float
    pnl_std: float
    pnl_5th: float  # 5パーセンタイル（悲観的）
    pnl_95th: float  # 95パーセンタイル（楽観的）
    # ROI分布
    roi_mean: float
    roi_median: float
    roi_5th: float
    roi_95th: float
    # リスク指標
    max_drawdown_mean: float
    max_drawdown_95th: float
    ruin_probability: float  # 破産確率（bankroll <= 0 になる確率）
    # 全シミュレーションの最終PnL
    all_final_pnls: list[float]
    # 全シミュレーションの最大DD
    all_max_drawdowns: list[float]


class MonteCarloSimulator:
    """モンテカルロシミュレーター。

    過去のベット結果（各ベットのPnL）をブートストラップ（復元抽出）して
    N回のシミュレーションを実行し、戦略の確率的な収益分布を推定する。
    """

    def __init__(self, seed: int | None = None) -> None:
        self._rng = np.random.default_rng(seed)

    def run(
        self,
        bet_pnls: list[float],
        n_simulations: int = 10000,
        n_bets_per_sim: int | None = None,
        initial_bankroll: int = 1_000_000,
    ) -> MonteCarloResult:
        """シミュレーションを実行する。

        Args:
            bet_pnls: 各ベットのPnL（払戻 - 賭金）のリスト
            n_simulations: シミュレーション回数
            n_bets_per_sim: 1シミュレーションあたりのベット数（Noneで元データと同数）
            initial_bankroll: 初期資金

        Returns:
            MonteCarloResult
        """
        if not bet_pnls:
            raise ValueError("ベットデータが空です")

        pnl_array = np.array(bet_pnls)
        n_bets = n_bets_per_sim or len(pnl_array)

        logger.info(
            f"モンテカルロ開始: {n_simulations}回シミュレーション, "
            f"{n_bets}ベット/回, 初期資金={initial_bankroll:,}円"
        )

        final_pnls = []
        max_drawdowns = []
        ruin_count = 0

        for _ in range(n_simulations):
            # ブートストラップ: 復元抽出
            sampled = self._rng.choice(pnl_array, size=n_bets, replace=True)

            # 累積PnL計算
            cumulative = np.cumsum(sampled)
            equity_curve = initial_bankroll + cumulative

            # 最終PnL
            final_pnl = float(cumulative[-1])
            final_pnls.append(final_pnl)

            # 最大ドローダウン
            running_max = np.maximum.accumulate(equity_curve)
            drawdown = (running_max - equity_curve) / np.maximum(running_max, 1)
            max_dd = float(np.max(drawdown))
            max_drawdowns.append(max_dd)

            # 破産チェック
            if np.any(equity_curve <= 0):
                ruin_count += 1

        pnl_arr = np.array(final_pnls)
        dd_arr = np.array(max_drawdowns)
        total_stake_est = abs(pnl_array[pnl_array < 0].sum()) if np.any(pnl_array < 0) else float(n_bets * 1000)

        result = MonteCarloResult(
            n_simulations=n_simulations,
            n_bets=n_bets,
            initial_bankroll=initial_bankroll,
            pnl_mean=float(np.mean(pnl_arr)),
            pnl_median=float(np.median(pnl_arr)),
            pnl_std=float(np.std(pnl_arr)),
            pnl_5th=float(np.percentile(pnl_arr, 5)),
            pnl_95th=float(np.percentile(pnl_arr, 95)),
            roi_mean=float(np.mean(pnl_arr / max(total_stake_est, 1))),
            roi_median=float(np.median(pnl_arr / max(total_stake_est, 1))),
            roi_5th=float(np.percentile(pnl_arr / max(total_stake_est, 1), 5)),
            roi_95th=float(np.percentile(pnl_arr / max(total_stake_est, 1), 95)),
            max_drawdown_mean=float(np.mean(dd_arr)),
            max_drawdown_95th=float(np.percentile(dd_arr, 95)),
            ruin_probability=ruin_count / n_simulations,
            all_final_pnls=final_pnls,
            all_max_drawdowns=max_drawdowns,
        )

        logger.info(
            f"モンテカルロ完了: PnL平均={result.pnl_mean:+,.0f}円, "
            f"PnL中央値={result.pnl_median:+,.0f}円, "
            f"破産確率={result.ruin_probability:.2%}"
        )

        return result
