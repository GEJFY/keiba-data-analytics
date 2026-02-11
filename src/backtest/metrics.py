"""バックテストKPI計算モジュール。

仕様書 9.3節で定義されたKPI指標を算出する。
"""

from dataclasses import dataclass

from src.strategy.base import Bet


@dataclass
class BacktestMetrics:
    """バックテストのKPI指標。"""

    total_stake: int
    total_payout: int
    pnl: int
    roi: float
    win_rate: float
    recovery_rate: float  # 回収率
    max_drawdown: float
    max_consecutive_losses: int
    sharpe_ratio: float
    profit_factor: float
    monthly_win_rate: float
    calmar_ratio: float


def calculate_metrics(bets: list[Bet], initial_bankroll: int) -> BacktestMetrics:
    """ベットリストからKPI指標を算出する。"""
    if not bets:
        return BacktestMetrics(
            total_stake=0,
            total_payout=0,
            pnl=0,
            roi=0.0,
            win_rate=0.0,
            recovery_rate=0.0,
            max_drawdown=0.0,
            max_consecutive_losses=0,
            sharpe_ratio=0.0,
            profit_factor=0.0,
            monthly_win_rate=0.0,
            calmar_ratio=0.0,
        )

    total_stake = sum(bet.stake_yen for bet in bets)
    # TODO: 実際の払戻額は結果データから取得する必要がある
    total_payout = 0
    pnl = total_payout - total_stake
    roi = pnl / total_stake if total_stake > 0 else 0.0
    recovery_rate = total_payout / total_stake if total_stake > 0 else 0.0

    return BacktestMetrics(
        total_stake=total_stake,
        total_payout=total_payout,
        pnl=pnl,
        roi=roi,
        win_rate=0.0,  # TODO: 結果データから算出
        recovery_rate=recovery_rate,
        max_drawdown=0.0,  # TODO: 累積P&Lから算出
        max_consecutive_losses=0,  # TODO: 結果データから算出
        sharpe_ratio=0.0,  # TODO: 日次リターンから算出
        profit_factor=0.0,  # TODO: 総利益/総損失から算出
        monthly_win_rate=0.0,  # TODO: 月次データから算出
        calmar_ratio=0.0,  # TODO: 年間リターン/最大DDから算出
    )
