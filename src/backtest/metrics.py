"""バックテストKPI計算モジュール。

仕様書 9.3節で定義されたKPI指標を算出する。
実績データ（KakuteiJyuni + NL_HR_PAY）が利用可能な場合は実績ベース、
利用不可の場合は推定確率ベースでKPIを算出する。
"""

import math
from dataclasses import dataclass
from typing import Any

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
    sortino_ratio: float = 0.0
    var_95: float = 0.0  # 95% VaR（1ベットあたり最大損失見込み額）
    max_consecutive_wins: int = 0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    payoff_ratio: float = 0.0  # 平均利益 / 平均損失
    edge: float = 0.0  # 1ベットあたり期待利益


def calculate_payout(
    bet_type: str,
    selection: str,
    stake: int,
    payouts: dict[str, Any],
    kakutei: dict[str, int],
) -> int:
    """払戻金額を計算する。

    ResultCollector._calculate_payout() と同一ロジック。
    バックテストでの的中判定に使用する。

    Args:
        bet_type: 券種（WIN/PLACE等）
        selection: 馬番
        stake: 投票額
        payouts: 払戻データ（provider.get_payouts()の戻り値）
        kakutei: 確定着順マップ {馬番: 着順}

    Returns:
        払戻金額（円）。不的中の場合は0。
    """
    jyuni = kakutei.get(selection, 0)

    if bet_type == "WIN":
        if jyuni != 1:
            return 0
        for pay in payouts.get("tansyo", []):
            if isinstance(pay, dict) and pay.get("selection") == selection:
                return int(pay.get("pay", 0)) * (stake // 100)
        return 0

    elif bet_type == "PLACE":
        if not (1 <= jyuni <= 3):
            return 0
        for pay in payouts.get("fukusyo", []):
            if isinstance(pay, dict) and pay.get("selection") == selection:
                return int(pay.get("pay", 0)) * (stake // 100)
        return 0

    return 0


def _resolve_actual_results(
    bets: list[Bet],
    race_results: dict[str, dict],
) -> list[dict]:
    """実際のレース結果からベット結果を判定する。

    Args:
        bets: ベットリスト
        race_results: {race_key: {"kakutei": {馬番: 着順}, "payouts": {...}}}

    Returns:
        [{"stake", "payout", "pnl", "is_win"}, ...]
    """
    results = []
    for bet in bets:
        result_data = race_results.get(bet.race_key, {})
        kakutei = result_data.get("kakutei", {})
        payouts = result_data.get("payouts", {})

        payout = calculate_payout(
            bet.bet_type, bet.selection, bet.stake_yen, payouts, kakutei,
        )
        pnl = payout - bet.stake_yen
        is_win = payout > 0

        results.append({
            "stake": bet.stake_yen,
            "payout": payout,
            "pnl": pnl,
            "is_win": is_win,
        })
    return results


def _simulate_results(bets: list[Bet]) -> list[dict]:
    """ベットの推定確率とオッズから期待値ベースの結果を推定する。

    バックテスト時に実際の着順結果が利用可能な場合はそちらを使用すべきだが、
    結果データが無い場合は推定確率に基づく期待値推定を行う。

    Returns:
        [{"stake", "payout", "pnl", "is_win"}, ...]
    """
    results = []
    for bet in bets:
        prob = bet.est_prob
        odds = bet.odds_at_bet

        # 期待値ベースの推定払戻:
        # 推定勝率 × オッズ × 賭金 = 期待払戻額
        expected_payout = int(prob * odds * bet.stake_yen)
        pnl = expected_payout - bet.stake_yen
        is_win = expected_payout > bet.stake_yen

        results.append({
            "stake": bet.stake_yen,
            "payout": expected_payout,
            "pnl": pnl,
            "is_win": is_win,
        })
    return results


def calculate_metrics(
    bets: list[Bet],
    initial_bankroll: int,
    race_results: dict[str, dict] | None = None,
) -> BacktestMetrics:
    """ベットリストからKPI指標を算出する。

    Args:
        bets: ベットリスト
        initial_bankroll: 初期資金
        race_results: 実績データ {race_key: {"kakutei": {...}, "payouts": {...}}}。
                      指定時は実績ベース、Noneの場合は推定確率ベース。

    Returns:
        算出されたKPI指標
    """
    if not bets:
        return BacktestMetrics(
            total_stake=0, total_payout=0, pnl=0, roi=0.0,
            win_rate=0.0, recovery_rate=0.0, max_drawdown=0.0,
            max_consecutive_losses=0, sharpe_ratio=0.0,
            profit_factor=0.0, monthly_win_rate=0.0, calmar_ratio=0.0,
        )

    if race_results:
        results = _resolve_actual_results(bets, race_results)
    else:
        results = _simulate_results(bets)

    total_stake = sum(r["stake"] for r in results)
    total_payout = sum(r["payout"] for r in results)
    pnl = total_payout - total_stake
    roi = pnl / total_stake if total_stake > 0 else 0.0
    recovery_rate = total_payout / total_stake if total_stake > 0 else 0.0

    # 勝率
    wins = sum(1 for r in results if r["is_win"])
    win_rate = wins / len(results) if results else 0.0

    # 最大ドローダウン（累積P&Lベース）
    cumulative = 0
    peak = 0
    max_dd = 0.0
    for r in results:
        cumulative += r["pnl"]
        if cumulative > peak:
            peak = cumulative
        dd = (peak - cumulative) / max(initial_bankroll, 1)
        if dd > max_dd:
            max_dd = dd

    # 最大連敗数
    max_consec = 0
    current_consec = 0
    for r in results:
        if not r["is_win"]:
            current_consec += 1
            max_consec = max(max_consec, current_consec)
        else:
            current_consec = 0

    # シャープレシオ（ベット単位のリターン標準偏差）
    returns = [r["pnl"] / max(r["stake"], 1) for r in results]
    avg_return = sum(returns) / len(returns) if returns else 0.0
    if len(returns) > 1:
        variance = sum((r - avg_return) ** 2 for r in returns) / (len(returns) - 1)
        std_return = math.sqrt(variance)
        sharpe_ratio = avg_return / std_return if std_return > 0 else 0.0
    else:
        sharpe_ratio = 0.0

    # プロフィットファクター（総利益 / 総損失）
    total_profit = sum(r["pnl"] for r in results if r["pnl"] > 0)
    total_loss = abs(sum(r["pnl"] for r in results if r["pnl"] < 0))
    profit_factor = total_profit / total_loss if total_loss > 0 else (999.9 if total_profit > 0 else 0.0)

    # カルマーレシオ（ROI / 最大DD）
    calmar_ratio = roi / max_dd if max_dd > 0 else 0.0

    # --- 追加リスク指標 ---

    # ソルティノレシオ（下方偏差のみ使用 — 損失リスクのみ考慮）
    downside_returns = [r for r in returns if r < 0]
    if len(downside_returns) > 1:
        downside_var = sum(r ** 2 for r in downside_returns) / len(downside_returns)
        downside_std = math.sqrt(downside_var)
        sortino_ratio = avg_return / downside_std if downside_std > 0 else 0.0
    else:
        sortino_ratio = 0.0

    # 95% VaR（損失の95パーセンタイル）
    pnl_list = sorted([r["pnl"] for r in results])
    if len(pnl_list) >= 20:
        idx_5pct = max(0, int(len(pnl_list) * 0.05))
        var_95 = float(abs(pnl_list[idx_5pct]))
    else:
        var_95 = float(abs(min(pnl_list))) if pnl_list else 0.0

    # 最大連勝数
    max_consec_wins = 0
    current_consec_wins = 0
    for r in results:
        if r["is_win"]:
            current_consec_wins += 1
            max_consec_wins = max(max_consec_wins, current_consec_wins)
        else:
            current_consec_wins = 0

    # 平均利益・平均損失・ペイオフレシオ
    win_pnls = [r["pnl"] for r in results if r["pnl"] > 0]
    loss_pnls = [r["pnl"] for r in results if r["pnl"] < 0]
    avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
    avg_loss = abs(sum(loss_pnls) / len(loss_pnls)) if loss_pnls else 0.0
    payoff_ratio = avg_win / avg_loss if avg_loss > 0 else 0.0

    # エッジ（1ベットあたり期待利益）
    edge = pnl / len(results) if results else 0.0

    return BacktestMetrics(
        total_stake=total_stake,
        total_payout=total_payout,
        pnl=pnl,
        roi=roi,
        win_rate=win_rate,
        recovery_rate=recovery_rate,
        max_drawdown=max_dd,
        max_consecutive_losses=max_consec,
        sharpe_ratio=sharpe_ratio,
        profit_factor=profit_factor,
        monthly_win_rate=win_rate,
        calmar_ratio=calmar_ratio,
        sortino_ratio=sortino_ratio,
        var_95=var_95,
        max_consecutive_wins=max_consec_wins,
        avg_win=avg_win,
        avg_loss=avg_loss,
        payoff_ratio=payoff_ratio,
        edge=edge,
    )
