"""自動投票の安全機構モジュール。

緊急停止、二重投票防止、オッズ急変検知などの
安全制御を提供する。
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone

from loguru import logger


@dataclass
class SafetyState:
    """安全機構の状態。"""

    is_emergency_stopped: bool = False
    consecutive_losses: int = 0
    daily_loss: int = 0
    executed_bets: set[str] = field(default_factory=set)  # "race_key:umaban" の集合


class SafetyGuard:
    """自動投票の安全機構。"""

    def __init__(
        self,
        max_consecutive_losses: int = 20,
        max_daily_loss: int = 200_000,
        odds_deviation_threshold: float = 0.30,
    ) -> None:
        self._max_consecutive_losses = max_consecutive_losses
        self._max_daily_loss = max_daily_loss
        self._odds_deviation_threshold = odds_deviation_threshold
        self._state = SafetyState()

    @property
    def is_stopped(self) -> bool:
        return self._state.is_emergency_stopped

    def check_can_bet(self) -> tuple[bool, str]:
        """投票可否を判定する。"""
        if self._state.is_emergency_stopped:
            return False, "緊急停止中"

        if self._state.consecutive_losses >= self._max_consecutive_losses:
            self._state.is_emergency_stopped = True
            logger.error(
                f"連敗数 {self._state.consecutive_losses} >= "
                f"閾値 {self._max_consecutive_losses}: 緊急停止"
            )
            return False, f"連敗数超過 ({self._state.consecutive_losses}連敗)"

        if self._state.daily_loss >= self._max_daily_loss:
            logger.error(f"日次損失 {self._state.daily_loss:,}円 >= 閾値 {self._max_daily_loss:,}円")
            return False, f"日次損失上限超過 ({self._state.daily_loss:,}円)"

        return True, "OK"

    def check_duplicate_bet(self, race_key: str, selection: str) -> bool:
        """二重投票を検出する。"""
        bet_key = f"{race_key}:{selection}"
        if bet_key in self._state.executed_bets:
            logger.warning(f"二重投票検出: {bet_key}")
            return True
        return False

    def check_odds_deviation(self, odds_at_decision: float, odds_current: float) -> tuple[bool, float]:
        """オッズ急変を検知する。"""
        if odds_at_decision <= 0:
            return True, 0.0
        deviation = abs(odds_current - odds_at_decision) / odds_at_decision
        if deviation > self._odds_deviation_threshold:
            logger.warning(
                f"オッズ急変: {odds_at_decision:.1f} → {odds_current:.1f} "
                f"(乖離率 {deviation:.1%})"
            )
            return True, deviation
        return False, deviation

    def record_result(self, is_win: bool, pnl: int) -> None:
        """結果を記録する。"""
        if is_win:
            self._state.consecutive_losses = 0
        else:
            self._state.consecutive_losses += 1

        if pnl < 0:
            self._state.daily_loss += abs(pnl)

    def register_bet(self, race_key: str, selection: str) -> None:
        """投票を登録する（二重投票防止用）。"""
        self._state.executed_bets.add(f"{race_key}:{selection}")

    def reset_daily(self) -> None:
        """日次の状態をリセットする。"""
        self._state.daily_loss = 0
        self._state.executed_bets.clear()
        if not self._state.is_emergency_stopped:
            logger.info("日次安全状態リセット完了")
