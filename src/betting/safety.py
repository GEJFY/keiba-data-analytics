"""自動投票の安全機構モジュール。

緊急停止、二重投票防止、オッズ急変検知などの
安全制御を提供する。

安全チェック項目:
    1. 連敗数閾値（デフォルト20連敗）による緊急停止
    2. 日次損失上限（デフォルト20万円）による投票停止
    3. 同一レース・馬番への二重投票防止
    4. オッズ急変検知（判断時と現在の乖離率30%超）
"""

from dataclasses import dataclass, field

from loguru import logger


@dataclass
class SafetyState:
    """安全機構の内部状態。

    Attributes:
        is_emergency_stopped: 緊急停止フラグ（手動解除が必要）
        consecutive_losses: 現在の連敗数
        daily_loss: 当日の累計損失額（円）
        executed_bets: 当日の投票済みキー集合（"race_key:selection"）
    """

    is_emergency_stopped: bool = False
    consecutive_losses: int = 0
    daily_loss: int = 0
    executed_bets: set[str] = field(default_factory=set)


class SafetyGuard:
    """自動投票の安全機構。

    全ての投票実行前にcheck_can_bet()を呼び出し、
    安全条件を満たさない場合は投票をブロックする。
    """

    def __init__(
        self,
        max_consecutive_losses: int = 20,
        max_daily_loss: int = 200_000,
        odds_deviation_threshold: float = 0.30,
    ) -> None:
        """
        Args:
            max_consecutive_losses: 緊急停止までの最大連敗数
            max_daily_loss: 日次損失上限（円）
            odds_deviation_threshold: オッズ急変判定閾値（0.30 = 30%）
        """
        self._max_consecutive_losses = max_consecutive_losses
        self._max_daily_loss = max_daily_loss
        self._odds_deviation_threshold = odds_deviation_threshold
        self._state = SafetyState()

    @property
    def is_stopped(self) -> bool:
        """緊急停止中かどうかを返す。"""
        return self._state.is_emergency_stopped

    def check_can_bet(self) -> tuple[bool, str]:
        """投票可否を判定する。

        Returns:
            (可否フラグ, 理由メッセージ) のタプル
        """
        if self._state.is_emergency_stopped:
            return False, "緊急停止中"

        if self._state.consecutive_losses >= self._max_consecutive_losses:
            self._state.is_emergency_stopped = True
            logger.error(
                f"連敗数 {self._state.consecutive_losses} >= "
                f"閾値 {self._max_consecutive_losses}: 緊急停止発動"
            )
            return False, f"連敗数超過 ({self._state.consecutive_losses}連敗)"

        if self._state.daily_loss >= self._max_daily_loss:
            logger.error(f"日次損失 {self._state.daily_loss:,}円 >= 閾値 {self._max_daily_loss:,}円: 投票停止")
            return False, f"日次損失上限超過 ({self._state.daily_loss:,}円)"

        return True, "OK"

    def check_duplicate_bet(self, race_key: str, selection: str) -> bool:
        """二重投票を検出する。

        Args:
            race_key: レースキー
            selection: 馬番または組合せ

        Returns:
            二重投票の場合True
        """
        bet_key = f"{race_key}:{selection}"
        if bet_key in self._state.executed_bets:
            logger.warning(f"二重投票検出: {bet_key}")
            return True
        return False

    def check_odds_deviation(self, odds_at_decision: float, odds_current: float) -> tuple[bool, float]:
        """オッズ急変を検知する。

        Args:
            odds_at_decision: 判断時のオッズ
            odds_current: 現在のオッズ

        Returns:
            (急変検知フラグ, 乖離率) のタプル
        """
        if odds_at_decision <= 0:
            return True, 0.0
        deviation = abs(odds_current - odds_at_decision) / odds_at_decision
        if deviation > self._odds_deviation_threshold:
            logger.warning(
                f"オッズ急変: {odds_at_decision:.1f} → {odds_current:.1f} "
                f"(乖離率 {deviation:.1%} > 閾値 {self._odds_deviation_threshold:.1%})"
            )
            return True, deviation
        return False, deviation

    def record_result(self, is_win: bool, pnl: int) -> None:
        """レース結果を記録する。

        Args:
            is_win: 的中フラグ
            pnl: 損益（円、損失はマイナス値）
        """
        if is_win:
            self._state.consecutive_losses = 0
        else:
            self._state.consecutive_losses += 1

        if pnl < 0:
            self._state.daily_loss += abs(pnl)

    def register_bet(self, race_key: str, selection: str) -> None:
        """投票を登録する（二重投票防止用）。

        Args:
            race_key: レースキー
            selection: 馬番または組合せ
        """
        self._state.executed_bets.add(f"{race_key}:{selection}")

    def reset_daily(self) -> None:
        """日次の状態をリセットする。日替わり時に呼び出す。

        注: 緊急停止状態はリセットされない（手動解除が必要）。
        """
        self._state.daily_loss = 0
        self._state.executed_bets.clear()
        if not self._state.is_emergency_stopped:
            logger.info("日次安全状態リセット完了")
