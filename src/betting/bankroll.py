"""資金管理モジュール。

卍氏の資金管理手法を参考にした投票金額決定ロジック。
"""

from enum import Enum

from loguru import logger


class BettingMethod(Enum):
    """投票金額決定方式。"""

    EQUAL = "equal"
    EV_PROPORTIONAL = "ev_proportional"
    QUARTER_KELLY = "quarter_kelly"


class BankrollManager:
    """資金管理クラス。"""

    def __init__(
        self,
        initial_balance: int,
        method: BettingMethod = BettingMethod.QUARTER_KELLY,
        max_daily_rate: float = 0.20,
        max_per_race_rate: float = 0.05,
        drawdown_cutoff: float = 0.30,
    ) -> None:
        self._initial_balance = initial_balance
        self._current_balance = initial_balance
        self._peak_balance = initial_balance
        self._method = method
        self._max_daily_rate = max_daily_rate
        self._max_per_race_rate = max_per_race_rate
        self._drawdown_cutoff = drawdown_cutoff
        self._daily_total_stake = 0

    @property
    def current_balance(self) -> int:
        return self._current_balance

    @property
    def current_drawdown(self) -> float:
        """現在のドローダウン率を返す。"""
        if self._peak_balance == 0:
            return 0.0
        return 1.0 - (self._current_balance / self._peak_balance)

    def calculate_stake(
        self,
        estimated_prob: float,
        odds: float,
        fixed_rate: float = 0.005,
    ) -> int:
        """投票金額を算出する。"""
        # ドローダウン制限チェック
        scale = 1.0
        if self.current_drawdown > self._drawdown_cutoff:
            scale = 0.5
            logger.warning(
                f"ドローダウン {self.current_drawdown:.1%} > "
                f"閾値 {self._drawdown_cutoff:.1%}: 投票額50%縮小"
            )

        if self._method == BettingMethod.EQUAL:
            stake = int(self._current_balance * fixed_rate * scale)

        elif self._method == BettingMethod.EV_PROPORTIONAL:
            ev = estimated_prob * odds
            if ev <= 1.0:
                return 0
            base = self._current_balance * fixed_rate
            stake = int(base * (ev - 1.0) * 10 * scale)

        elif self._method == BettingMethod.QUARTER_KELLY:
            # Kelly基準: f* = (p*b - q) / b
            p = estimated_prob
            q = 1.0 - p
            b = odds - 1.0
            if b <= 0 or (p * b - q) <= 0:
                return 0
            kelly_fraction = (p * b - q) / b
            # Quarter Kelly
            fraction = kelly_fraction * 0.25 * scale
            stake = int(self._current_balance * fraction)

        else:
            stake = 0

        # 上限制約
        max_stake = int(self._current_balance * self._max_per_race_rate)
        stake = min(stake, max_stake)

        # 日次上限チェック
        daily_max = int(self._current_balance * self._max_daily_rate)
        remaining_daily = daily_max - self._daily_total_stake
        stake = min(stake, max(0, remaining_daily))

        # 100円単位に丸め（JRA最低投票単位）
        stake = (stake // 100) * 100

        return max(0, stake)

    def record_bet(self, stake: int) -> None:
        """投票を記録する。"""
        self._current_balance -= stake
        self._daily_total_stake += stake

    def record_payout(self, payout: int) -> None:
        """払戻を記録する。"""
        self._current_balance += payout
        self._peak_balance = max(self._peak_balance, self._current_balance)

    def reset_daily(self) -> None:
        """日次の投票総額をリセットする。"""
        self._daily_total_stake = 0
