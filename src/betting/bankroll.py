"""資金管理モジュール。

卍氏の資金管理手法を参考にした投票金額決定ロジック。

投票金額決定方式:
    - EQUAL: 固定比率方式（残高 × fixed_rate）
    - EV_PROPORTIONAL: EV比例方式（EV超過分に比例）
    - QUARTER_KELLY: Kelly基準の25%（推奨）

安全機構:
    - ドローダウン閾値超過時: 投票額50%縮小
    - レースあたり上限: 残高の5%
    - 日次上限: 残高の20%
    - JRA最低投票単位: 100円に丸め
"""

from enum import Enum

from loguru import logger


class BettingMethod(Enum):
    """投票金額決定方式。"""

    EQUAL = "equal"                    # 固定比率
    EV_PROPORTIONAL = "ev_proportional"  # EV比例
    QUARTER_KELLY = "quarter_kelly"      # Kelly基準×0.25（推奨）


class BankrollManager:
    """資金管理クラス。

    Kelly基準のQuarter Kelly（f* × 0.25）をデフォルトとし、
    ドローダウン制限、日次上限、レースあたり上限を組み合わせた
    多層的なリスク管理を実装する。
    """

    def __init__(
        self,
        initial_balance: int,
        method: BettingMethod = BettingMethod.QUARTER_KELLY,
        max_daily_rate: float = 0.20,
        max_per_race_rate: float = 0.05,
        drawdown_cutoff: float = 0.30,
    ) -> None:
        """
        Args:
            initial_balance: 初期資金（円）
            method: 投票金額決定方式
            max_daily_rate: 日次投票上限率（残高比）
            max_per_race_rate: レースあたり投票上限率（残高比）
            drawdown_cutoff: ドローダウン縮小閾値（30%超で半減）
        """
        if initial_balance <= 0:
            raise ValueError(f"初期資金は正の値が必要です: {initial_balance}")

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
        """現在の残高を返す。"""
        return self._current_balance

    @property
    def current_drawdown(self) -> float:
        """現在のドローダウン率を返す（0.0〜1.0）。"""
        if self._peak_balance == 0:
            return 0.0
        return 1.0 - (self._current_balance / self._peak_balance)

    def calculate_stake(
        self,
        estimated_prob: float,
        odds: float,
        fixed_rate: float = 0.005,
    ) -> int:
        """投票金額を算出する。

        Args:
            estimated_prob: 推定勝率（0.0〜1.0）
            odds: 実際のオッズ
            fixed_rate: EQUAL方式の固定比率

        Returns:
            投票金額（円、100円単位）。投票対象外の場合は0。
        """
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
            # Kelly基準: f* = (p*b - q) / b  ここで b = odds - 1
            p = estimated_prob
            q = 1.0 - p
            b = odds - 1.0
            if b <= 0 or (p * b - q) <= 0:
                return 0
            kelly_fraction = (p * b - q) / b
            fraction = kelly_fraction * 0.25 * scale
            stake = int(self._current_balance * fraction)

        else:
            stake = 0

        # レースあたり上限制約
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
        """投票を記録し、残高を減算する。

        Args:
            stake: 投票金額（円）
        """
        self._current_balance -= stake
        self._daily_total_stake += stake
        logger.debug(f"投票記録: {stake:,}円 (残高: {self._current_balance:,}円)")

    def record_payout(self, payout: int) -> None:
        """払戻を記録し、残高を加算する。

        Args:
            payout: 払戻金額（円）
        """
        self._current_balance += payout
        self._peak_balance = max(self._peak_balance, self._current_balance)
        logger.debug(f"払戻記録: {payout:,}円 (残高: {self._current_balance:,}円)")

    def reset_daily(self) -> None:
        """日次の投票総額をリセットする。日替わり時に呼び出す。"""
        self._daily_total_stake = 0
        logger.info("日次投票総額リセット")
