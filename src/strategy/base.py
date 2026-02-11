"""戦略プラグインの基底クラス。

全ての投資戦略はこのインターフェースを実装する。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class Bet:
    """投票指示データクラス。"""

    race_key: str
    bet_type: str  # WIN/PLACE/EXACTA/TRIFECTA等
    selection: str  # 馬番または組合せ
    stake_yen: int
    est_prob: float
    odds_at_bet: float
    est_ev: float
    factor_details: dict[str, float]  # 各ファクターの加減点内訳


class Strategy(ABC):
    """戦略プラグインの基底クラス。"""

    @abstractmethod
    def name(self) -> str:
        """戦略名を返す。"""
        ...

    @abstractmethod
    def version(self) -> str:
        """戦略バージョンを返す（例: "1.0.0"）。"""
        ...

    @abstractmethod
    def run(
        self,
        race_data: dict[str, Any],
        entries: list[dict[str, Any]],
        odds: dict[str, float],
        bankroll: int,
        params: dict[str, Any],
    ) -> list[Bet]:
        """
        戦略を実行し、投票指示リストを返す。

        Args:
            race_data: レース情報（NL_RAのレコード）
            entries: 出走馬リスト（NL_SEのレコード群）
            odds: オッズ情報（馬番→オッズのdict）
            bankroll: 現在の残高
            params: 戦略パラメータ

        Returns:
            投票指示のリスト。投票対象なしの場合は空リスト。
        """
        ...
