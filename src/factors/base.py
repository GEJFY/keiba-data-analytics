"""ファクター基底クラス。

全てのファクタールールはこの基底クラスを継承する。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class FactorResult:
    """ファクター評価結果。"""

    rule_id: int
    rule_name: str
    score: float  # +1, 0, -1 等
    weight: float
    weighted_score: float  # score × weight
    detail: str  # 判定理由の説明


class BaseFactor(ABC):
    """ファクタールールの基底クラス。"""

    @abstractmethod
    def name(self) -> str:
        """ファクター名を返す。"""
        ...

    @abstractmethod
    def category(self) -> str:
        """カテゴリーを返す。"""
        ...

    @abstractmethod
    def evaluate(
        self,
        horse: dict[str, Any],
        race: dict[str, Any],
        all_entries: list[dict[str, Any]],
    ) -> FactorResult:
        """
        馬に対してファクターを評価する。

        Args:
            horse: 対象馬のデータ（NL_SEのレコード）
            race: レース情報（NL_RAのレコード）
            all_entries: 同レース全出走馬データ

        Returns:
            ファクター評価結果
        """
        ...
