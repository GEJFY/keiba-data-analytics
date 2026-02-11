"""確率校正モジュール。

スコアから確率への変換精度を保証するための
キャリブレーション機能を提供する。
"""

from abc import ABC, abstractmethod

import numpy as np
from numpy.typing import NDArray


class ProbabilityCalibrator(ABC):
    """確率校正の基底クラス。"""

    @abstractmethod
    def fit(self, scores: NDArray[np.float64], labels: NDArray[np.int64]) -> None:
        """校正モデルを訓練する。"""
        ...

    @abstractmethod
    def predict_proba(self, score: float) -> float:
        """スコアから確率を予測する。"""
        ...


class PlattCalibrator(ProbabilityCalibrator):
    """Plattスケーリングによる確率校正。

    ロジスティック回帰でスコアを確率に変換する。
    """

    def __init__(self) -> None:
        self._a: float = 0.0
        self._b: float = 0.0
        self._is_fitted: bool = False

    def fit(self, scores: NDArray[np.float64], labels: NDArray[np.int64]) -> None:
        """スコアとラベルからPlattスケーリングのパラメータを学習する。"""
        from sklearn.linear_model import LogisticRegression

        model = LogisticRegression()
        model.fit(scores.reshape(-1, 1), labels)
        self._a = float(model.coef_[0][0])
        self._b = float(model.intercept_[0])
        self._is_fitted = True

    def predict_proba(self, score: float) -> float:
        """スコアを確率に変換する。"""
        if not self._is_fitted:
            raise RuntimeError("校正モデルが未訓練です。fit()を先に呼び出してください。")
        logit = self._a * score + self._b
        return float(1.0 / (1.0 + np.exp(-logit)))


class IsotonicCalibrator(ProbabilityCalibrator):
    """Isotonic Regressionによる確率校正。

    単調性を保証するノンパラメトリック校正。
    """

    def __init__(self) -> None:
        from sklearn.isotonic import IsotonicRegression

        self._model = IsotonicRegression(out_of_bounds="clip")
        self._is_fitted: bool = False

    def fit(self, scores: NDArray[np.float64], labels: NDArray[np.int64]) -> None:
        """スコアとラベルからIsotonic Regressionモデルを学習する。"""
        self._model.fit(scores, labels)
        self._is_fitted = True

    def predict_proba(self, score: float) -> float:
        """スコアを確率に変換する。"""
        if not self._is_fitted:
            raise RuntimeError("校正モデルが未訓練です。fit()を先に呼び出してください。")
        result = self._model.predict([score])
        return float(result[0])
