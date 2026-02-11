"""確率校正モジュールの単体テスト。"""

import numpy as np
import pytest

from src.scoring.calibration import (
    IsotonicCalibrator,
    PlattCalibrator,
    ProbabilityCalibrator,
)


class TestPlattCalibrator:
    """PlattCalibratorクラスのテスト。"""

    def test_predict_proba_before_fit_raises(self) -> None:
        """fit前のpredict_probaがRuntimeErrorを発生させること。"""
        calibrator = PlattCalibrator()
        with pytest.raises(RuntimeError, match="未訓練"):
            calibrator.predict_proba(100.0)

    def test_fit_and_predict(self) -> None:
        """fitしたモデルで正しく確率予測ができること。"""
        calibrator = PlattCalibrator()
        # 明確に分離可能なデータ
        scores = np.array([10.0, 20.0, 30.0, 80.0, 90.0, 100.0])
        labels = np.array([0, 0, 0, 1, 1, 1])
        calibrator.fit(scores, labels)

        # 低スコアは低確率
        low_prob = calibrator.predict_proba(15.0)
        assert 0.0 < low_prob < 0.5

        # 高スコアは高確率
        high_prob = calibrator.predict_proba(95.0)
        assert 0.5 < high_prob < 1.0

    def test_predict_proba_returns_float(self) -> None:
        """predict_probaがfloatを返すこと。"""
        calibrator = PlattCalibrator()
        scores = np.array([10.0, 90.0, 20.0, 80.0])
        labels = np.array([0, 1, 0, 1])
        calibrator.fit(scores, labels)

        result = calibrator.predict_proba(50.0)
        assert isinstance(result, float)

    def test_predict_proba_bounded(self) -> None:
        """predict_probaが0〜1の範囲に収まること。"""
        calibrator = PlattCalibrator()
        scores = np.array([10.0, 90.0, 20.0, 80.0])
        labels = np.array([0, 1, 0, 1])
        calibrator.fit(scores, labels)

        # 極端な値でも0〜1に収まる
        assert 0.0 <= calibrator.predict_proba(-1000.0) <= 1.0
        assert 0.0 <= calibrator.predict_proba(1000.0) <= 1.0

    def test_is_fitted_flag(self) -> None:
        """fit後にis_fittedフラグがTrueになること。"""
        calibrator = PlattCalibrator()
        assert calibrator._is_fitted is False
        scores = np.array([10.0, 90.0])
        labels = np.array([0, 1])
        calibrator.fit(scores, labels)
        assert calibrator._is_fitted is True


class TestIsotonicCalibrator:
    """IsotonicCalibratorクラスのテスト。"""

    def test_predict_proba_before_fit_raises(self) -> None:
        """fit前のpredict_probaがRuntimeErrorを発生させること。"""
        calibrator = IsotonicCalibrator()
        with pytest.raises(RuntimeError, match="未訓練"):
            calibrator.predict_proba(100.0)

    def test_fit_and_predict(self) -> None:
        """fitしたモデルで正しく確率予測ができること。"""
        calibrator = IsotonicCalibrator()
        scores = np.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0])
        labels = np.array([0, 0, 0, 0, 0, 1, 1, 1, 1, 1])
        calibrator.fit(scores, labels)

        # 低スコアは低確率
        low_prob = calibrator.predict_proba(20.0)
        assert low_prob <= 0.5

        # 高スコアは高確率
        high_prob = calibrator.predict_proba(90.0)
        assert high_prob >= 0.5

    def test_predict_proba_returns_float(self) -> None:
        """predict_probaがfloatを返すこと。"""
        calibrator = IsotonicCalibrator()
        scores = np.array([10.0, 20.0, 30.0, 80.0, 90.0, 100.0])
        labels = np.array([0, 0, 0, 1, 1, 1])
        calibrator.fit(scores, labels)

        result = calibrator.predict_proba(50.0)
        assert isinstance(result, float)

    def test_monotonicity(self) -> None:
        """予測確率が単調増加であること。"""
        calibrator = IsotonicCalibrator()
        scores = np.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0])
        labels = np.array([0, 0, 0, 0, 0, 1, 1, 1, 1, 1])
        calibrator.fit(scores, labels)

        probs = [calibrator.predict_proba(s) for s in [20.0, 40.0, 60.0, 80.0]]
        for i in range(len(probs) - 1):
            assert probs[i] <= probs[i + 1]

    def test_is_fitted_flag(self) -> None:
        """fit後にis_fittedフラグがTrueになること。"""
        calibrator = IsotonicCalibrator()
        assert calibrator._is_fitted is False
        scores = np.array([10.0, 90.0])
        labels = np.array([0, 1])
        calibrator.fit(scores, labels)
        assert calibrator._is_fitted is True


class TestProbabilityCalibrator:
    """ProbabilityCalibrator ABCのテスト。"""

    def test_cannot_instantiate_abstract(self) -> None:
        """抽象クラスを直接インスタンス化できないこと。"""
        with pytest.raises(TypeError):
            ProbabilityCalibrator()  # type: ignore[abstract]
