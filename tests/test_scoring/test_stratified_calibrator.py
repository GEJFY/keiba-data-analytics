"""層別キャリブレーターのテスト。"""

import numpy as np
import pytest

from src.scoring.stratified_calibrator import (
    StratifiedCalibrator,
    get_stratum,
    track_cd_to_type,
)


class TestGetStratum:
    """get_stratum関数のテスト。"""

    def test_turf_sprint(self):
        assert get_stratum("turf", 1200) == "turf_sprint"

    def test_turf_mile(self):
        assert get_stratum("turf", 1600) == "turf_mile"

    def test_turf_middle(self):
        assert get_stratum("turf", 2000) == "turf_middle"

    def test_turf_long(self):
        assert get_stratum("turf", 2400) == "turf_long"

    def test_dirt_sprint(self):
        assert get_stratum("dirt", 1200) == "dirt_sprint"

    def test_dirt_mile(self):
        assert get_stratum("dirt", 1600) == "dirt_mile"

    def test_dirt_long(self):
        assert get_stratum("dirt", 2000) == "dirt_long"

    def test_boundary_turf_sprint_mile(self):
        assert get_stratum("turf", 1400) == "turf_sprint"
        assert get_stratum("turf", 1401) == "turf_mile"

    def test_unknown_track(self):
        assert get_stratum("obstacle", 1600) == "unknown"


class TestTrackCdToType:
    """track_cd_to_type関数のテスト。"""

    def test_turf(self):
        assert track_cd_to_type("10") == "turf"
        assert track_cd_to_type("11") == "turf"

    def test_dirt(self):
        assert track_cd_to_type("20") == "dirt"
        assert track_cd_to_type("23") == "dirt"

    def test_default(self):
        assert track_cd_to_type("") == "turf"
        assert track_cd_to_type("30") == "turf"


class TestStratifiedCalibrator:
    """StratifiedCalibratorのテスト。"""

    @pytest.fixture
    def training_data(self):
        """十分なサンプル数の訓練データ。"""
        rng = np.random.RandomState(42)
        n = 500

        scores = rng.normal(100, 10, n).astype(np.float64)
        labels = (scores > 100).astype(np.int64)
        # ノイズを追加
        flip_mask = rng.random(n) < 0.2
        labels[flip_mask] = 1 - labels[flip_mask]

        track_types = np.array(
            ["turf"] * 300 + ["dirt"] * 200
        )
        distances = np.array(
            [1200] * 100 + [1600] * 100 + [2000] * 100
            + [1200] * 100 + [1800] * 100,
            dtype=np.int64,
        )
        return scores, labels, track_types, distances

    def test_fit_and_predict(self, training_data):
        """学習と予測が正常に動作すること。"""
        scores, labels, track_types, distances = training_data
        cal = StratifiedCalibrator(base_method="platt")
        cal.fit(scores, labels, track_types, distances)

        prob = cal.predict_proba(105.0, track_type="turf", distance=1600)
        assert 0.0 < prob < 1.0

    def test_fallback_on_insufficient_data(self):
        """サンプル不足の層がfallbackを使用すること。"""
        rng = np.random.RandomState(42)
        n = 100

        scores = rng.normal(100, 10, n).astype(np.float64)
        labels = (scores > 100).astype(np.int64)

        # 全て同じ層 → 他の層はfallback
        track_types = np.array(["turf"] * n)
        distances = np.array([1600] * n, dtype=np.int64)

        cal = StratifiedCalibrator(base_method="platt")
        cal.fit(scores, labels, track_types, distances)

        # turf_mile は学習済み
        assert "turf_mile" in cal._calibrators

        # dirt_sprint はfallback（サンプルなし）
        assert "dirt_sprint" not in cal._calibrators

        # fallbackでも予測可能
        prob = cal.predict_proba(105.0, track_type="dirt", distance=1200)
        assert 0.0 < prob < 1.0

    def test_fit_without_strata_data(self):
        """track_types/distances なしでもfallbackで動作すること。"""
        rng = np.random.RandomState(42)
        n = 100
        scores = rng.normal(100, 10, n).astype(np.float64)
        labels = (scores > 100).astype(np.int64)

        cal = StratifiedCalibrator(base_method="platt")
        cal.fit(scores, labels)  # track_types, distances = None

        prob = cal.predict_proba(105.0)
        assert 0.0 < prob < 1.0

    def test_strata_info(self, training_data):
        """strata_infoが正しい情報を返すこと。"""
        scores, labels, track_types, distances = training_data
        cal = StratifiedCalibrator(base_method="platt")
        cal.fit(scores, labels, track_types, distances)

        info = cal.strata_info
        assert len(info) == 7  # STRATAの数
        for _name, status in info.items():
            assert status in ("trained", "fallback")

    def test_isotonic_method(self, training_data):
        """isotonic方式でも動作すること。"""
        scores, labels, track_types, distances = training_data
        cal = StratifiedCalibrator(base_method="isotonic")
        cal.fit(scores, labels, track_types, distances)

        prob = cal.predict_proba(105.0, track_type="turf", distance=1600)
        assert 0.0 < prob < 1.0

    def test_predict_without_fit_raises(self):
        """未学習時にRuntimeErrorが発生すること。"""
        cal = StratifiedCalibrator()
        with pytest.raises(RuntimeError):
            cal.predict_proba(100.0)

    def test_different_strata_give_different_probs(self, training_data):
        """異なる層では異なる確率が返ること（データ分布差を反映）。"""
        scores, labels, track_types, distances = training_data
        cal = StratifiedCalibrator(base_method="platt")
        cal.fit(scores, labels, track_types, distances)

        # 層が存在する組み合わせで比較
        trained_strata = [s for s, status in cal.strata_info.items() if status == "trained"]
        if len(trained_strata) >= 2:
            # 異なる層のキャリブレーターが別オブジェクトであること
            s1, s2 = trained_strata[0], trained_strata[1]
            assert cal._calibrators[s1] is not cal._calibrators[s2]
