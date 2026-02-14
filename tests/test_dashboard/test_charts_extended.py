"""追加チャート関数の単体テスト。

histogram_chart, scatter_chart, horizontal_bar_chart,
radar_chart, cumulative_line_chart, multi_bar_comparison をテストする。
"""

import pytest

from src.dashboard.components.charts import (
    cumulative_line_chart,
    histogram_chart,
    horizontal_bar_chart,
    multi_bar_comparison,
    radar_chart,
    scatter_chart,
)


@pytest.mark.unit
class TestHistogramChart:
    """histogram_chart のテスト。"""

    def test_basic(self) -> None:
        fig = histogram_chart([1, 2, 3, 4, 5], "テスト")
        assert fig is not None
        assert fig.layout.title.text == "テスト"

    def test_custom_nbins(self) -> None:
        fig = histogram_chart([1, 2, 3, 4, 5, 6, 7, 8], "分布", nbins=4)
        assert fig is not None

    def test_empty_values(self) -> None:
        fig = histogram_chart([], "空データ")
        assert fig is not None


@pytest.mark.unit
class TestScatterChart:
    """scatter_chart のテスト。"""

    def test_basic(self) -> None:
        fig = scatter_chart([1, 2, 3], [10, 20, 30], ["A", "B", "C"], "散布図", "X", "Y")
        assert fig is not None
        assert fig.layout.title.text == "散布図"

    def test_empty(self) -> None:
        fig = scatter_chart([], [], [], "空散布図", "X", "Y")
        assert fig is not None


@pytest.mark.unit
class TestHorizontalBarChart:
    """horizontal_bar_chart のテスト。"""

    def test_basic(self) -> None:
        fig = horizontal_bar_chart(["A", "B", "C"], [10, 20, 30], "横棒テスト")
        assert fig is not None
        assert fig.layout.title.text == "横棒テスト"

    def test_empty(self) -> None:
        fig = horizontal_bar_chart([], [], "空横棒")
        assert fig is not None


@pytest.mark.unit
class TestRadarChart:
    """radar_chart のテスト。"""

    def test_basic(self) -> None:
        fig = radar_chart(["速さ", "スタミナ", "パワー"], [80, 60, 70], "レーダー")
        assert fig is not None
        assert fig.layout.title.text == "レーダー"

    def test_no_fill(self) -> None:
        fig = radar_chart(["A", "B", "C"], [50, 60, 70], "ノーフィル", fill=False)
        assert fig is not None


@pytest.mark.unit
class TestCumulativeLineChart:
    """cumulative_line_chart のテスト。"""

    def test_basic(self) -> None:
        fig = cumulative_line_chart(["2024-01", "2024-02", "2024-03"], [100, 150, 120], "累積")
        assert fig is not None
        assert fig.layout.title.text == "累積"

    def test_empty(self) -> None:
        fig = cumulative_line_chart([], [], "空累積")
        assert fig is not None


@pytest.mark.unit
class TestMultiBarComparison:
    """multi_bar_comparison のテスト。"""

    def test_basic(self) -> None:
        data_series = [
            {"name": "シリーズA", "values": [10, 20, 30]},
            {"name": "シリーズB", "values": [15, 25, 35]},
        ]
        fig = multi_bar_comparison(["X", "Y", "Z"], data_series, "複数棒")
        assert fig is not None
        assert fig.layout.title.text == "複数棒"

    def test_single_series(self) -> None:
        data_series = [{"name": "単独", "values": [1, 2, 3]}]
        fig = multi_bar_comparison(["A", "B", "C"], data_series, "単独棒")
        assert fig is not None

    def test_empty_series(self) -> None:
        fig = multi_bar_comparison([], [], "空棒")
        assert fig is not None
