"""Plotlyチャートコンポーネントのテスト。"""

import plotly.graph_objects as go
import pytest

from src.dashboard.components.charts import (
    bar_chart,
    cumulative_pnl_chart,
    drawdown_chart,
    equity_curve,
    monthly_heatmap,
    pie_chart,
)


class TestCumulativePnlChart:
    """累積P&Lチャートのテスト。"""

    def test_returns_figure(self) -> None:
        fig = cumulative_pnl_chart(["2025-01", "2025-02"], [100, 200])
        assert isinstance(fig, go.Figure)

    def test_with_negative_values(self) -> None:
        """損失を含むデータでもFigureを返す。"""
        fig = cumulative_pnl_chart(
            ["2025-01", "2025-02", "2025-03"],
            [100, -50, 200],
        )
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 2  # 損失部分のトレースが追加

    def test_custom_title(self) -> None:
        fig = cumulative_pnl_chart(["2025-01"], [100], title="テスト")
        assert fig.layout.title.text == "テスト"

    def test_empty_data(self) -> None:
        fig = cumulative_pnl_chart([], [])
        assert isinstance(fig, go.Figure)


class TestDrawdownChart:
    """ドローダウンチャートのテスト。"""

    def test_returns_figure(self) -> None:
        fig = drawdown_chart(["2025-01", "2025-02"], [-0.05, -0.10])
        assert isinstance(fig, go.Figure)

    def test_y_axis_reversed(self) -> None:
        """Y軸が反転していること。"""
        fig = drawdown_chart(["2025-01"], [-0.05])
        assert fig.layout.yaxis.autorange == "reversed"


class TestEquityCurve:
    """エクイティカーブのテスト。"""

    def test_returns_figure(self) -> None:
        fig = equity_curve(
            ["2025-01", "2025-02", "2025-03"],
            [1_000_000, 1_050_000, 1_100_000],
        )
        assert isinstance(fig, go.Figure)

    def test_custom_title(self) -> None:
        fig = equity_curve(["2025-01"], [1_000_000], title="残高推移")
        assert fig.layout.title.text == "残高推移"


class TestBarChart:
    """棒グラフのテスト。"""

    def test_returns_figure(self) -> None:
        fig = bar_chart(["A", "B", "C"], [100, -50, 200])
        assert isinstance(fig, go.Figure)

    def test_positive_negative_colors(self) -> None:
        """正負で色分けされること。"""
        fig = bar_chart(["A", "B"], [100, -50])
        marker_colors = fig.data[0].marker.color
        assert len(marker_colors) == 2
        assert marker_colors[0] != marker_colors[1]  # 異なる色


class TestMonthlyHeatmap:
    """月次P&Lヒートマップのテスト。"""

    def test_returns_figure(self) -> None:
        fig = monthly_heatmap(
            [2024, 2025],
            list(range(1, 13)),
            [[0] * 12, [0] * 12],
        )
        assert isinstance(fig, go.Figure)

    def test_with_values(self) -> None:
        """値を含むヒートマップが生成されること。"""
        values = [
            [10000, -5000, 20000, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ]
        fig = monthly_heatmap([2025], list(range(1, 13)), values)
        assert isinstance(fig, go.Figure)
        # Plotlyは内部で型変換する場合がある（list/tuple）ため値のみ比較
        for actual_row, expected_row in zip(fig.data[0].z, values):
            assert list(actual_row) == expected_row

    def test_custom_title(self) -> None:
        fig = monthly_heatmap([2025], list(range(1, 13)), [[0] * 12], title="テスト")
        assert fig.layout.title.text == "テスト"

    def test_empty_data(self) -> None:
        fig = monthly_heatmap([], [], [])
        assert isinstance(fig, go.Figure)


class TestPieChart:
    """円グラフのテスト。"""

    def test_returns_figure(self) -> None:
        fig = pie_chart(["WIN", "PLACE", "EXACTA"], [50000, 30000, 20000])
        assert isinstance(fig, go.Figure)

    def test_custom_title(self) -> None:
        fig = pie_chart(["A", "B"], [100, 200], title="テスト")
        assert fig.layout.title.text == "テスト"

    def test_donut_hole(self) -> None:
        """ドーナツ型であること。"""
        fig = pie_chart(["A", "B"], [100, 200])
        assert fig.data[0].hole == 0.4
