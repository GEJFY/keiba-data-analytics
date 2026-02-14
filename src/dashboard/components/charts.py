"""Plotlyチャートコンポーネント群。

技術仕様書 Section 11 に準拠したグラフ関数。
"""

import plotly.graph_objects as go

from src.dashboard.components.theme import (
    ACCENT_BLUE,
    ACCENT_GREEN,
    ACCENT_RED,
    ACCENT_YELLOW,
    BG_PRIMARY,
    BG_SECONDARY,
    BORDER,
    TEXT_PRIMARY,
    TEXT_SECONDARY,
)

# 共通レイアウト
_BASE_LAYOUT = dict(
    paper_bgcolor=BG_PRIMARY,
    plot_bgcolor=BG_SECONDARY,
    font=dict(color=TEXT_PRIMARY, family="JetBrains Mono, Consolas, monospace"),
    margin=dict(l=40, r=20, t=40, b=40),
    xaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER),
    yaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER),
)


def cumulative_pnl_chart(
    dates: list[str],
    cumulative_pnl: list[int],
    title: str = "Cumulative P&L",
) -> go.Figure:
    """累積P&L面グラフ。利益は緑、損失は赤で塗り分け。"""
    colors = [ACCENT_GREEN if v >= 0 else ACCENT_RED for v in cumulative_pnl]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=cumulative_pnl,
            mode="lines",
            fill="tozeroy",
            line=dict(color=ACCENT_GREEN, width=2),
            fillcolor="rgba(63, 185, 80, 0.15)",
            name="P&L",
            hovertemplate="%{x}<br>P&L: %{y:,.0f}円<extra></extra>",
        )
    )
    # 損失部分を赤で重ねる
    neg_pnl = [min(0, v) for v in cumulative_pnl]
    if any(v < 0 for v in neg_pnl):
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=neg_pnl,
                mode="lines",
                fill="tozeroy",
                line=dict(color=ACCENT_RED, width=0),
                fillcolor="rgba(248, 81, 73, 0.15)",
                name="Loss",
                showlegend=False,
            )
        )

    fig.update_layout(**_BASE_LAYOUT, title=title, showlegend=False)
    return fig


def drawdown_chart(
    dates: list[str],
    drawdown_pct: list[float],
    title: str = "Drawdown",
) -> go.Figure:
    """ドローダウン曲線（赤の反転面グラフ）。"""
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=drawdown_pct,
            mode="lines",
            fill="tozeroy",
            line=dict(color=ACCENT_RED, width=2),
            fillcolor="rgba(248, 81, 73, 0.2)",
            name="Drawdown",
            hovertemplate="%{x}<br>DD: %{y:.1%}<extra></extra>",
        )
    )
    layout = {k: v for k, v in _BASE_LAYOUT.items() if k != "yaxis"}
    fig.update_layout(
        **layout,
        title=title,
        yaxis=dict(
            gridcolor=BORDER,
            zerolinecolor=BORDER,
            tickformat=".0%",
            autorange="reversed",
        ),
        showlegend=False,
    )
    return fig


def equity_curve(
    dates: list[str],
    balances: list[int],
    title: str = "Equity Curve",
) -> go.Figure:
    """エクイティカーブ（残高推移）。"""
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=balances,
            mode="lines",
            line=dict(color=ACCENT_GREEN, width=2),
            name="Balance",
            hovertemplate="%{x}<br>%{y:,.0f}円<extra></extra>",
        )
    )
    fig.update_layout(**_BASE_LAYOUT, title=title, showlegend=False)
    return fig


def bar_chart(
    labels: list[str],
    values: list[float],
    title: str = "",
    color: str = ACCENT_GREEN,
    value_format: str = ",.0f",
) -> go.Figure:
    """汎用棒グラフ。"""
    bar_colors = [
        ACCENT_GREEN if v >= 0 else ACCENT_RED for v in values
    ]
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=labels,
            y=values,
            marker_color=bar_colors,
            hovertemplate=f"%{{x}}<br>%{{y:{value_format}}}<extra></extra>",
        )
    )
    fig.update_layout(**_BASE_LAYOUT, title=title, showlegend=False)
    return fig


def weight_comparison_chart(
    factor_names: list[str],
    current_weights: list[float],
    optimized_weights: list[float],
    title: str = "Weight比較: 現在 vs 最適",
) -> go.Figure:
    """現在Weight vs 最適Weightの比較棒グラフ。"""
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="現在",
            x=factor_names,
            y=current_weights,
            marker_color=TEXT_SECONDARY,
            hovertemplate="%{x}<br>現在: %{y:.2f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            name="最適",
            x=factor_names,
            y=optimized_weights,
            marker_color=ACCENT_GREEN,
            hovertemplate="%{x}<br>最適: %{y:.2f}<extra></extra>",
        )
    )
    layout = {k: v for k, v in _BASE_LAYOUT.items() if k != "margin"}
    fig.update_layout(
        **layout,
        title=title,
        barmode="group",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_tickangle=-45,
        margin=dict(l=40, r=20, t=60, b=120),
    )
    return fig


def monthly_heatmap(
    years: list[int],
    months: list[int],
    values: list[list[float]],
    title: str = "月次P&Lヒートマップ",
    value_format: str = ",",
) -> go.Figure:
    """年×月のヒートマップ。利益は緑、損失は赤。"""
    month_labels = [f"{m}月" for m in months]
    year_labels = [str(y) for y in years]

    fig = go.Figure(
        data=go.Heatmap(
            z=values,
            x=month_labels,
            y=year_labels,
            colorscale=[
                [0, ACCENT_RED],
                [0.5, BG_SECONDARY],
                [1, ACCENT_GREEN],
            ],
            zmid=0,
            hovertemplate="%{y}年%{x}<br>P&L: %{z:,}円<extra></extra>",
            texttemplate="%{z:,}",
            textfont=dict(size=11, color=TEXT_PRIMARY),
        )
    )
    layout = {k: v for k, v in _BASE_LAYOUT.items() if k not in ("xaxis", "yaxis")}
    fig.update_layout(
        **layout,
        title=title,
        xaxis=dict(side="top", gridcolor=BORDER, zerolinecolor=BORDER),
        yaxis=dict(autorange="reversed", gridcolor=BORDER, zerolinecolor=BORDER),
        height=max(200, len(years) * 60 + 100),
    )
    return fig


def pie_chart(
    labels: list[str],
    values: list[float],
    title: str = "",
) -> go.Figure:
    """円グラフ。"""
    fig = go.Figure(
        data=go.Pie(
            labels=labels,
            values=values,
            hole=0.4,
            marker=dict(colors=[ACCENT_GREEN, ACCENT_RED, ACCENT_BLUE, ACCENT_YELLOW]),
            textinfo="label+percent",
            textfont=dict(color=TEXT_PRIMARY),
            hovertemplate="%{label}<br>%{value:,}<br>%{percent}<extra></extra>",
        )
    )
    fig.update_layout(
        paper_bgcolor=BG_PRIMARY,
        plot_bgcolor=BG_SECONDARY,
        font=dict(color=TEXT_PRIMARY, family="JetBrains Mono, Consolas, monospace"),
        title=title,
        showlegend=True,
        legend=dict(font=dict(color=TEXT_SECONDARY)),
        margin=dict(l=20, r=20, t=40, b=20),
    )
    return fig


def histogram_chart(
    values: list[float],
    title: str = "",
    nbins: int = 20,
    color: str = ACCENT_BLUE,
    xaxis_title: str = "",
) -> go.Figure:
    """ヒストグラム。"""
    fig = go.Figure()
    fig.add_trace(
        go.Histogram(
            x=values,
            nbinsx=nbins,
            marker_color=color,
            opacity=0.85,
            hovertemplate="範囲: %{x}<br>件数: %{y}<extra></extra>",
        )
    )
    layout = dict(_BASE_LAYOUT)
    if xaxis_title:
        layout["xaxis"] = dict(gridcolor=BORDER, zerolinecolor=BORDER, title=xaxis_title)
    fig.update_layout(**layout, title=title, showlegend=False, bargap=0.05)
    return fig


def scatter_chart(
    x: list[float],
    y: list[float],
    labels: list[str] | None = None,
    title: str = "",
    xaxis_title: str = "",
    yaxis_title: str = "",
    color: str = ACCENT_BLUE,
) -> go.Figure:
    """散布図。"""
    hover = (
        "%{text}<br>" if labels else ""
    ) + f"{xaxis_title}: %{{x:.2f}}<br>{yaxis_title}: %{{y:.3f}}<extra></extra>"

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x,
            y=y,
            mode="markers",
            text=labels,
            marker=dict(color=color, size=8, opacity=0.7),
            hovertemplate=hover,
        )
    )
    layout = dict(_BASE_LAYOUT)
    layout["xaxis"] = dict(gridcolor=BORDER, zerolinecolor=BORDER, title=xaxis_title)
    layout["yaxis"] = dict(gridcolor=BORDER, zerolinecolor=BORDER, title=yaxis_title)
    fig.update_layout(**layout, title=title, showlegend=False)
    return fig


def horizontal_bar_chart(
    labels: list[str],
    values: list[float],
    title: str = "",
    color: str = ACCENT_BLUE,
    value_format: str = ",.0f",
) -> go.Figure:
    """横棒グラフ。"""
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=values,
            y=labels,
            orientation="h",
            marker_color=color,
            hovertemplate=f"%{{y}}<br>%{{x:{value_format}}}<extra></extra>",
        )
    )
    layout = {k: v for k, v in _BASE_LAYOUT.items() if k not in ("yaxis", "margin")}
    fig.update_layout(
        **layout,
        title=title,
        showlegend=False,
        yaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER, autorange="reversed"),
        margin=dict(l=140, r=20, t=40, b=40),
        height=max(300, len(labels) * 28),
    )
    return fig


def radar_chart(
    categories: list[str],
    values: list[float],
    title: str = "",
    fill: bool = True,
) -> go.Figure:
    """レーダーチャート。values は 0〜100 のスケール推奨。"""
    # 閉じるために先頭を末尾にも追加
    cats = list(categories) + [categories[0]]
    vals = list(values) + [values[0]]

    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=vals,
            theta=cats,
            fill="toself" if fill else "none",
            fillcolor="rgba(88, 166, 255, 0.2)",
            line=dict(color=ACCENT_BLUE, width=2),
            marker=dict(size=6, color=ACCENT_BLUE),
        )
    )
    fig.update_layout(
        paper_bgcolor=BG_PRIMARY,
        font=dict(color=TEXT_PRIMARY, family="JetBrains Mono, Consolas, monospace"),
        title=title,
        polar=dict(
            bgcolor=BG_SECONDARY,
            radialaxis=dict(visible=True, gridcolor=BORDER, range=[0, 100]),
            angularaxis=dict(gridcolor=BORDER),
        ),
        showlegend=False,
        margin=dict(l=60, r=60, t=40, b=40),
    )
    return fig


def cumulative_line_chart(
    x: list[str],
    y: list[float],
    title: str = "",
    yaxis_format: str = ".1%",
) -> go.Figure:
    """累積推移折れ線グラフ。"""
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x,
            y=y,
            mode="lines",
            line=dict(color=ACCENT_BLUE, width=2),
            hovertemplate=f"%{{x}}<br>%{{y:{yaxis_format}}}<extra></extra>",
        )
    )
    layout = {k: v for k, v in _BASE_LAYOUT.items() if k != "yaxis"}
    fig.update_layout(
        **layout,
        title=title,
        showlegend=False,
        yaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER, tickformat=yaxis_format),
    )
    return fig


def multi_bar_comparison(
    labels: list[str],
    data_series: list[dict],
    title: str = "",
) -> go.Figure:
    """複数系列の比較棒グラフ。

    Args:
        labels: X軸ラベル
        data_series: [{"name": str, "values": list[float], "color": str}, ...]
    """
    fig = go.Figure()
    for series in data_series:
        fig.add_trace(
            go.Bar(
                name=series["name"],
                x=labels,
                y=series["values"],
                marker_color=series.get("color", ACCENT_BLUE),
                hovertemplate=f"%{{x}}<br>{series['name']}: %{{y:.3f}}<extra></extra>",
            )
        )
    layout = {k: v for k, v in _BASE_LAYOUT.items() if k != "margin"}
    fig.update_layout(
        **layout,
        title=title,
        barmode="group",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=60, b=80),
    )
    return fig


def importance_chart(
    factor_names: list[str],
    importances: list[float],
    title: str = "Permutation Importance",
) -> go.Figure:
    """Permutation Importance横棒グラフ（降順）。"""
    # 降順ソート
    sorted_pairs = sorted(
        zip(factor_names, importances), key=lambda x: x[1], reverse=True
    )
    names = [p[0] for p in sorted_pairs]
    vals = [p[1] for p in sorted_pairs]

    colors = [ACCENT_GREEN if v > 0 else ACCENT_RED for v in vals]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=vals,
            y=names,
            orientation="h",
            marker_color=colors,
            hovertemplate="%{y}<br>PI: %{x:.4f}<extra></extra>",
        )
    )
    layout = {k: v for k, v in _BASE_LAYOUT.items() if k not in ("yaxis", "margin")}
    fig.update_layout(
        **layout,
        title=title,
        showlegend=False,
        yaxis=dict(
            gridcolor=BORDER,
            zerolinecolor=BORDER,
            autorange="reversed",
        ),
        margin=dict(l=200, r=20, t=40, b=40),
        height=max(400, len(names) * 28),
    )
    return fig
