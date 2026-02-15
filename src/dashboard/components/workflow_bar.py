"""全ページ共通のワークフローステップバー。

推奨作業順序を視覚的に表示し、ユーザーを次のアクションへ誘導する。
"""

from __future__ import annotations

import streamlit as st

from src.dashboard.components.theme import (
    ACCENT_BLUE,
    ACCENT_GREEN,
    BG_TERTIARY,
    BORDER,
    TEXT_SECONDARY,
)

# ワークフロー定義
WORKFLOW_STEPS = [
    {"key": "data", "label": "データ取込", "page": "pages/page_data.py", "icon": "1"},
    {"key": "factor", "label": "ファクター分析", "page": "pages/page_factor_analysis.py", "icon": "2"},
    {"key": "optimize", "label": "最適化", "page": "pages/page_factors.py", "icon": "3"},
    {"key": "backtest", "label": "バックテスト", "page": "pages/page_backtest.py", "icon": "4"},
    {"key": "betting", "label": "投票", "page": "pages/page_strategy.py", "icon": "5"},
]


def _get_completed_steps() -> set[str]:
    """完了ステップの集合を返す。"""
    return st.session_state.get("workflow_completed") or set()


def mark_step_completed(step_key: str) -> None:
    """ワークフローステップを完了としてマークする。"""
    completed = st.session_state.setdefault("workflow_completed", set())
    completed.add(step_key)


def is_step_completed(step_key: str) -> bool:
    """ステップが完了しているか判定する。"""
    return step_key in _get_completed_steps()


def render_workflow_bar(current_step: str) -> None:
    """ワークフローステップバーをページ上部に表示する。

    Args:
        current_step: 現在のページに対応するステップキー
    """
    completed = _get_completed_steps()

    # 全ステップを1つのHTML blockでレンダリング（カラム分割による描画崩れ防止）
    steps_html_parts: list[str] = []
    for i, step in enumerate(WORKFLOW_STEPS):
        key = step["key"]
        is_current = key == current_step
        is_done = key in completed

        if is_done and not is_current:
            circle_bg = ACCENT_GREEN
            circle_text = "white"
            label_color = ACCENT_GREEN
            display = "&#10003;"
        elif is_current:
            circle_bg = ACCENT_BLUE
            circle_text = "white"
            label_color = ACCENT_BLUE
            display = step["icon"]
        else:
            circle_bg = BG_TERTIARY
            circle_text = TEXT_SECONDARY
            label_color = TEXT_SECONDARY
            display = step["icon"]

        font_weight = "700" if is_current else "400"

        # コネクタ矢印（最後のステップ以外）
        if i < len(WORKFLOW_STEPS) - 1:
            connector_color = ACCENT_GREEN if is_done else BORDER
            connector = (
                f'<div style="flex:1;height:2px;background:{connector_color};'
                f'align-self:center;margin:0 4px;"></div>'
            )
        else:
            connector = ""

        step_html = (
            f'<div style="display:flex;flex-direction:column;align-items:center;'
            f'min-width:60px;">'
            f'<div style="width:36px;height:36px;border-radius:50%;'
            f'background:{circle_bg};color:{circle_text};'
            f'display:flex;align-items:center;justify-content:center;'
            f'font-weight:700;font-size:0.9rem;'
            f'border:2px solid {circle_bg};">'
            f'{display}</div>'
            f'<div style="font-size:0.75rem;color:{label_color};'
            f'font-weight:{font_weight};margin-top:4px;white-space:nowrap;">'
            f'{step["label"]}</div>'
            f'</div>'
        )

        steps_html_parts.append(step_html)
        if connector:
            steps_html_parts.append(connector)

    bar_html = (
        '<div style="display:flex;align-items:flex-start;'
        'justify-content:center;padding:8px 0;">'
        + "".join(steps_html_parts)
        + "</div>"
    )

    st.markdown(bar_html, unsafe_allow_html=True)

    # 薄い区切り線
    st.markdown(
        f'<hr style="margin:8px 0 16px 0;border-color:{BORDER};opacity:0.5;">',
        unsafe_allow_html=True,
    )
