"""KPIカード表示コンポーネント。"""

import streamlit as st


def render_kpi_row(
    metrics: list[dict],
) -> None:
    """KPIカードを横一列に表示する。

    Args:
        metrics: [{"label": "ROI", "value": "5.2%", "delta": "+1.2%"}, ...]
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics, strict=False):
        with col:
            st.metric(
                label=m.get("label", ""),
                value=m.get("value", "—"),
                delta=m.get("delta"),
            )
