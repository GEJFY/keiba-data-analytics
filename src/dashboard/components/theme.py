"""ダッシュボード ダークテーマ定義。

技術仕様書 Section 11 に基づくカラースキーム。
"""

import streamlit as st

# カラーパレット
BG_PRIMARY = "#0D1117"
BG_SECONDARY = "#161B22"
BG_TERTIARY = "#21262D"
TEXT_PRIMARY = "#E6EDF3"
TEXT_SECONDARY = "#8B949E"
ACCENT_BLUE = "#58A6FF"
ACCENT_GREEN = "#3FB950"
ACCENT_RED = "#F85149"
ACCENT_YELLOW = "#D29922"
BORDER = "#30363D"


def apply_theme() -> None:
    """Streamlitにダークテーマ用CSSを注入する。"""
    st.markdown(
        f"""
        <style>
        /* メインエリア */
        .stApp {{
            background-color: {BG_PRIMARY};
            color: {TEXT_PRIMARY};
        }}

        /* サイドバー */
        section[data-testid="stSidebar"] {{
            background-color: {BG_SECONDARY};
        }}

        /* メトリクスカード */
        [data-testid="stMetric"] {{
            background-color: {BG_SECONDARY};
            border: 1px solid {BORDER};
            border-radius: 8px;
            padding: 16px;
        }}
        [data-testid="stMetricValue"] {{
            font-family: 'JetBrains Mono', 'Consolas', monospace;
            font-size: 1.8rem;
        }}
        [data-testid="stMetricDelta"] > div {{
            font-family: 'JetBrains Mono', 'Consolas', monospace;
        }}

        /* タブ */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 8px;
        }}
        .stTabs [data-baseweb="tab"] {{
            background-color: {BG_TERTIARY};
            border-radius: 6px;
            padding: 8px 16px;
            color: {TEXT_SECONDARY};
        }}
        .stTabs [aria-selected="true"] {{
            background-color: {ACCENT_BLUE};
            color: white;
        }}

        /* データフレーム */
        .stDataFrame {{
            border: 1px solid {BORDER};
            border-radius: 6px;
        }}

        /* ボタン */
        .stButton > button {{
            border: 1px solid {BORDER};
            border-radius: 6px;
        }}
        .stButton > button:hover {{
            border-color: {ACCENT_BLUE};
            color: {ACCENT_BLUE};
        }}

        /* 情報ボックス */
        .stAlert {{
            border-radius: 6px;
        }}

        /* セクション区切り */
        hr {{
            border-color: {BORDER};
        }}

        /* ソースバッジ */
        .source-badge {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            color: #fff;
        }}

        /* タスク進捗アニメーション */
        @keyframes task-pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.6; }}
        }}
        .task-running {{
            animation: task-pulse 2s infinite;
        }}

        /* プログレスバーのカスタム */
        .stProgress > div > div > div {{
            background-color: {ACCENT_BLUE};
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
