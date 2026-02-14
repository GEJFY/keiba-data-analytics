"""Keiba Data Analytics — Streamlit ダッシュボード エントリーポイント。

起動:
    streamlit run src/dashboard/app.py
"""

from datetime import timedelta
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv
from loguru import logger

from src.dashboard.components.task_status import render_task_sidebar
from src.dashboard.components.theme import apply_theme
from src.dashboard.config_loader import get_db_managers, load_config
from src.dashboard.task_manager import TaskManager

# プロジェクトルートの .env をロード
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def _init_llm_gateway(config: dict) -> None:
    """LLM Gatewayを初期化してsession_stateに格納する。"""
    try:
        from src.llm_gateway.config import create_gateway

        gateway = create_gateway()
        if gateway._providers:
            st.session_state.llm_gateway = gateway
            providers = list(gateway._providers.keys())
            logger.info(f"LLM Gateway初期化完了: プロバイダー={providers}")
        else:
            st.session_state.llm_gateway = None
            logger.info("LLM Gateway: 利用可能なプロバイダーなし（API key未設定）")
    except Exception as e:
        st.session_state.llm_gateway = None
        logger.warning(f"LLM Gateway初期化失敗: {e}")


def _init_session_state() -> None:
    """初回起動時にsession_stateを初期化する。"""
    if "initialized" in st.session_state:
        return

    config = load_config()
    st.session_state.config = config

    jvlink_db, ext_db = get_db_managers(config)
    st.session_state.jvlink_db = jvlink_db
    st.session_state.ext_db = ext_db
    st.session_state.task_manager = TaskManager()
    st.session_state.workflow_completed = set()

    # LLM Gateway初期化
    _init_llm_gateway(config)

    st.session_state.initialized = True


@st.fragment(run_every=timedelta(seconds=3))
def _task_refresh_trigger() -> None:
    """バックグラウンドタスク状態を定期チェックし、変化があればページを更新する。

    @st.fragment(run_every=3) により3秒ごとに自動実行される。
    アクティブタスクまたは未通知の完了タスクがある場合にのみ
    フルページ更新を発火する。タスクがなければ何もしない。
    """
    tm: TaskManager | None = st.session_state.get("task_manager")
    if not tm:
        return
    if tm.get_active_tasks() or tm.has_pending_notifications():
        st.rerun(scope="app")


def main() -> None:
    """ダッシュボードのメインエントリーポイント。"""
    st.set_page_config(
        page_title="Keiba Data Analytics",
        page_icon="🏇",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    apply_theme()
    _init_session_state()

    # ナビゲーション定義
    pages = {
        "データ管理": [
            st.Page("pages/page_data.py", title="データ管理", icon="🗄️"),
        ],
        "分析": [
            st.Page("pages/page_factors.py", title="ファクター管理", icon="📊"),
            st.Page("pages/page_factor_analysis.py", title="ファクター分析", icon="🔬"),
            st.Page("pages/page_strategy.py", title="戦略実行", icon="🎯"),
        ],
        "運用": [
            st.Page("pages/page_pnl.py", title="収支", icon="💰"),
            st.Page("pages/page_backtest.py", title="バックテスト", icon="📈"),
            st.Page("pages/page_model_search.py", title="モデル探索", icon="🔍"),
            st.Page("pages/page_automation.py", title="自動化", icon="⚡"),
        ],
        "AI": [
            st.Page("pages/page_ai.py", title="AIアシスタント", icon="🤖"),
        ],
        "ヘルプ": [
            st.Page("pages/page_help.py", title="ユーザーマニュアル", icon="📖"),
        ],
    }
    pg = st.navigation(pages)

    # サイドバー共通情報
    with st.sidebar:
        st.markdown("### Keiba Data Analytics")
        st.caption("GY指数方式バリュー投資戦略")
        st.divider()
        jvlink_db = st.session_state.jvlink_db
        ext_db = st.session_state.ext_db
        st.caption(f"JVLink DB: `{jvlink_db.db_path.name}`")
        st.caption(f"拡張DB: `{ext_db.db_path.name}`")

        # バックグラウンドタスク状況
        render_task_sidebar()

    pg.run()

    # タスク自動更新トリガー（非ブロッキング）
    _task_refresh_trigger()


if __name__ == "__main__":
    main()
