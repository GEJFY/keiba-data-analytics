"""Tab 6: AIアシスタントページ。

6つのAIエージェント機能を提供:
    1. レース分析 — スコアリング結果のAI分析コメント
    2. ファクター提案 — 新規ファクタールール候補の生成
    3. パフォーマンスレポート — バックテスト結果の分析レポート
    4. 自然言語クエリ — DB検索
    5. アラート解釈 — オッズ変動等のアラート分析
    6. ディープリサーチ — 馬/騎手/コースの詳細分析

LLM Gateway未設定時もフォールバック応答で動作する。
"""

import asyncio

import streamlit as st

from src.agents.alert_interpreter import AlertInterpreterAgent
from src.agents.deep_research import DeepResearchAgent
from src.agents.factor_proposal import FactorProposalAgent
from src.agents.nl_query import NLQueryAgent
from src.agents.race_analysis import RaceAnalysisAgent
from src.agents.report import ReportAgent
from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider
from src.factors.registry import FactorRegistry
from src.scoring.engine import ScoringEngine

# 競馬場コード
JYO_MAP = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
}


def _run_async(coro):
    """Streamlit環境でasync関数を実行するヘルパー。"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, coro).result()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


# ==============================
# ページ本体
# ==============================
st.header("AIアシスタント")

jvlink_db: DatabaseManager = st.session_state.jvlink_db
ext_db: DatabaseManager = st.session_state.ext_db

# エージェント初期化（LLM Gateway未設定でもフォールバック動作）
gateway = st.session_state.get("llm_gateway", None)

# LLM接続ステータス表示
if gateway and gateway._providers:
    providers = list(gateway._providers.keys())
    st.success(f"LLM Gateway 接続済み: {', '.join(providers)}")
else:
    st.info(
        "LLM Gateway 未接続（フォールバックモードで動作中）。\n\n"
        "`.env` に `AZURE_OPENAI_API_KEY` と `AZURE_OPENAI_ENDPOINT` を設定して再起動してください。"
    )

tab_analysis, tab_factor, tab_report, tab_nl, tab_alert, tab_research = st.tabs([
    "レース分析", "ファクター提案", "レポート生成",
    "自然言語クエリ", "アラート解釈", "ディープリサーチ",
])

# ===== Tab 1: レース分析 =====
with tab_analysis:
    st.subheader("レース分析")
    st.caption("選択したレースのスコアリング結果をAIが分析します。")

    provider = JVLinkDataProvider(jvlink_db)
    races = provider.get_race_list(limit=200)

    if not races:
        st.warning("レースデータがありません。")
    else:
        race_options = {}
        for r in races:
            jyo = JYO_MAP.get(r.get("JyoCD", ""), r.get("JyoCD", ""))
            label = (
                f"{r['Year']}/{r['MonthDay'][:2]}/{r['MonthDay'][2:]} "
                f"{jyo} {r['RaceNum']}R {r.get('RaceName', '')}"
            )
            race_options[label] = r

        selected = st.selectbox("レースを選択", list(race_options.keys()), key="ai_race")
        race_row = race_options[selected]

        if st.button("分析実行", key="btn_analysis"):
            race_key = JVLinkDataProvider.build_race_key(race_row)
            race_info = provider.get_race_info(race_key)
            entries = provider.get_race_entries(race_key)
            odds_map = provider.get_odds(race_key)

            if race_info and entries:
                engine = ScoringEngine(ext_db, jvlink_provider=provider)
                scored = engine.score_race(race_info, entries, odds_map, race_key=race_key)

                agent = RaceAnalysisAgent(gateway=gateway)
                with st.spinner("分析中..."):
                    result = _run_async(agent.run({
                        "race_info": race_info,
                        "scored_results": scored,
                    }))
                st.markdown(result)
            else:
                st.warning("レースデータの取得に失敗しました。")

# ===== Tab 2: ファクター提案 =====
with tab_factor:
    st.subheader("ファクター提案")
    st.caption("現在のファクター構成を分析し、新規ルール候補を提案します。")

    if st.button("提案を生成", key="btn_factor"):
        registry = FactorRegistry(ext_db)
        active_rules = registry.get_active_rules()

        # バックテストサマリー取得
        bt_rows = ext_db.execute_query(
            "SELECT strategy_version, roi, win_rate, total_bets, pnl, max_drawdown "
            "FROM backtest_results ORDER BY executed_at DESC LIMIT 3"
        ) if ext_db.table_exists("backtest_results") else []

        bt_summary = ""
        if bt_rows:
            for r in bt_rows:
                bt_summary += (
                    f"- {r.get('strategy_version', '?')}: "
                    f"ROI={r.get('roi', 0):+.1%}, "
                    f"勝率={r.get('win_rate', 0):.1%}, "
                    f"P&L={r.get('pnl', 0):+,}円\n"
                )
        else:
            bt_summary = "バックテスト未実行"

        agent = FactorProposalAgent(gateway=gateway)
        with st.spinner("ファクター候補を生成中..."):
            result = _run_async(agent.run({
                "existing_rules": active_rules,
                "backtest_summary": bt_summary,
            }))
        st.markdown(result)

# ===== Tab 3: レポート生成 =====
with tab_report:
    st.subheader("パフォーマンスレポート")
    st.caption("バックテスト結果と運用データからレポートを生成します。")

    if st.button("レポート生成", key="btn_report"):
        # バックテスト結果取得
        bt_rows = ext_db.execute_query(
            "SELECT * FROM backtest_results ORDER BY executed_at DESC LIMIT 5"
        ) if ext_db.table_exists("backtest_results") else []

        # 有効ファクター取得
        registry = FactorRegistry(ext_db)
        active_rules = registry.get_active_rules()

        agent = ReportAgent(gateway=gateway)
        with st.spinner("レポート生成中..."):
            result = _run_async(agent.run({
                "backtest_results": bt_rows,
                "active_rules": active_rules,
            }))
        st.markdown(result)

# ===== Tab 4: 自然言語クエリ =====
with tab_nl:
    st.subheader("自然言語クエリ")
    st.caption("競馬データベースに対して自然言語で質問できます。")

    question = st.text_input(
        "質問を入力してください",
        placeholder="例: 1番人気の勝率は？",
        key="nl_question",
    )
    if st.button("検索", key="btn_nl") and question:
        agent = NLQueryAgent(gateway=gateway, jvlink_db=jvlink_db)
        with st.spinner("検索中..."):
            result = _run_async(agent.run({"question": question}))
        st.markdown(result)

# ===== Tab 5: アラート解釈 =====
with tab_alert:
    st.subheader("アラート解釈")
    st.caption("発生中のアラートを入力して、投資判断への影響を分析します。")

    alert_type = st.selectbox(
        "アラートタイプ",
        ["ODDS_DROP", "ODDS_SURGE", "SCRATCHED", "TRACK_CHANGE", "WEATHER_CHANGE"],
        key="alert_type",
    )
    alert_message = st.text_input("アラートメッセージ", key="alert_message")

    alert_data: dict = {}
    if alert_type in ("ODDS_DROP", "ODDS_SURGE"):
        rate = st.slider("変動率", 0.0, 1.0, 0.3, key="alert_rate")
        key_name = "drop_rate" if alert_type == "ODDS_DROP" else "surge_rate"
        alert_data[key_name] = rate
    elif alert_type == "SCRATCHED":
        umaban = st.text_input("馬番", key="alert_umaban")
        alert_data["umaban"] = umaban

    if st.button("アラートを解釈", key="btn_alert") and alert_message:
        agent = AlertInterpreterAgent(gateway=gateway)
        with st.spinner("解釈中..."):
            result = _run_async(agent.run({
                "alerts": [{"type": alert_type, "message": alert_message, "data": alert_data}],
                "race_info": {},
                "current_bets": [],
            }))
        st.markdown(result)

# ===== Tab 6: ディープリサーチ =====
with tab_research:
    st.subheader("ディープリサーチ")
    st.caption("馬・騎手・コースの詳細分析レポートを生成します。")

    research_type = st.selectbox(
        "リサーチタイプ",
        ["horse", "jockey", "course"],
        format_func=lambda x: {"horse": "馬", "jockey": "騎手", "course": "コース"}[x],
        key="research_type",
    )

    context: dict = {"type": research_type}
    if research_type == "horse":
        context["bamei"] = st.text_input("馬名", key="research_bamei")
    elif research_type == "jockey":
        context["kisyu"] = st.text_input("騎手略称", key="research_kisyu")
    elif research_type == "course":
        context["jyo_cd"] = st.selectbox(
            "競馬場", list(JYO_MAP.keys()),
            format_func=lambda x: JYO_MAP[x], key="research_jyo",
        )
        context["kyori"] = st.text_input("距離(m)", value="1600", key="research_kyori")

    if st.button("リサーチ実行", key="btn_research"):
        agent = DeepResearchAgent(gateway=gateway, jvlink_db=jvlink_db)
        with st.spinner("リサーチ中..."):
            result = _run_async(agent.run(context))
        st.markdown(result)
