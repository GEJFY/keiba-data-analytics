"""Tab: モデル探索結果ページ。

自律モデル探索の結果表示、パラメータ傾向分析、最適構成の適用を行う。
"""

import pandas as pd
import streamlit as st

from src.data.db import DatabaseManager
from src.search.reporter import SearchReporter
from src.search.result_store import ResultStore


def _safe_pct(val) -> str:
    try:
        return f"{float(val):+.1%}"
    except (ValueError, TypeError):
        return "—"


# ==============================
# ページ本体
# ==============================
st.header("モデル探索")

ext_db: DatabaseManager = st.session_state.ext_db

store = ResultStore(ext_db)
store.init_tables()

# --- セッション一覧 ---
st.subheader("探索セッション一覧")
sessions = store.get_sessions()

if not sessions:
    st.info(
        "探索セッションがまだありません。\n\n"
        "CLIから実行:\n"
        "```\npython scripts/run_model_search.py --date-from 20240101 --date-to 20250101\n```"
    )
    st.stop()

session_rows = []
for s in sessions:
    elapsed_h = (s.get("total_elapsed_seconds") or 0) / 3600
    session_rows.append({
        "ID": s["session_id"],
        "期間": f"{s['date_from']}~{s['date_to']}",
        "トライアル": s["n_trials"],
        "状態": s["status"],
        "所要時間": f"{elapsed_h:.1f}h" if elapsed_h > 0 else "—",
        "開始": s.get("started_at", "")[:16],
    })
st.dataframe(pd.DataFrame(session_rows), use_container_width=True, hide_index=True)

# --- セッション選択 ---
session_ids = [s["session_id"] for s in sessions]
selected_session = st.selectbox("セッションを選択", session_ids)

if not selected_session:
    st.stop()

# --- レポート生成 ---
reporter = SearchReporter(store)
summary = reporter.generate(selected_session)

# KPI
c1, c2, c3, c4 = st.columns(4)
c1.metric("完了", f"{summary.completed_trials}/{summary.total_trials}")
c2.metric("エラー", summary.error_trials)
best_score = summary.best_trial.get("composite_score", 0) if summary.best_trial else 0
c3.metric("最高スコア", f"{best_score:.1f}/100")
c4.metric("所要時間", f"{summary.elapsed_total_seconds / 3600:.1f}h")

# --- 上位構成 ---
st.subheader("上位構成 (Top 10)")
top_trials = summary.top_10_trials

if top_trials:
    rows = []
    for i, t in enumerate(top_trials, 1):
        rows.append({
            "#": i,
            "スコア": f"{t.get('composite_score', 0):.1f}",
            "ROI": _safe_pct(t.get("roi")),
            "Sharpe": f"{t.get('sharpe_ratio', 0):.3f}",
            "MaxDD": _safe_pct(t.get("max_drawdown")),
            "勝率": _safe_pct(t.get("win_rate")),
            "PF": f"{t.get('profit_factor', 0):.2f}",
            "ベット": t.get("total_bets", 0),
            "EV閾値": t.get("ev_threshold"),
            "正則化": t.get("regularization"),
            "窓(月)": t.get("train_window_months"),
            "校正": t.get("calibration_method"),
            "方式": t.get("betting_method"),
            "WF窓": t.get("wf_n_windows"),
            "Max/R": t.get("max_bets_per_race"),
            "Factor": t.get("factor_selection"),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # 最優秀構成の詳細
    best = top_trials[0]
    with st.expander("最優秀構成の詳細", expanded=True):
        bc1, bc2, bc3, bc4 = st.columns(4)
        bc1.metric("ROI (OOS)", _safe_pct(best.get("roi")))
        bc2.metric("Sharpe", f"{best.get('sharpe_ratio', 0):.3f}")
        bc3.metric("MaxDD", _safe_pct(best.get("max_drawdown")))
        bc4.metric("PF", f"{best.get('profit_factor', 0):.2f}")

        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("MC 5%ile ROI", _safe_pct(best.get("mc_roi_5th")))
        mc2.metric("MC 95%ile ROI", _safe_pct(best.get("mc_roi_95th")))
        mc3.metric("破産確率", _safe_pct(best.get("mc_ruin_probability")))
        mc4.metric("過学習比", f"{best.get('wf_overfitting_ratio', 0):.2f}")

        # レーダーチャート
        from src.dashboard.components.charts import radar_chart
        radar_cats = ["ROI", "Sharpe", "勝率", "PF", "1-MaxDD"]
        radar_vals = [
            min(max(float(best.get("roi", 0)) * 100, 0), 100),
            min(max(float(best.get("sharpe_ratio", 0)) * 50, 0), 100),
            min(max(float(best.get("win_rate", 0)) * 100, 0), 100),
            min(max(float(best.get("profit_factor", 0)) * 25, 0), 100),
            min(max((1 - float(best.get("max_drawdown", 0))) * 100, 0), 100),
        ]
        fig_radar = radar_chart(radar_cats, radar_vals, "最優秀構成プロファイル")
        st.plotly_chart(fig_radar, use_container_width=True)

else:
    st.info("有効なトライアルがありません。")

# --- パラメータ傾向分析 ---
st.divider()
st.subheader("パラメータ傾向分析")
st.caption("各パラメータの値ごとに平均スコアを算出し、最適な傾向を特定します。")

if summary.parameter_trends:
    param_labels = {
        "train_window_months": "訓練窓 (月)",
        "ev_threshold": "EV閾値",
        "regularization": "正則化 (C)",
        "target_jyuni": "ターゲット",
        "calibration_method": "校正方式",
        "betting_method": "投票方式",
        "wf_n_windows": "WF窓数",
        "max_bets_per_race": "Max ベット/R",
        "factor_selection": "ファクター選択",
    }

    cols = st.columns(3)
    for i, (param, values) in enumerate(summary.parameter_trends.items()):
        with cols[i % 3]:
            label = param_labels.get(param, param)
            st.markdown(f"**{label}**")
            trend_rows = []
            for v in values:
                trend_rows.append({
                    "値": v["value"],
                    "平均": f"{v['avg_score']:.1f}",
                    "n": v["count"],
                    "最高": f"{v['max_score']:.1f}",
                })
            st.dataframe(
                pd.DataFrame(trend_rows),
                use_container_width=True,
                hide_index=True,
                height=min(200, 35 * (len(trend_rows) + 1)),
            )
            # ミニ棒グラフ
            from src.dashboard.components.charts import horizontal_bar_chart
            trend_labels = [str(v["value"]) for v in values]
            trend_vals = [v["avg_score"] for v in values]
            fig_trend = horizontal_bar_chart(trend_labels, trend_vals, "")
            fig_trend.update_layout(height=max(120, len(values) * 25 + 40), margin=dict(l=60, r=10, t=10, b=10))
            st.plotly_chart(fig_trend, use_container_width=True)

# --- 最適構成の適用 ---
st.divider()
st.subheader("最適構成の適用")

if top_trials:
    st.info(
        "上位構成のパラメータを確認し、ファクター分析ページで "
        "同じ設定でWeight最適化を実行してください。\n\n"
        "推奨手順:\n"
        "1. 上記パラメータ傾向で最適なEV閾値・正則化を確認\n"
        "2. ファクター分析ページで該当パラメータでWeight最適化\n"
        "3. バックテストページでWalk-Forward検証"
    )

    if summary.recommendation:
        with st.expander("テキストレポート"):
            st.code(summary.recommendation)
