"""Tab 4: 収支ダッシュボードページ。

KPIカード、累積P&Lチャート、ドローダウン、月次損益表・ヒートマップ、
券種別成績、投票履歴を表示する。
"""

import pandas as pd
import streamlit as st

from src.dashboard.components.charts import (
    bar_chart,
    cumulative_pnl_chart,
    drawdown_chart,
    monthly_heatmap,
    pie_chart,
)
from src.dashboard.components.kpi_cards import render_kpi_row
from src.dashboard.components.workflow_bar import render_workflow_bar
from src.data.db import DatabaseManager


@st.cache_data(ttl=60, show_spinner=False)
def _load_bankroll_log(_ext_db: DatabaseManager) -> pd.DataFrame:
    """bankroll_logテーブルを読み込む。"""
    if not _ext_db.table_exists("bankroll_log"):
        return pd.DataFrame()
    rows = _ext_db.execute_query(
        "SELECT date, opening_balance, total_stake, total_payout, "
        "closing_balance, pnl, roi FROM bankroll_log ORDER BY date"
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def _load_bets(_ext_db: DatabaseManager) -> pd.DataFrame:
    """betsテーブルを読み込む。"""
    if not _ext_db.table_exists("bets"):
        return pd.DataFrame()
    rows = _ext_db.execute_query(
        "SELECT bet_id, race_key, bet_type, selection, stake_yen, "
        "est_prob, odds_at_bet, est_ev, status, result, payout_yen, created_at "
        "FROM bets ORDER BY created_at DESC LIMIT 500"
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _compute_cumulative(df_log: pd.DataFrame) -> tuple[list, list, list]:
    """bankroll_logから累積P&L、ドローダウン系列を計算する。"""
    dates = df_log["date"].tolist()
    cum_pnl = []
    drawdowns = []
    running = 0
    peak = 0
    for _, row in df_log.iterrows():
        running += row["pnl"]
        cum_pnl.append(running)
        if running > peak:
            peak = running
        dd = (peak - running) / max(peak, 1) if peak > 0 else 0.0
        drawdowns.append(-dd)
    return dates, cum_pnl, drawdowns


def _build_monthly_table(df_log: pd.DataFrame) -> pd.DataFrame:
    """bankroll_logから月次集計テーブルを構築する。"""
    df = df_log.copy()
    df["year_month"] = df["date"].astype(str).str[:7]  # YYYY-MM or YYYYMM→YYYY-MM
    # YYYYMMDDフォーマットの場合
    if df["year_month"].str.len().max() == 6:
        df["year_month"] = df["date"].astype(str).str[:4] + "-" + df["date"].astype(str).str[4:6]

    monthly = df.groupby("year_month").agg(
        total_stake=("total_stake", "sum"),
        total_payout=("total_payout", "sum"),
        pnl=("pnl", "sum"),
        days=("date", "count"),
    ).reset_index()

    monthly["roi"] = monthly.apply(
        lambda r: r["pnl"] / r["total_stake"] if r["total_stake"] > 0 else 0.0, axis=1
    )
    monthly["recovery_rate"] = monthly.apply(
        lambda r: r["total_payout"] / r["total_stake"] if r["total_stake"] > 0 else 0.0, axis=1
    )
    return monthly.sort_values("year_month")


def _build_heatmap_data(
    monthly: pd.DataFrame,
) -> tuple[list[int], list[int], list[list[float]]]:
    """月次データからヒートマップ用のデータを構築する。"""
    if monthly.empty:
        return [], [], []

    monthly = monthly.copy()
    monthly["year"] = monthly["year_month"].str[:4].astype(int)
    monthly["month"] = monthly["year_month"].str[5:7].astype(int)

    years = sorted(monthly["year"].unique())
    months = list(range(1, 13))

    values = []
    for y in years:
        row = []
        for m in months:
            match = monthly[(monthly["year"] == y) & (monthly["month"] == m)]
            row.append(int(match["pnl"].sum()) if not match.empty else 0)
        values.append(row)

    return years, months, values


def _build_bet_type_stats(df_bets: pd.DataFrame) -> pd.DataFrame:
    """券種別成績を集計する。"""
    if df_bets.empty or "bet_type" not in df_bets.columns:
        return pd.DataFrame()

    settled = df_bets[df_bets["status"] == "SETTLED"].copy() if "status" in df_bets.columns else df_bets.copy()
    if settled.empty:
        return pd.DataFrame()

    stats = settled.groupby("bet_type").agg(
        n_bets=("bet_id", "count"),
        total_stake=("stake_yen", "sum"),
        total_payout=("payout_yen", "sum"),
    ).reset_index()

    if "result" in settled.columns:
        wins = settled[settled["result"] == "WIN"].groupby("bet_type").size().reset_index(name="n_wins")
        stats = stats.merge(wins, on="bet_type", how="left")
        stats["n_wins"] = stats["n_wins"].fillna(0).astype(int)
    else:
        stats["n_wins"] = 0

    stats["pnl"] = stats["total_payout"] - stats["total_stake"]
    stats["win_rate"] = stats.apply(
        lambda r: r["n_wins"] / r["n_bets"] if r["n_bets"] > 0 else 0.0, axis=1
    )
    stats["roi"] = stats.apply(
        lambda r: r["pnl"] / r["total_stake"] if r["total_stake"] > 0 else 0.0, axis=1
    )

    return stats.sort_values("n_bets", ascending=False)


# ==============================
# ページ本体
# ==============================
st.header("収支ダッシュボード")
render_workflow_bar("betting")

ext_db: DatabaseManager = st.session_state.ext_db

# --- bankroll_log 取得 ---
df_log = _load_bankroll_log(ext_db)
df_bets = _load_bets(ext_db)

if df_log.empty and df_bets.empty:
    st.info(
        "収支データがまだありません。\n\n"
        "戦略実行やバックテストを行うとデータが蓄積されます。\n"
        "`scripts/demo_scenario.py` を実行するとサンプルデータが生成されます。"
    )

# --- KPIカード ---
if not df_log.empty:
    total_stake = int(df_log["total_stake"].sum())
    total_payout = int(df_log["total_payout"].sum())
    total_pnl = int(df_log["pnl"].sum())
    recovery_rate = total_payout / total_stake if total_stake > 0 else 0.0
    roi = total_pnl / total_stake if total_stake > 0 else 0.0
    latest_balance = int(df_log.iloc[-1]["closing_balance"])

    # 最大ドローダウン計算
    _, cum_pnl, drawdowns = _compute_cumulative(df_log)
    max_dd = min(drawdowns) if drawdowns else 0.0

    render_kpi_row([
        {"label": "累計P&L", "value": f"{total_pnl:+,}円"},
        {"label": "ROI", "value": f"{roi:+.1%}"},
        {"label": "回収率", "value": f"{recovery_rate:.1%}"},
        {"label": "残高", "value": f"{latest_balance:,}円"},
        {"label": "最大DD", "value": f"{max_dd:.1%}"},
    ])

    st.divider()

    # --- チャート ---
    dates, cum_pnl_vals, dd_vals = _compute_cumulative(df_log)

    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        fig_pnl = cumulative_pnl_chart(dates, cum_pnl_vals, "累積 P&L")
        st.plotly_chart(fig_pnl, use_container_width=True)
    with col_chart2:
        fig_dd = drawdown_chart(dates, dd_vals, "ドローダウン")
        st.plotly_chart(fig_dd, use_container_width=True)

    # --- 日次収支棒グラフ ---
    st.subheader("日次P&L")
    fig_daily = bar_chart(
        labels=df_log["date"].tolist(),
        values=df_log["pnl"].tolist(),
        title="日次 P&L",
    )
    st.plotly_chart(fig_daily, use_container_width=True)

    st.divider()

    # --- 月次損益表 ---
    st.subheader("月次損益表")
    monthly = _build_monthly_table(df_log)
    if not monthly.empty:
        display_monthly = monthly.copy()
        display_monthly.columns = ["年月", "投票額", "払戻額", "P&L", "稼働日数", "ROI", "回収率"]
        display_monthly["投票額"] = display_monthly["投票額"].apply(lambda x: f"{int(x):,}")
        display_monthly["払戻額"] = display_monthly["払戻額"].apply(lambda x: f"{int(x):,}")
        display_monthly["P&L"] = display_monthly["P&L"].apply(lambda x: f"{int(x):+,}")
        display_monthly["ROI"] = display_monthly["ROI"].apply(lambda x: f"{x:+.1%}")
        display_monthly["回収率"] = display_monthly["回収率"].apply(lambda x: f"{x:.1%}")
        st.dataframe(display_monthly, use_container_width=True, hide_index=True)

        # --- 月次P&Lヒートマップ ---
        years, months, heatmap_values = _build_heatmap_data(monthly)
        if years:
            st.subheader("月次P&Lヒートマップ")
            fig_hm = monthly_heatmap(years, months, heatmap_values)
            st.plotly_chart(fig_hm, use_container_width=True)

        # --- 月次P&L棒グラフ ---
        st.subheader("月次P&L推移")
        fig_monthly_bar = bar_chart(
            labels=monthly["year_month"].tolist(),
            values=monthly["pnl"].tolist(),
            title="月次 P&L",
        )
        st.plotly_chart(fig_monthly_bar, use_container_width=True)

# --- 券種別成績 ---
if not df_bets.empty:
    st.divider()
    st.subheader("券種別成績")
    bet_stats = _build_bet_type_stats(df_bets)
    if not bet_stats.empty:
        col_table, col_pie = st.columns([3, 2])
        with col_table:
            display_stats = bet_stats.copy()
            display_stats.columns = [
                "券種", "件数", "投票額", "払戻額", "的中数", "P&L", "的中率", "ROI",
            ]
            display_stats["投票額"] = display_stats["投票額"].apply(lambda x: f"{int(x):,}")
            display_stats["払戻額"] = display_stats["払戻額"].apply(lambda x: f"{int(x):,}")
            display_stats["P&L"] = display_stats["P&L"].apply(lambda x: f"{int(x):+,}")
            display_stats["的中率"] = display_stats["的中率"].apply(lambda x: f"{x:.1%}")
            display_stats["ROI"] = display_stats["ROI"].apply(lambda x: f"{x:+.1%}")
            st.dataframe(display_stats, use_container_width=True, hide_index=True)

        with col_pie:
            fig_pie = pie_chart(
                labels=bet_stats["bet_type"].tolist(),
                values=bet_stats["total_stake"].tolist(),
                title="投票額構成比",
            )
            st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("決済済みの投票データがありません。")

# --- 投票履歴 ---
st.divider()
st.subheader("投票履歴")
if df_bets.empty:
    st.info("投票履歴はまだありません。")
else:
    # フィルター
    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        bet_types = ["全て"] + sorted(df_bets["bet_type"].unique().tolist())
        selected_type = st.selectbox("券種フィルタ", bet_types)
    with col_filter2:
        if "result" in df_bets.columns:
            results = ["全て"] + sorted(df_bets["result"].dropna().unique().tolist())
            selected_result = st.selectbox("結果フィルタ", results)
        else:
            selected_result = "全て"

    filtered = df_bets.copy()
    if selected_type != "全て":
        filtered = filtered[filtered["bet_type"] == selected_type]
    if selected_result != "全て" and "result" in filtered.columns:
        filtered = filtered[filtered["result"] == selected_result]

    st.dataframe(filtered, use_container_width=True, hide_index=True, height=400)
    st.caption(f"表示中: {len(filtered)} 件 / 全 {len(df_bets)} 件")

# --- CSVエクスポート ---
st.divider()
st.subheader("データエクスポート")
col_exp1, col_exp2, col_exp3 = st.columns(3)
with col_exp1:
    if not df_log.empty:
        csv_log = df_log.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "日次収支CSV", csv_log, "bankroll_log.csv", "text/csv",
            help="bankroll_logテーブルの全データをCSVでダウンロード",
        )
with col_exp2:
    if not df_bets.empty:
        csv_bets = df_bets.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "投票履歴CSV", csv_bets, "betting_history.csv", "text/csv",
            help="投票履歴の全データをCSVでダウンロード",
        )
with col_exp3:
    if not df_log.empty:
        monthly = _build_monthly_table(df_log)
        if not monthly.empty:
            csv_monthly = monthly.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "月次集計CSV", csv_monthly, "monthly_pnl.csv", "text/csv",
                help="月次集計データをCSVでダウンロード",
            )
