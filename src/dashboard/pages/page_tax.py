"""税務レポートページ。

TaxReportGenerator を利用して年次の確定申告用サマリーを表示する。
"""

from datetime import datetime

import pandas as pd
import streamlit as st

from src.data.db import DatabaseManager
from src.reporting.tax_report import TaxReport, TaxReportGenerator


def _format_monthly_df(report: TaxReport) -> pd.DataFrame:
    """月次内訳を DataFrame に変換する。"""
    if not report.monthly_breakdown:
        return pd.DataFrame()
    rows = []
    for m in report.monthly_breakdown:
        win_rate = m.n_wins / m.n_bets if m.n_bets > 0 else 0.0
        rows.append({
            "月": m.month,
            "投票数": m.n_bets,
            "的中数": m.n_wins,
            "的中率": f"{win_rate:.1%}",
            "投票額": f"{m.total_stake:,}",
            "払戻額": f"{m.total_payout:,}",
            "収支": f"{m.pnl:+,}",
        })
    return pd.DataFrame(rows)


def _format_top_payouts_df(report: TaxReport) -> pd.DataFrame:
    """高額払戻を DataFrame に変換する。"""
    if not report.top_payouts:
        return pd.DataFrame()
    rows = []
    for p in report.top_payouts:
        rows.append({
            "日付": p.get("date", ""),
            "レースキー": p.get("race_key", ""),
            "馬番": p.get("selection", ""),
            "投票額": f"{p.get('stake', 0):,}",
            "払戻額": f"{p.get('payout', 0):,}",
            "利益": f"{p.get('profit', 0):+,}",
        })
    return pd.DataFrame(rows)


# --- ページ本体 ---

st.header("税務レポート")
st.caption("確定申告用の年次収支サマリーを生成します。")

ext_db: DatabaseManager = st.session_state.ext_db
current_year = datetime.now().year

year = st.selectbox(
    "対象年を選択",
    options=list(range(current_year, 2019, -1)),
    index=0,
    key="tax_year",
)

if st.button("レポート生成", key="btn_tax_generate"):
    gen = TaxReportGenerator(ext_db)
    report = gen.generate(year)
    st.session_state.tax_report = report

report: TaxReport | None = st.session_state.get("tax_report")

if report is None:
    st.info("対象年を選択して「レポート生成」を押してください。")
    st.stop()

if report.n_bets == 0:
    st.warning(f"{report.year}年の照合済みベットがありません。")
    st.stop()

# KPIカード
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("総払戻", f"{report.total_payout:,}円")
with col2:
    pnl = report.total_payout - report.total_stake
    st.metric("収支", f"{pnl:+,}円")
with col3:
    st.metric("一時所得", f"{report.ichiji_shotoku:,}円")
with col4:
    st.metric("課税対象額", f"{report.taxable_amount:,}円")

# 計算内訳
with st.expander("一時所得計算内訳", expanded=False):
    st.markdown(f"""
| 項目 | 金額 |
|------|------|
| 総収入金額（払戻合計） | {report.gross_income:,}円 |
| 控除可能経費（的中分購入費） | {report.deductible_expense:,}円 |
| 特別控除額 | {report.special_deduction:,}円 |
| **一時所得** | **{report.ichiji_shotoku:,}円** |
| **課税対象額（×1/2）** | **{report.taxable_amount:,}円** |
""")
    st.caption("※ハズレ馬券の購入費は一時所得の経費に算入できません。")

# 月次内訳
st.subheader("月次内訳")
df_monthly = _format_monthly_df(report)
if not df_monthly.empty:
    st.dataframe(df_monthly, use_container_width=True, hide_index=True)

# 高額払戻
st.subheader("高額払戻 Top10")
df_top = _format_top_payouts_df(report)
if not df_top.empty:
    st.dataframe(df_top, use_container_width=True, hide_index=True)

# ダウンロード
gen = TaxReportGenerator(ext_db)
summary_text = gen.format_summary(report)
st.download_button(
    label="テキストレポートをダウンロード",
    data=summary_text,
    file_name=f"tax_report_{report.year}.txt",
    mime="text/plain",
    key="btn_tax_download",
)
