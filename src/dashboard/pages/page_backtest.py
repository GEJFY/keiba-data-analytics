"""Tab 5: バックテストページ。

過去のバックテスト結果表示、新規バックテスト実行を提供する。
GYバリュー戦略をDBのレースデータに対して実行し、KPIを算出する。
実績データ（KakuteiJyuni + NL_HR_PAY）に基づく正確なバックテスト。
バックグラウンド実行対応。
"""

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import streamlit as st

from src.backtest.engine import BacktestConfig, BacktestEngine
from src.dashboard.components.charts import drawdown_chart, equity_curve
from src.dashboard.components.date_defaults import backtest_defaults
from src.dashboard.components.task_status import show_task_progress
from src.dashboard.components.workflow_bar import mark_step_completed, render_workflow_bar
from src.dashboard.config_loader import PROJECT_ROOT
from src.dashboard.task_manager import TaskManager
from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider
from src.factors.registry import FactorRegistry
from src.strategy.plugins.gy_value import GYValueStrategy


def _load_backtest_results(ext_db: DatabaseManager) -> pd.DataFrame:
    """backtest_resultsテーブルを読み込む。"""
    if not ext_db.table_exists("backtest_results"):
        return pd.DataFrame()
    rows = ext_db.execute_query(
        "SELECT bt_id, strategy_version, date_from, date_to, "
        "total_races, total_bets, total_stake, total_payout, "
        "pnl, roi, win_rate, max_drawdown, sharpe_ratio, executed_at "
        "FROM backtest_results ORDER BY executed_at DESC"
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ==============================================================
# バックグラウンドタスク用ラッパー
# ==============================================================

def _resolve_db_paths() -> tuple[str, str]:
    """メインスレッドでDBパスを解決する（submit前に呼ぶこと）。"""
    config = st.session_state.config
    db_cfg = config.get("database", {})
    jvlink_path = str((PROJECT_ROOT / db_cfg.get("jvlink_db_path", "data/jvlink.db")).resolve())
    ext_path = str((PROJECT_ROOT / db_cfg.get("extension_db_path", "data/extension.db")).resolve())
    return jvlink_path, ext_path


def _run_backtest_bg(
    jvlink_db_path: str,
    ext_db_path: str,
    date_from: str,
    date_to: str,
    initial_bankroll: int,
    ev_threshold: float,
    strategy_version: str,
    exclude_overlapping_factors: bool = False,
    progress_callback: Any = None,
) -> dict | None:
    """バックテストをバックグラウンドで実行してDB保存する。"""
    jvlink_db = DatabaseManager(jvlink_db_path)
    ext_db = DatabaseManager(ext_db_path)
    provider = JVLinkDataProvider(jvlink_db)

    if progress_callback:
        progress_callback(0, 100, "対象レースを取得中...")

    # 対象レースを一括取得（バッチクエリで高速化）
    d_from = date_from.replace("-", "").replace("/", "")
    d_to = date_to.replace("-", "").replace("/", "")
    with jvlink_db.session():
        target_races = provider.fetch_races_batch(
            date_from=d_from,
            date_to=d_to,
            max_races=10000,
            include_payouts=True,
        )
    # オッズなしレースを除外
    target_races = [r for r in target_races if r["odds"]]

    if not target_races:
        return None

    if progress_callback:
        progress_callback(0, len(target_races), f"バックテスト開始: {len(target_races)}レース")

    # 戦略・エンジン構築
    strategy = GYValueStrategy(ext_db, jvlink_db=jvlink_db, ev_threshold=ev_threshold)
    engine = BacktestEngine(strategy)
    config = BacktestConfig(
        date_from=date_from,
        date_to=date_to,
        initial_bankroll=initial_bankroll,
        strategy_version=strategy_version,
        exclude_overlapping_factors=exclude_overlapping_factors,
    )

    result = engine.run(target_races, config, progress_callback=progress_callback)

    # backtest_results DB保存
    now = datetime.now(UTC).isoformat()
    ext_db.execute_write(
        """INSERT INTO backtest_results
        (strategy_version, date_from, date_to, total_races, total_bets,
         total_stake, total_payout, pnl, roi, win_rate,
         max_drawdown, sharpe_ratio, params_json, executed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            strategy_version, date_from, date_to,
            result.total_races, result.total_bets,
            result.metrics.total_stake, result.metrics.total_payout,
            result.metrics.pnl, result.metrics.roi, result.metrics.win_rate,
            result.metrics.max_drawdown, result.metrics.sharpe_ratio,
            f'{{"ev_threshold": {ev_threshold}}}', now,
        ),
    )

    # bankroll_log に日次スナップショットを保存
    if result.daily_snapshots:
        for snap in result.daily_snapshots:
            roi = snap.pnl / max(snap.opening_balance, 1)
            ext_db.execute_write(
                """INSERT INTO bankroll_log
                (date, opening_balance, total_stake, total_payout,
                 closing_balance, pnl, roi, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    snap.date, snap.opening_balance, snap.total_stake,
                    snap.total_payout, snap.closing_balance, snap.pnl,
                    roi, f"backtest:{strategy_version}",
                ),
            )

    return {
        "total_races": result.total_races,
        "total_bets": result.total_bets,
        "metrics": result.metrics,
        "daily_snapshots": result.daily_snapshots,
    }


# ==============================================================
# 進捗表示ヘルパー
# ==============================================================

# ==============================================================
# 結果表示ヘルパー
# ==============================================================

def _render_backtest_result(result: dict) -> None:
    """バックテスト結果のKPI・チャートを表示する。"""
    mark_step_completed("backtest")
    m = result["metrics"]
    st.success(
        f"バックテスト完了: {result['total_races']}レース / "
        f"{result['total_bets']}ベット"
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ROI", f"{m.roi:+.1%}")
    c2.metric("勝率", f"{m.win_rate:.1%}")
    c3.metric("P&L", f"{m.pnl:+,}円")
    c4.metric("最大DD", f"{m.max_drawdown:.1%}")

    # 詳細リスク指標
    with st.expander("詳細リスク指標", expanded=False):
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("回収率", f"{m.recovery_rate:.1%}",
                  help="投票額に対する払戻額の割合。100%以上で利益。")
        r2.metric("シャープレシオ", f"{m.sharpe_ratio:.3f}",
                  help="リターン/リスク比。高いほど効率的。0.5以上で良好。")
        r3.metric("ソルティノレシオ", f"{m.sortino_ratio:.3f}",
                  help="下方リスクのみ考慮。Sharpeより保守的な評価。")
        r4.metric("カルマーレシオ", f"{m.calmar_ratio:.3f}",
                  help="ROI/最大DD。1.0以上でDD対比リターンが優秀。")

        r5, r6, r7, r8 = st.columns(4)
        r5.metric("PF", f"{m.profit_factor:.2f}",
                  help="プロフィットファクター（総利益/総損失）。1.0以上で利益。")
        r6.metric("ペイオフレシオ", f"{m.payoff_ratio:.2f}",
                  help="平均利益/平均損失。勝率が低くてもこれが高ければ利益。")
        r7.metric("95% VaR", f"{m.var_95:,.0f}円",
                  help="95%の確率でこの金額以内に損失が収まる見込み。")
        r8.metric("1ベット期待値", f"{m.edge:+,.0f}円",
                  help="1ベットあたりの期待利益。正ならエッジあり。")

        r9, r10, r11, r12 = st.columns(4)
        r9.metric("平均利益", f"{m.avg_win:,.0f}円")
        r10.metric("平均損失", f"{m.avg_loss:,.0f}円")
        r11.metric("最大連勝", f"{m.max_consecutive_wins}")
        r12.metric("最大連敗", f"{m.max_consecutive_losses}")

    # エクイティカーブ（今回実行分）
    snapshots = result.get("daily_snapshots", [])
    if snapshots:
        dates = [s.date for s in snapshots]
        balances = [s.closing_balance for s in snapshots]

        st.subheader("エクイティカーブ")
        fig = equity_curve(dates, balances, "残高推移")
        st.plotly_chart(fig, use_container_width=True)

        # ドローダウン
        dd_pct = []
        peak_balance = balances[0]
        for b in balances:
            if b > peak_balance:
                peak_balance = b
            dd = (peak_balance - b) / max(peak_balance, 1)
            dd_pct.append(-dd)

        if any(v < 0 for v in dd_pct):
            st.subheader("ドローダウン")
            fig_dd = drawdown_chart(dates, dd_pct, "ドローダウン推移")
            st.plotly_chart(fig_dd, use_container_width=True)


# ==============================
# ページ本体
# ==============================
st.header("バックテスト")
render_workflow_bar("backtest")

tm: TaskManager = st.session_state.task_manager
jvlink_db: DatabaseManager = st.session_state.jvlink_db
ext_db: DatabaseManager = st.session_state.ext_db
config = st.session_state.config
_jvlink_db_path, _ext_db_path = _resolve_db_paths()

# --- 過去の結果 ---
st.subheader("バックテスト結果一覧")
df_bt = _load_backtest_results(ext_db)

if df_bt.empty:
    st.info(
        "バックテスト結果がまだありません。\n\n"
        "下のフォームからバックテストを実行してください。"
    )
else:
    # 結果テーブル
    df_display = df_bt.copy()
    df_display["roi"] = df_display["roi"].apply(lambda x: f"{x:+.1%}" if pd.notna(x) else "—")
    df_display["win_rate"] = df_display["win_rate"].apply(lambda x: f"{x:.1%}" if x > 0 else "—")
    df_display["max_drawdown"] = df_display["max_drawdown"].apply(
        lambda x: f"{x:.1%}" if x > 0 else "—"
    )
    df_display["pnl"] = df_display["pnl"].apply(lambda x: f"{x:+,}")
    df_display["total_stake"] = df_display["total_stake"].apply(lambda x: f"{x:,}")

    st.dataframe(
        df_display[["bt_id", "strategy_version", "date_from", "date_to",
                     "total_races", "total_bets", "total_stake", "pnl",
                     "roi", "win_rate", "max_drawdown", "executed_at"]],
        use_container_width=True,
        hide_index=True,
    )

    # エクイティカーブ（bankroll_logがあれば）
    if ext_db.table_exists("bankroll_log"):
        rows = ext_db.execute_query(
            "SELECT date, closing_balance FROM bankroll_log ORDER BY date"
        )
        if rows:
            dates = [r["date"] for r in rows]
            balances = [r["closing_balance"] for r in rows]
            st.subheader("エクイティカーブ")
            fig = equity_curve(dates, balances, "残高推移")
            st.plotly_chart(fig, use_container_width=True)

            # ドローダウン計算
            dd_pct = []
            peak_balance = balances[0]
            for b in balances:
                if b > peak_balance:
                    peak_balance = b
                dd = (peak_balance - b) / max(peak_balance, 1)
                dd_pct.append(-dd)

            if any(v < 0 for v in dd_pct):
                st.subheader("ドローダウン")
                fig_dd = drawdown_chart(dates, dd_pct, "ドローダウン推移")
                st.plotly_chart(fig_dd, use_container_width=True)

# --- 新規バックテスト実行 ---
st.divider()
st.subheader("新規バックテスト実行")

bankroll_config = config.get("bankroll", {})

# --- データリーケージチェック ---
registry = FactorRegistry(ext_db)

# バックグラウンドタスク進捗表示
is_running = tm.has_running("バックテスト")
show_task_progress("bt_task_id", "bt_result", tm)

# 前回の結果表示
bt_result = st.session_state.get("bt_result")
if bt_result is not None:
    _render_backtest_result(bt_result)
    if st.button("結果をクリア", key="btn_bt_clear"):
        del st.session_state["bt_result"]
        st.toast("結果をクリアしました", icon="🗑️")
        st.rerun()

bt_default_from, bt_default_to = backtest_defaults()

with st.form("backtest_form"):
    col1, col2 = st.columns(2)
    with col1:
        date_from = st.date_input("開始日", value=bt_default_from)
        initial_bankroll = st.number_input(
            "初期資金 (円)", value=bankroll_config.get("initial_balance", 1_000_000), step=100_000
        )
    with col2:
        date_to = st.date_input("終了日", value=bt_default_to)
        strategy_version = st.text_input("戦略バージョン", value="GY_VALUE v1.0.0")

    ev_threshold = st.slider(
        "EV閾値", min_value=1.00, max_value=1.50, value=1.15, step=0.01,
        help="推奨: 1.15。高いほどROI改善（件数減少）。1.05=緩め、1.15=標準、1.30=厳選。",
    )
    exclude_overlapping = st.checkbox(
        "訓練データ重複ファクターを除外する（データリーケージ防止）",
        value=False,
        help="Weight最適化に使用したデータ期間と重複するファクターを除外してバックテストを実行します。",
    )

    btn_label = "実行中..." if is_running else "バックテスト実行"
    submitted = st.form_submit_button(btn_label, disabled=is_running)
    if submitted:
        # データリーケージチェック結果を表示
        overlap_info = registry.check_training_overlap(str(date_from), str(date_to))
        if overlap_info["has_overlap"]:
            overlap_names = [r["rule_name"] for r in overlap_info["overlapping_rules"]]
            if exclude_overlapping:
                st.info(
                    f"データリーケージ防止: {len(overlap_names)}件の重複ファクターを除外して実行します。"
                )
            else:
                st.warning(
                    f"注意: {len(overlap_names)}件のファクターがWeight訓練期間とバックテスト期間で重複しています。"
                    f" 過学習した結果になる可能性があります。"
                )
            with st.expander("重複ファクター詳細"):
                for rule in overlap_info["overlapping_rules"]:
                    st.write(
                        f"- **{rule['rule_name']}**: "
                        f"訓練期間 {rule.get('training_from', '?')} ~ {rule.get('training_to', '?')}"
                    )
        elif overlap_info["no_training_info"]:
            st.info(
                f"{len(overlap_info['no_training_info'])}件のファクターに訓練期間が未記録です。"
                " Weight最適化後に自動記録されます。"
            )

        # バックグラウンドで実行
        task_id = tm.submit(
            name="バックテスト",
            target=_run_backtest_bg,
            kwargs={
                "jvlink_db_path": _jvlink_db_path,
                "ext_db_path": _ext_db_path,
                "date_from": str(date_from),
                "date_to": str(date_to),
                "initial_bankroll": initial_bankroll,
                "ev_threshold": ev_threshold,
                "strategy_version": strategy_version,
                "exclude_overlapping_factors": exclude_overlapping,
            },
        )
        st.session_state["bt_task_id"] = task_id
        st.toast("バックテストを開始しました — サイドバーで進捗を確認できます", icon="⏳")
        st.rerun()
