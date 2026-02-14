"""Tab 7: 自動化モニタリングページ。

パイプライン実行履歴の表示、設定確認、手動実行（dryrun）を提供する。
バックグラウンド実行対応。
"""

from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st

from src.dashboard.components.task_status import show_task_progress
from src.dashboard.components.workflow_bar import render_workflow_bar
from src.dashboard.config_loader import PROJECT_ROOT
from src.dashboard.task_manager import TaskManager
from src.data.db import DatabaseManager


def _load_pipeline_runs(ext_db: DatabaseManager) -> pd.DataFrame:
    """pipeline_runsテーブルを読み込む。"""
    if not ext_db.table_exists("pipeline_runs"):
        return pd.DataFrame()
    rows = ext_db.execute_query(
        "SELECT run_id, run_date, status, sync_status, "
        "races_found, races_scored, total_bets, total_stake, "
        "reconciled, errors, started_at, completed_at "
        "FROM pipeline_runs ORDER BY run_id DESC LIMIT 50"
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _get_latest_run(df: pd.DataFrame) -> dict | None:
    """最新の実行レコードを取得する。"""
    if df.empty:
        return None
    return df.iloc[0].to_dict()


# ==============================================================
# バックグラウンドタスク用ラッパー
# ==============================================================

def _run_pipeline_bg(
    jvlink_db_path: str,
    ext_db_path: str,
    run_config: dict,
    target_date: str,
    progress_callback: Any = None,
) -> dict:
    """パイプラインをバックグラウンドで実行する（dryrunモード強制）。"""
    from src.automation.pipeline import RaceDayPipeline

    jvlink_db = DatabaseManager(jvlink_db_path)
    ext_db = DatabaseManager(ext_db_path)

    if progress_callback:
        progress_callback(0, 3, "パイプライン初期化中...")

    pipeline = RaceDayPipeline(jvlink_db, ext_db, run_config)

    if progress_callback:
        progress_callback(1, 3, "パイプライン実行中...")

    result = pipeline.run_full(target_date=target_date)

    if progress_callback:
        progress_callback(3, 3, "パイプライン完了")

    return {
        "status": result.status,
        "races_found": result.races_found,
        "total_bets": result.total_bets,
        "total_stake": result.total_stake,
        "errors": result.errors,
    }


# ==============================
# ページ本体
# ==============================
def _resolve_db_paths() -> tuple[str, str]:
    """メインスレッドでDBパスを解決する（submit前に呼ぶこと）。"""
    config = st.session_state.config
    db_cfg = config.get("database", {})
    jvlink_path = str((PROJECT_ROOT / db_cfg.get("jvlink_db_path", "data/jvlink.db")).resolve())
    ext_path = str((PROJECT_ROOT / db_cfg.get("extension_db_path", "data/extension.db")).resolve())
    return jvlink_path, ext_path


st.header("自動化モニタリング")
render_workflow_bar("betting")

tm: TaskManager = st.session_state.task_manager
ext_db: DatabaseManager = st.session_state.ext_db
config = st.session_state.config
_jvlink_db_path, _ext_db_path = _resolve_db_paths()

auto_cfg = config.get("automation", {})
betting_cfg = config.get("betting", {})

# --- KPIカード ---
df_runs = _load_pipeline_runs(ext_db)
latest = _get_latest_run(df_runs)

if latest:
    st.subheader("最新パイプライン実行")
    status_map = {
        "SUCCESS": "OK",
        "PARTIAL": "一部エラー",
        "FAILED": "失敗",
        "RUNNING": "実行中",
    }
    status_label = status_map.get(latest.get("status", ""), latest.get("status", ""))

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("実行日", latest.get("run_date", "—"))
    c2.metric("ステータス", status_label)
    c3.metric("投票数", latest.get("total_bets", 0))
    c4.metric("合計投票額", f"{latest.get('total_stake', 0):,}円")
    c5.metric("照合数", latest.get("reconciled", 0))
else:
    st.info(
        "パイプライン実行履歴がありません。\n\n"
        "CLIまたは下のフォームからパイプラインを実行してください。"
    )

# --- 実行履歴テーブル ---
st.divider()
st.subheader("実行履歴")

if df_runs.empty:
    st.info("実行履歴がありません。")
else:
    df_display = df_runs.copy()
    df_display["total_stake"] = df_display["total_stake"].apply(
        lambda x: f"{x:,}" if pd.notna(x) else "—"
    )

    # エラー件数表示
    def _error_count(errors):
        if not errors or errors == "[]":
            return 0
        try:
            import json
            return len(json.loads(errors))
        except Exception:
            return 0

    df_display["error_count"] = df_display["errors"].apply(_error_count)

    st.dataframe(
        df_display[["run_id", "run_date", "status", "sync_status",
                     "races_found", "total_bets", "total_stake",
                     "reconciled", "error_count", "started_at"]],
        column_config={
            "run_id": "ID",
            "run_date": "実行日",
            "status": "ステータス",
            "sync_status": "同期",
            "races_found": "レース数",
            "total_bets": "投票数",
            "total_stake": "投票額",
            "reconciled": "照合数",
            "error_count": "エラー",
            "started_at": "開始時刻",
        },
        hide_index=True,
    )

# --- 設定表示 ---
st.divider()
st.subheader("現在の設定")

col_a, col_b = st.columns(2)
with col_a:
    st.markdown("**パイプライン設定**")
    st.text(f"自動化: {'有効' if auto_cfg.get('enabled', False) else '無効'}")
    race_days = auto_cfg.get("race_days", [])
    day_names = {0: "月", 1: "火", 2: "水", 3: "木", 4: "金", 5: "土", 6: "日"}
    days_str = ", ".join(day_names.get(d, str(d)) for d in race_days) if race_days else "毎日"
    st.text(f"レース日: {days_str}")
    st.text(f"最大レース数/日: {auto_cfg.get('max_races_per_day', 36)}")
    st.text(f"自動照合: {'有効' if auto_cfg.get('auto_reconcile', True) else '無効'}")

with col_b:
    st.markdown("**投票設定**")
    st.text(f"投票方式: {betting_cfg.get('method', 'dryrun')}")
    st.text(f"承認必須: {'はい' if betting_cfg.get('approval_required', True) else 'いいえ'}")
    st.text(f"CSV出力先: {betting_cfg.get('csv_output_dir', './data/ipatgo')}")
    st.text(f"連敗上限: {betting_cfg.get('max_consecutive_losses', 20)}")

# --- 手動実行 ---
st.divider()
st.subheader("手動パイプライン実行")
st.caption("dryrunモード強制で安全に実行されます。")

# バックグラウンドタスク進捗表示
is_running = tm.has_running("パイプライン実行")
show_task_progress("pipeline_task_id", "pipeline_result", tm)

# 前回の結果表示
pipeline_result = st.session_state.get("pipeline_result")
if pipeline_result is not None:
    if pipeline_result["status"] == "SUCCESS":
        st.success(
            f"パイプライン完了: {pipeline_result['races_found']}レース / "
            f"{pipeline_result['total_bets']}ベット / "
            f"{pipeline_result['total_stake']:,}円"
        )
    elif pipeline_result["status"] == "PARTIAL":
        st.warning(
            f"一部エラー: {pipeline_result['races_found']}レース / "
            f"{pipeline_result['total_bets']}ベット / "
            f"エラー{len(pipeline_result['errors'])}件"
        )
    else:
        errors = pipeline_result.get("errors", [])
        st.error(f"失敗: {', '.join(errors[:3])}")

    if st.button("結果をクリア", key="btn_pipeline_clear"):
        del st.session_state["pipeline_result"]
        st.rerun()

with st.form("pipeline_manual_form"):
    target_date = st.date_input("対象日", value=datetime.now().date())
    btn_label = "実行中..." if is_running else "パイプライン実行 (dryrun)"
    submitted = st.form_submit_button(btn_label, disabled=is_running)

    if submitted:
        # dryrunモード強制のconfigを構築（メインスレッドで）
        run_config = dict(config)
        run_config.setdefault("betting", {})["method"] = "dryrun"
        task_id = tm.submit(
            name="パイプライン実行",
            target=_run_pipeline_bg,
            kwargs={
                "jvlink_db_path": _jvlink_db_path,
                "ext_db_path": _ext_db_path,
                "run_config": run_config,
                "target_date": target_date.strftime("%Y%m%d"),
            },
        )
        st.session_state["pipeline_task_id"] = task_id
        st.rerun()
