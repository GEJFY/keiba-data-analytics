"""サイドバーに表示するバックグラウンドタスク進捗ウィジェット。

ページ共通の `show_task_progress()` と次ステップヒントも提供する。
"""

from __future__ import annotations

import streamlit as st

from src.dashboard.task_manager import TaskManager, TaskStatus

# タスク完了時の次ステップヒント
_NEXT_STEP_MAP: dict[str, str] = {
    "JVLink同期": "→ ファクター分析でファクターの有効性を確認しましょう",
    "重要度分析": "→ Weight最適化を実行しましょう",
    "Weight最適化": "→ DBに反映してキャリブレーター再学習へ",
    "キャリブレーター学習": "→ バックテストでROIを確認しましょう",
    "バックテスト": "→ 収支ページで損益を確認しましょう",
    "パイプライン実行": "→ 収支ページで本日の結果を確認しましょう",
}

# st.balloons() を表示する主要タスク
_CELEBRATION_TASKS: set[str] = {"バックテスト", "Weight最適化", "キャリブレーター学習"}


def show_task_progress(
    task_key: str,
    result_key: str,
    tm: TaskManager,
    next_step_hint: str = "",
) -> bool:
    """バックグラウンドタスクの進捗を表示する（全ページ共通）。

    Args:
        task_key: session_stateに保存されたタスクIDのキー
        result_key: 完了時に結果を保存するキー
        tm: TaskManager インスタンス
        next_step_hint: 完了時に表示する次ステップヒント（空なら自動判定）

    Returns:
        True: タスクが完了して結果が格納された（st.rerun()済み）
        False: 実行中/失敗/タスクなし
    """
    task_id = st.session_state.get(task_key)
    if not task_id:
        return False

    task = tm.get_progress(task_id)
    if task is None:
        del st.session_state[task_key]
        return False

    if task.status == TaskStatus.RUNNING:
        pct = task.percent
        elapsed = task.elapsed_sec
        st.progress(pct, text=f"{task.message} ({elapsed:.0f}秒経過)")
        st.caption("他のタブに移動しても処理は継続します")
        return False

    if task.status == TaskStatus.COMPLETED:
        st.session_state[result_key] = task.result
        # 主要タスク完了時のお祝い演出
        if task.name in _CELEBRATION_TASKS:
            st.balloons()
        del st.session_state[task_key]
        st.rerun()
        return True

    if task.status == TaskStatus.FAILED:
        st.error(f"エラー: {task.error}")
        del st.session_state[task_key]
        return False

    return False


def render_task_sidebar() -> None:
    """サイドバーにアクティブタスク一覧と完了通知を表示する。"""
    tm: TaskManager | None = st.session_state.get("task_manager")
    if tm is None:
        return

    # 未通知の完了/失敗タスクをトースト通知（次ステップヒント付き）
    for task in tm.get_unnotified_completed():
        if task.status == TaskStatus.COMPLETED:
            hint = _NEXT_STEP_MAP.get(task.name, "")
            msg = f"{task.name} が完了しました ({task.elapsed_sec:.0f}秒)"
            if hint:
                msg += f"\n{hint}"
            st.toast(msg, icon="\u2705")
        elif task.status == TaskStatus.FAILED:
            st.toast(f"{task.name} が失敗しました: {task.error[:80]}", icon="\u274c")

    all_tasks = tm.get_all_tasks()
    if not all_tasks:
        return

    active = [t for t in all_tasks if t.status in (TaskStatus.PENDING, TaskStatus.RUNNING)]
    completed = [t for t in all_tasks if t.status == TaskStatus.COMPLETED]
    failed = [t for t in all_tasks if t.status == TaskStatus.FAILED]

    st.sidebar.divider()
    st.sidebar.markdown("#### \u23f3 \u30bf\u30b9\u30af\u72b6\u6cc1")

    # 実行中タスク
    for task in active:
        pct = task.percent
        elapsed = task.elapsed_sec
        time_text = f" ({elapsed:.0f}s)" if elapsed > 0 else ""
        st.sidebar.progress(
            pct,
            text=f"**{task.name}**{time_text}  \n{task.message}",
        )

    # 完了タスク（直近3件）
    for task in completed[:3]:
        st.sidebar.success(
            f"{task.name} \u2014 {task.elapsed_sec:.0f}\u79d2",
            icon="\u2705",
        )

    # 失敗タスク（直近3件）
    for task in failed[:3]:
        st.sidebar.error(
            f"{task.name}: {task.error[:60]}",
            icon="\u274c",
        )

    # クリアボタン
    if completed or failed:
        if st.sidebar.button("\u5b8c\u4e86\u30bf\u30b9\u30af\u3092\u30af\u30ea\u30a2", key="btn_clear_tasks"):
            tm.clear_completed()
            st.rerun()
