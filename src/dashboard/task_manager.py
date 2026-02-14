"""バックグラウンドタスク管理モジュール。

ページ遷移してもタスクが中断されないよう、threading.Thread で
バックグラウンド実行し、進捗を session_state 経由で UI に伝える。
"""

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from loguru import logger


class TaskStatus(Enum):
    """タスク実行ステータス。"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class TaskProgress:
    """タスクの進捗情報（スレッド間共有）。"""

    task_id: str
    name: str
    status: TaskStatus = TaskStatus.PENDING
    current: int = 0
    total: int = 100
    message: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result: Any = None
    error: str = ""
    _notified: bool = field(default=False, repr=False)

    @property
    def percent(self) -> float:
        """進捗率（0.0 〜 1.0）を返す。"""
        if self.total <= 0:
            return 0.0
        return min(self.current / self.total, 1.0)

    @property
    def elapsed_sec(self) -> float:
        """経過秒数を返す。"""
        if self.started_at is None:
            return 0.0
        end = self.finished_at or datetime.now()
        return (end - self.started_at).total_seconds()


class TaskManager:
    """バックグラウンドタスクの管理クラス。

    st.session_state["task_manager"] に格納して使う。
    threading.Thread で実行し、進捗を TaskProgress 経由で共有する。
    """

    def __init__(self) -> None:
        self._tasks: dict[str, TaskProgress] = {}
        self._lock = threading.Lock()

    def submit(
        self,
        name: str,
        target: Callable[..., Any],
        args: tuple = (),
        kwargs: dict[str, Any] | None = None,
    ) -> str:
        """タスクをバックグラウンドで実行する。

        target は `progress_callback(current, total, message)` を
        キーワード引数として受け取る必要がある。
        TaskManager が自動的にコールバックを注入する。

        Args:
            name: タスク表示名（例: "Weight最適化"）
            target: 実行する関数
            args: 位置引数
            kwargs: キーワード引数

        Returns:
            task_id（進捗照会用）
        """
        task_id = uuid4().hex[:8]
        progress = TaskProgress(task_id=task_id, name=name)

        with self._lock:
            self._tasks[task_id] = progress

        def _wrapper() -> None:
            progress.status = TaskStatus.RUNNING
            progress.started_at = datetime.now()
            try:
                kw = dict(kwargs or {})
                kw["progress_callback"] = self._make_callback(task_id)
                result = target(*args, **kw)
                progress.result = result
                progress.status = TaskStatus.COMPLETED
                logger.info(f"タスク完了: {name} ({task_id})")
            except Exception as e:
                progress.error = str(e)
                progress.status = TaskStatus.FAILED
                logger.error(f"タスク失敗: {name} ({task_id}): {e}")
            finally:
                progress.finished_at = datetime.now()

        thread = threading.Thread(target=_wrapper, daemon=True, name=f"task-{task_id}")
        thread.start()
        logger.info(f"タスク開始: {name} ({task_id})")
        return task_id

    def _make_callback(self, task_id: str) -> Callable[[int, int, str], None]:
        """進捗更新用コールバックを生成する。"""

        def callback(current: int, total: int, message: str = "") -> None:
            with self._lock:
                if task_id in self._tasks:
                    t = self._tasks[task_id]
                    t.current = current
                    t.total = total
                    if message:
                        t.message = message

        return callback

    def get_progress(self, task_id: str) -> TaskProgress | None:
        """指定タスクの進捗情報を取得する。"""
        with self._lock:
            return self._tasks.get(task_id)

    def get_all_tasks(self) -> list[TaskProgress]:
        """全タスクのリストを返す（新しい順）。"""
        with self._lock:
            return sorted(
                self._tasks.values(),
                key=lambda t: t.started_at or datetime.min,
                reverse=True,
            )

    def get_active_tasks(self) -> list[TaskProgress]:
        """実行中のタスクのみ返す。"""
        with self._lock:
            return [
                t
                for t in self._tasks.values()
                if t.status in (TaskStatus.PENDING, TaskStatus.RUNNING)
            ]

    def has_running(self, name: str) -> bool:
        """指定名のタスクが実行中かどうか。"""
        with self._lock:
            return any(
                t.name == name
                and t.status in (TaskStatus.PENDING, TaskStatus.RUNNING)
                for t in self._tasks.values()
            )

    def get_unnotified_completed(self) -> list[TaskProgress]:
        """未通知の完了/失敗タスクを返し、通知済みにする。"""
        with self._lock:
            result = [
                t
                for t in self._tasks.values()
                if t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED)
                and not t._notified
            ]
            for t in result:
                t._notified = True
            return result

    def has_pending_notifications(self) -> bool:
        """未通知の完了/失敗タスクがあるか確認する。"""
        with self._lock:
            return any(
                t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED)
                and not t._notified
                for t in self._tasks.values()
            )

    def clear_completed(self) -> None:
        """完了・失敗タスクをクリアする。"""
        with self._lock:
            self._tasks = {
                k: v
                for k, v in self._tasks.items()
                if v.status in (TaskStatus.PENDING, TaskStatus.RUNNING)
            }
