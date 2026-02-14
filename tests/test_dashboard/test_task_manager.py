"""TaskManager のユニットテスト。"""

import threading
import time

import pytest

from src.dashboard.task_manager import TaskManager, TaskProgress, TaskStatus


class TestTaskProgress:
    """TaskProgress dataclass のテスト。"""

    def test_percent_zero(self):
        tp = TaskProgress(task_id="t1", name="test", total=0)
        assert tp.percent == 0.0

    def test_percent_half(self):
        tp = TaskProgress(task_id="t1", name="test", current=50, total=100)
        assert tp.percent == pytest.approx(0.5)

    def test_percent_capped(self):
        tp = TaskProgress(task_id="t1", name="test", current=200, total=100)
        assert tp.percent == 1.0

    def test_elapsed_not_started(self):
        tp = TaskProgress(task_id="t1", name="test")
        assert tp.elapsed_sec == 0.0


class TestTaskManager:
    """TaskManager のテスト。"""

    def test_submit_and_complete(self):
        """タスクを投入し完了まで待つ。"""
        tm = TaskManager()

        def add(a, b, progress_callback=None):
            if progress_callback:
                progress_callback(1, 1, "done")
            return a + b

        task_id = tm.submit("add", add, args=(2, 3))
        # 完了を待つ
        for _ in range(50):
            p = tm.get_progress(task_id)
            if p and p.status == TaskStatus.COMPLETED:
                break
            time.sleep(0.05)

        p = tm.get_progress(task_id)
        assert p is not None
        assert p.status == TaskStatus.COMPLETED
        assert p.result == 5
        assert p.error == ""

    def test_submit_failure(self):
        """タスクが例外を送出したら FAILED になる。"""
        tm = TaskManager()

        def fail(progress_callback=None):
            raise ValueError("test error")

        task_id = tm.submit("fail", fail)
        for _ in range(50):
            p = tm.get_progress(task_id)
            if p and p.status == TaskStatus.FAILED:
                break
            time.sleep(0.05)

        p = tm.get_progress(task_id)
        assert p is not None
        assert p.status == TaskStatus.FAILED
        assert "test error" in p.error

    def test_has_running(self):
        """実行中タスクの検出。"""
        tm = TaskManager()
        event = threading.Event()

        def slow(progress_callback=None):
            event.wait(5)
            return "ok"

        tm.submit("slow", slow)
        time.sleep(0.1)
        assert tm.has_running("slow") is True
        assert tm.has_running("other") is False

        event.set()
        for _ in range(50):
            if not tm.has_running("slow"):
                break
            time.sleep(0.05)
        assert tm.has_running("slow") is False

    def test_get_active_tasks(self):
        """アクティブタスクの取得。"""
        tm = TaskManager()
        event = threading.Event()

        def wait(progress_callback=None):
            event.wait(5)

        tm.submit("w1", wait)
        tm.submit("w2", wait)
        time.sleep(0.1)

        active = tm.get_active_tasks()
        assert len(active) == 2

        event.set()
        time.sleep(0.2)
        assert len(tm.get_active_tasks()) == 0

    def test_unnotified_completed(self):
        """完了通知の管理。"""
        tm = TaskManager()

        def noop(progress_callback=None):
            return 42

        tm.submit("noop", noop)
        time.sleep(0.2)

        unnotified = tm.get_unnotified_completed()
        assert len(unnotified) == 1
        assert unnotified[0].result == 42

        # 2回目は空
        assert len(tm.get_unnotified_completed()) == 0

    def test_clear_completed(self):
        """完了タスクのクリア。"""
        tm = TaskManager()

        def noop(progress_callback=None):
            return 1

        tm.submit("t1", noop)
        time.sleep(0.2)

        assert len(tm.get_all_tasks()) == 1
        tm.clear_completed()
        assert len(tm.get_all_tasks()) == 0

    def test_progress_callback_injection(self):
        """progress_callback が自動注入される。"""
        tm = TaskManager()

        def tracked(progress_callback=None):
            if progress_callback:
                progress_callback(1, 3, "step 1")
                progress_callback(2, 3, "step 2")
                progress_callback(3, 3, "step 3")
            return "done"

        task_id = tm.submit("tracked", tracked)
        time.sleep(0.3)

        p = tm.get_progress(task_id)
        assert p is not None
        assert p.status == TaskStatus.COMPLETED
        assert p.current == 3
        assert p.total == 3
        assert p.message == "step 3"
