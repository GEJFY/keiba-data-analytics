"""ダッシュボードテスト用conftest。

ページモジュールはモジュールレベルでStreamlit APIを呼び出すため、
インポート前にsession_stateをモックする必要がある。
"""

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.data.db import DatabaseManager


def _make_mock_db(tmp_path: Path) -> DatabaseManager:
    """テスト用ダミーDBを作成する。"""
    return DatabaseManager(str(tmp_path / "mock_st.db"), wal_mode=False)


@pytest.fixture(autouse=True)
def mock_streamlit_for_pages(tmp_path: Path, monkeypatch):
    """Streamlit session_stateをモックし、ページモジュールを再読込可能にする。"""
    dummy_db = _make_mock_db(tmp_path)

    from src.dashboard.task_manager import TaskManager

    # session_stateのモック（dict-likeアクセスとattr-likeアクセスの両方対応）
    mock_state = MagicMock()
    mock_state.jvlink_db = dummy_db
    mock_state.ext_db = dummy_db
    mock_state.config = {}
    mock_state.task_manager = TaskManager()
    mock_state.get.return_value = None
    mock_state.__contains__ = lambda self, key: key in (
        "jvlink_db", "ext_db", "config", "task_manager",
    )

    import streamlit as st
    monkeypatch.setattr(st, "session_state", mock_state)

    # ウィジェット関数のモック（テスト環境で安全に動作するよう False / 空を返す）
    # st.button, st.form_submit_button 等が truthy な MagicMock を返すと
    # 意図しないコードパスが実行されてしまう対策
    _noop = MagicMock(return_value=None)
    _false = MagicMock(return_value=False)

    monkeypatch.setattr(st, "button", _false)
    monkeypatch.setattr(st, "form_submit_button", _false)
    monkeypatch.setattr(st, "rerun", MagicMock())

    # キャッシュされたページモジュールを除去して再インポートを強制
    page_modules = [
        k for k in sys.modules
        if k.startswith("src.dashboard.pages.page_")
    ]
    for mod_name in page_modules:
        del sys.modules[mod_name]

    yield mock_state
