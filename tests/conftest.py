"""pytest共通フィクスチャ。"""

from pathlib import Path

import pytest

from src.data.db import DatabaseManager


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> str:
    """一時的なSQLite DBパスを返す。"""
    return str(tmp_path / "test.db")


@pytest.fixture
def db_manager(tmp_db_path: str) -> DatabaseManager:
    """テスト用DatabaseManagerを返す。"""
    return DatabaseManager(tmp_db_path, wal_mode=False)


@pytest.fixture
def initialized_db(tmp_db_path: str) -> DatabaseManager:
    """拡張テーブル初期化済みのDatabaseManagerを返す。"""
    from scripts.init_db import init_extension_tables

    init_extension_tables(tmp_db_path)
    return DatabaseManager(tmp_db_path, wal_mode=False)


@pytest.fixture
def sample_race_key() -> str:
    """テスト用race_keyを返す。"""
    return "2025010106010101"  # Year=2025, MonthDay=0101, JyoCD=06, Kaiji=01, Nichiji=01, RaceNum=01
