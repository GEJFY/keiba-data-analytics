"""DatabaseManagerの単体テスト。"""

import sqlite3

import pytest

from src.data.db import DatabaseManager


class TestDatabaseManager:
    """DatabaseManagerクラスのテスト。"""

    def test_connect_creates_db_file(self, tmp_db_path: str) -> None:
        """接続時にDBファイルが自動生成されること。"""
        db = DatabaseManager(tmp_db_path, wal_mode=False)
        with db.connect() as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
        # ファイルが作成されていること
        from pathlib import Path
        assert Path(tmp_db_path).exists()

    def test_execute_query_returns_dict_list(self, db_manager: DatabaseManager) -> None:
        """execute_queryがdict形式のリストを返すこと。"""
        with db_manager.connect() as conn:
            conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO items VALUES (1, 'alpha')")
            conn.execute("INSERT INTO items VALUES (2, 'beta')")

        results = db_manager.execute_query("SELECT * FROM items ORDER BY id")
        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "alpha"}
        assert results[1] == {"id": 2, "name": "beta"}

    def test_execute_query_with_params(self, db_manager: DatabaseManager) -> None:
        """パラメータ付きクエリが正しく動作すること。"""
        with db_manager.connect() as conn:
            conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO items VALUES (1, 'alpha')")
            conn.execute("INSERT INTO items VALUES (2, 'beta')")

        results = db_manager.execute_query("SELECT * FROM items WHERE id = ?", (1,))
        assert len(results) == 1
        assert results[0]["name"] == "alpha"

    def test_execute_query_empty_result(self, db_manager: DatabaseManager) -> None:
        """結果が空の場合、空リストを返すこと。"""
        with db_manager.connect() as conn:
            conn.execute("CREATE TABLE items (id INTEGER)")

        results = db_manager.execute_query("SELECT * FROM items")
        assert results == []

    def test_execute_write_returns_rowcount(self, db_manager: DatabaseManager) -> None:
        """execute_writeが影響行数を返すこと。"""
        with db_manager.connect() as conn:
            conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO items VALUES (1, 'alpha')")
            conn.execute("INSERT INTO items VALUES (2, 'beta')")

        affected = db_manager.execute_write("UPDATE items SET name = 'gamma' WHERE id = 1")
        assert affected == 1

    def test_execute_write_multiple_rows(self, db_manager: DatabaseManager) -> None:
        """複数行に影響するUPDATEの行数が正しいこと。"""
        with db_manager.connect() as conn:
            conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO items VALUES (1, 'alpha')")
            conn.execute("INSERT INTO items VALUES (2, 'beta')")
            conn.execute("INSERT INTO items VALUES (3, 'gamma')")

        affected = db_manager.execute_write("DELETE FROM items WHERE id > 1")
        assert affected == 2

    def test_table_exists_true(self, db_manager: DatabaseManager) -> None:
        """存在するテーブルに対してTrueを返すこと。"""
        with db_manager.connect() as conn:
            conn.execute("CREATE TABLE items (id INTEGER)")

        assert db_manager.table_exists("items") is True

    def test_table_exists_false(self, db_manager: DatabaseManager) -> None:
        """存在しないテーブルに対してFalseを返すこと。"""
        assert db_manager.table_exists("nonexistent_table") is False

    def test_connect_rollback_on_error(self, db_manager: DatabaseManager) -> None:
        """例外発生時にロールバックされること。"""
        with db_manager.connect() as conn:
            conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")

        with pytest.raises(sqlite3.IntegrityError), db_manager.connect() as conn:
            conn.execute("INSERT INTO items VALUES (1)")
            conn.execute("INSERT INTO items VALUES (1)")  # 重複エラー

        # ロールバックされているので、レコードは0件
        results = db_manager.execute_query("SELECT COUNT(*) as cnt FROM items")
        assert results[0]["cnt"] == 0

    def test_wal_mode_enabled(self, tmp_db_path: str) -> None:
        """WALモードが有効化されること。"""
        db = DatabaseManager(tmp_db_path, wal_mode=True)
        with db.connect() as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            cursor = conn.execute("PRAGMA journal_mode")
            mode = cursor.fetchone()[0]
            assert mode == "wal"

    def test_nonexistent_db_path_warning(self, tmp_path: str) -> None:
        """存在しないパスでも警告のみでインスタンス化できること。"""
        # コンストラクタで例外が発生しないこと
        db = DatabaseManager("/nonexistent/path/test.db", wal_mode=False)
        assert db is not None
