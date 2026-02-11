"""SQLite接続・クエリ実行モジュール。

JVLinkToSQLiteが生成するDBおよび拡張テーブルDBへの
接続管理とクエリ実行を提供する。
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from loguru import logger


class DatabaseManager:
    """SQLiteデータベース接続管理クラス。"""

    def __init__(self, db_path: str, wal_mode: bool = True) -> None:
        self._db_path = Path(db_path)
        self._wal_mode = wal_mode
        if not self._db_path.exists():
            logger.warning(f"DBファイルが存在しません: {self._db_path}")

    @contextmanager
    def connect(self) -> Generator[sqlite3.Connection, None, None]:
        """DB接続のコンテキストマネージャ。"""
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        if self._wal_mode:
            conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute_query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """SELECTクエリを実行し、結果をdict形式で返す。"""
        with self.connect() as conn:
            cursor = conn.execute(sql, params)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_write(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        """INSERT/UPDATE/DELETEクエリを実行し、影響行数を返す。"""
        with self.connect() as conn:
            cursor = conn.execute(sql, params)
            return cursor.rowcount

    def table_exists(self, table_name: str) -> bool:
        """テーブルの存在を確認する。"""
        result = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return len(result) > 0
