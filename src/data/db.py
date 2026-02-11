"""SQLite接続・クエリ実行モジュール。

JVLinkToSQLiteが生成するDBおよび拡張テーブルDBへの
接続管理とクエリ実行を提供する。
WALモード、自動コミット/ロールバック、dict形式結果変換を標準装備。
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from loguru import logger


class DatabaseError(Exception):
    """データベース操作に関するアプリケーション例外。"""


class DatabaseManager:
    """SQLiteデータベース接続管理クラス。

    スレッドセーフではないため、マルチスレッド利用時は
    スレッドごとにインスタンスを生成すること。
    """

    def __init__(self, db_path: str, wal_mode: bool = True) -> None:
        self._db_path = Path(db_path)
        self._wal_mode = wal_mode
        if not self._db_path.exists():
            logger.warning(f"DBファイルが存在しません（初回接続時に自動生成）: {self._db_path}")

    @property
    def db_path(self) -> Path:
        """DBファイルパスを返す。"""
        return self._db_path

    @contextmanager
    def connect(self) -> Generator[sqlite3.Connection, None, None]:
        """DB接続のコンテキストマネージャ。

        正常終了時にcommit、例外発生時にrollbackを自動実行する。
        """
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        if self._wal_mode:
            conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"DB操作エラー（ロールバック実行）: {e}")
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute_query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """SELECTクエリを実行し、結果をdict形式で返す。

        Args:
            sql: SELECT文（パラメータプレースホルダ ? を使用）
            params: バインドパラメータのタプル

        Returns:
            dictのリスト（各dictがカラム名→値のマッピング）
        """
        with self.connect() as conn:
            cursor = conn.execute(sql, params)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_write(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        """INSERT/UPDATE/DELETEクエリを実行し、影響行数を返す。

        Args:
            sql: DML文（パラメータプレースホルダ ? を使用）
            params: バインドパラメータのタプル

        Returns:
            影響を受けた行数
        """
        with self.connect() as conn:
            cursor = conn.execute(sql, params)
            return cursor.rowcount

    def table_exists(self, table_name: str) -> bool:
        """テーブルの存在を確認する。

        Args:
            table_name: 確認対象のテーブル名

        Returns:
            テーブルが存在する場合True
        """
        result = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return len(result) > 0
