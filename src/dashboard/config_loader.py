"""ダッシュボード設定読込モジュール。

config/config.yaml からDB接続パス等を読み込み、
DatabaseManagerインスタンスを生成する。
"""

import sqlite3
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from src.data.db import DatabaseManager

# プロジェクトルート（src/dashboard/ の2階層上）
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
FALLBACK_DB_PATH = PROJECT_ROOT / "data" / "demo.db"


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    """YAML設定ファイルを読み込む。

    Args:
        config_path: 設定ファイルパス。Noneの場合はデフォルトパス。

    Returns:
        設定dict。ファイルが存在しない場合は空dict。
    """
    path = config_path or DEFAULT_CONFIG_PATH
    if not path.exists():
        logger.warning(f"設定ファイルが見つかりません: {path}")
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_db_managers(
    config: dict[str, Any],
) -> tuple[DatabaseManager, DatabaseManager]:
    """設定から2つのDatabaseManagerを生成する。

    Returns:
        (jvlink_db, extension_db) のタプル。
        config.yamlが無い場合はFALLBACK_DB_PATHを両方に使用。
    """
    db_config = config.get("database", {})

    # JVLink DB
    jvlink_path = db_config.get("jvlink_db_path", "")
    jvlink_resolved = (PROJECT_ROOT / jvlink_path).resolve() if jvlink_path else FALLBACK_DB_PATH
    if not jvlink_resolved.exists():
        logger.warning(
            f"JVLink DBが見つかりません: {jvlink_resolved} — "
            f"フォールバック: {FALLBACK_DB_PATH}"
        )
        jvlink_resolved = FALLBACK_DB_PATH

    # 拡張DB
    ext_path = db_config.get("extension_db_path", "")
    ext_resolved = (PROJECT_ROOT / ext_path).resolve() if ext_path else FALLBACK_DB_PATH
    if not ext_resolved.exists():
        logger.warning(
            f"拡張DBが見つかりません: {ext_resolved} — "
            f"フォールバック: {FALLBACK_DB_PATH}"
        )
        ext_resolved = FALLBACK_DB_PATH

    wal = db_config.get("wal_mode", True)

    ext_db = DatabaseManager(str(ext_resolved), wal_mode=wal)
    _ensure_ext_schema(ext_resolved)

    return (
        DatabaseManager(str(jvlink_resolved), wal_mode=wal),
        ext_db,
    )


def _ensure_ext_schema(db_path: Path) -> None:
    """拡張DBのスキーママイグレーションを実行する。

    既存DBに不足カラム・テーブルがあれば追加する。
    """
    if not db_path.exists():
        return
    try:
        conn = sqlite3.connect(str(db_path))
        # factor_rules テーブルが存在する場合のみマイグレーション
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "factor_rules" in tables:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(factor_rules)").fetchall()]
            if "training_from" not in cols:
                conn.execute("ALTER TABLE factor_rules ADD COLUMN training_from TEXT")
            if "training_to" not in cols:
                conn.execute("ALTER TABLE factor_rules ADD COLUMN training_to TEXT")
        # バージョン管理テーブル
        if "rule_set_snapshots" not in tables:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS rule_set_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_label TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    trigger TEXT DEFAULT 'manual',
                    calibrator_path TEXT,
                    calibrator_method TEXT,
                    config_json TEXT,
                    created_at TEXT NOT NULL,
                    created_by TEXT DEFAULT 'user'
                )
            """)
        if "factor_rules_archive" not in tables:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS factor_rules_archive (
                    archive_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER,
                    rule_id INTEGER NOT NULL,
                    rule_name TEXT NOT NULL,
                    category TEXT DEFAULT '',
                    description TEXT DEFAULT '',
                    sql_expression TEXT DEFAULT '',
                    weight REAL DEFAULT 1.0,
                    review_status TEXT DEFAULT 'DRAFT',
                    is_active INTEGER DEFAULT 0,
                    validation_score REAL,
                    decay_rate REAL,
                    min_sample_size INTEGER DEFAULT 100,
                    source TEXT DEFAULT 'manual',
                    training_from TEXT,
                    training_to TEXT,
                    archived_at TEXT NOT NULL,
                    archived_by TEXT DEFAULT 'system',
                    FOREIGN KEY (rule_id) REFERENCES factor_rules(rule_id),
                    FOREIGN KEY (snapshot_id) REFERENCES rule_set_snapshots(snapshot_id)
                )
            """)
        conn.commit()
    except Exception as e:
        logger.warning(f"拡張DBマイグレーション失敗: {e}")
    finally:
        conn.close()
