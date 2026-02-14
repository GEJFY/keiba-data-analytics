"""ダッシュボード設定読込モジュール。

config/config.yaml からDB接続パス等を読み込み、
DatabaseManagerインスタンスを生成する。
"""

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
    if jvlink_path:
        jvlink_resolved = (PROJECT_ROOT / jvlink_path).resolve()
    else:
        jvlink_resolved = FALLBACK_DB_PATH
    if not jvlink_resolved.exists():
        logger.warning(
            f"JVLink DBが見つかりません: {jvlink_resolved} — "
            f"フォールバック: {FALLBACK_DB_PATH}"
        )
        jvlink_resolved = FALLBACK_DB_PATH

    # 拡張DB
    ext_path = db_config.get("extension_db_path", "")
    if ext_path:
        ext_resolved = (PROJECT_ROOT / ext_path).resolve()
    else:
        ext_resolved = FALLBACK_DB_PATH
    if not ext_resolved.exists():
        logger.warning(
            f"拡張DBが見つかりません: {ext_resolved} — "
            f"フォールバック: {FALLBACK_DB_PATH}"
        )
        ext_resolved = FALLBACK_DB_PATH

    wal = db_config.get("wal_mode", True)
    return (
        DatabaseManager(str(jvlink_resolved), wal_mode=wal),
        DatabaseManager(str(ext_resolved), wal_mode=wal),
    )
