"""JVLink同期マネージャ。

JVLinkToSQLiteの実行を管理し、同期ログをdata_sync_logテーブルに記録する。
外部実行ファイル(JVLinkToSQLite.exe)のラッパーとして動作する。
"""

import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager
from src.data.validator import DataValidator


class JVLinkSyncManager:
    """JVLinkToSQLiteとの同期を管理するクラス。

    JVLinkToSQLite.exeの実行、同期結果のログ記録、
    データ検証を一元的に管理する。
    """

    def __init__(
        self,
        jvlink_db: DatabaseManager,
        ext_db: DatabaseManager,
        exe_path: str = "",
        enable_setup_data: bool = False,
    ) -> None:
        """
        Args:
            jvlink_db: JVLink DBマネージャ
            ext_db: 拡張DBマネージャ（sync_log記録用）
            exe_path: JVLinkToSQLite.exeのパス（空の場合は手動同期モード）
            enable_setup_data: Trueの場合SetupData（全履歴一括DL）を有効化。
                初回セットアップ時のみTrue。通常の差分更新ではFalse。
        """
        self._jvlink_db = jvlink_db
        self._ext_db = ext_db
        self._exe_path = exe_path
        self._enable_setup_data = enable_setup_data
        self._validator = DataValidator(jvlink_db)

    def run_sync(self, timeout_sec: int = 600) -> dict[str, Any]:
        """JVLinkToSQLiteを実行してデータを同期する。

        exe_pathが未設定の場合はスキップし、検証のみ行う。

        Args:
            timeout_sec: 実行タイムアウト（秒）

        Returns:
            {"sync_id", "status", "records_added", "error_message",
             "validation"} のdict
        """
        now = datetime.now(UTC).isoformat()

        # 同期ログ開始
        sync_id = self._start_sync_log(now)

        if not self._exe_path:
            logger.info("JVLinkToSQLite.exeパス未設定 — 手動同期モード（検証のみ）")
            validation = self._run_validation()
            self._finish_sync_log(sync_id, "SKIPPED", 0, "exe_path未設定")
            return {
                "sync_id": sync_id,
                "status": "SKIPPED",
                "exit_code": -1,
                "records_added": 0,
                "error_message": "exe_path未設定（手動同期モード）",
                "validation": validation,
            }

        # 実行前のレコード数取得
        before_counts = self._get_record_counts()

        # JVLinkToSQLite実行
        exit_code = -1
        error_msg = ""
        stdout_text = ""
        try:
            exe = Path(self._exe_path).resolve()
            if not exe.exists():
                raise FileNotFoundError(f"実行ファイルが見つかりません: {exe}")

            setting_xml = exe.parent / "setting.xml"
            if self._enable_setup_data:
                self._set_setup_data(setting_xml, enabled=True)
            else:
                # 通常更新: SetupDataを無効化してダイアログ表示を防止
                self._set_setup_data(setting_xml, enabled=False)

            # exeのディレクトリをcwdに設定（DLL依存解決のため）
            exe_dir = exe.parent

            # JVLink DBの絶対パスを取得（-d パラメータ用）
            db_path = Path(self._jvlink_db.db_path).resolve()

            cmd = [
                str(exe),
                "-m", "Exec",
                "-d", str(db_path),
                "-l", "Info",
            ]
            logger.info(f"JVLinkToSQLite実行開始: {' '.join(cmd)}")
            logger.info(f"  cwd: {exe_dir}")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="cp932",
                errors="replace",
                timeout=timeout_sec,
                check=False,
                cwd=str(exe_dir),
            )
            exit_code = result.returncode
            stdout_text = result.stdout[:2000] if result.stdout else ""
            if stdout_text:
                logger.info(f"JVLinkToSQLite stdout:\n{stdout_text}")
            if exit_code != 0:
                error_msg = result.stderr[:500] if result.stderr else f"exit_code={exit_code}"
                if stdout_text and not error_msg.strip():
                    error_msg = f"exit_code={exit_code} stdout={stdout_text[:300]}"
                logger.warning(f"JVLinkToSQLite異常終了: {error_msg}")

        except subprocess.TimeoutExpired:
            error_msg = f"タイムアウト({timeout_sec}秒)"
            logger.error(error_msg)
        except FileNotFoundError as e:
            error_msg = str(e)
            logger.error(error_msg)
        except Exception as e:
            error_msg = f"実行エラー: {e}"
            logger.error(error_msg)

        # 実行後のレコード数差分
        after_counts = self._get_record_counts()
        records_added = sum(after_counts.values()) - sum(before_counts.values())
        records_added = max(0, records_added)

        # 検証
        validation = self._run_validation()

        # ステータス判定
        if exit_code == 0:
            status = "SUCCESS"
        elif error_msg:
            status = "FAILED"
        else:
            status = "UNKNOWN"

        self._finish_sync_log(sync_id, status, records_added, error_msg, exit_code)

        logger.info(
            f"同期完了: status={status} records_added={records_added} "
            f"exit_code={exit_code}"
        )

        return {
            "sync_id": sync_id,
            "status": status,
            "exit_code": exit_code,
            "records_added": records_added,
            "error_message": error_msg,
            "stdout": stdout_text,
            "validation": validation,
        }

    def get_sync_history(self, limit: int = 20) -> list[dict[str, Any]]:
        """同期履歴を取得する。

        Args:
            limit: 取得件数上限

        Returns:
            同期ログのリスト（新しい順）
        """
        if not self._ext_db.table_exists("data_sync_log"):
            return []
        return self._ext_db.execute_query(
            "SELECT * FROM data_sync_log ORDER BY sync_id DESC LIMIT ?",
            (limit,),
        )

    def get_last_sync(self) -> dict[str, Any] | None:
        """最新の同期情報を取得する。"""
        history = self.get_sync_history(limit=1)
        return history[0] if history else None

    def _run_validation(self) -> dict[str, Any]:
        """データ検証を実行する。"""
        try:
            return self._validator.run_full_check()
        except Exception as e:
            logger.error(f"データ検証エラー: {e}")
            return {"error": str(e)}

    @staticmethod
    def _set_setup_data(setting_xml: Path, *, enabled: bool) -> None:
        """setting.xmlのJVSetupDataUpdateSetting/IsEnabledを設定する。

        enabled=True: 全履歴一括DL（初回セットアップ用）
        enabled=False: 通常差分更新のみ（セットアップダイアログ非表示）
        """
        if not setting_xml.exists():
            logger.warning(f"setting.xml not found: {setting_xml}")
            return
        try:
            text = setting_xml.read_text(encoding="utf-8")
            target = "true" if enabled else "false"
            current = "false" if enabled else "true"
            new_text = re.sub(
                rf"(<JVSetupDataUpdateSetting>\s*<IsEnabled>){current}(</IsEnabled>)",
                rf"\1{target}\2",
                text,
                count=1,
            )
            if new_text != text:
                setting_xml.write_text(new_text, encoding="utf-8")
                logger.info(f"setting.xml: SetupData {current} → {target}")
            else:
                logger.debug(f"setting.xml: SetupData already {target}")
        except Exception as e:
            logger.warning(f"setting.xml更新エラー（続行します）: {e}")

    def _get_record_counts(self) -> dict[str, int]:
        """主要テーブルのレコード数を取得する。"""
        counts: dict[str, int] = {}
        for table in ["NL_RA_RACE", "NL_SE_RACE_UMA", "NL_HR_PAY"]:
            if self._jvlink_db.table_exists(table):
                rows = self._jvlink_db.execute_query(f"SELECT COUNT(*) as cnt FROM [{table}]")
                counts[table] = rows[0]["cnt"] if rows else 0
            else:
                counts[table] = 0
        return counts

    def _start_sync_log(self, started_at: str) -> int:
        """同期ログを開始する。"""
        if not self._ext_db.table_exists("data_sync_log"):
            logger.warning("data_sync_logテーブルが存在しません")
            return 0
        try:
            self._ext_db.execute_write(
                "INSERT INTO data_sync_log (started_at, status) VALUES (?, 'RUNNING')",
                (started_at,),
            )
            rows = self._ext_db.execute_query(
                "SELECT MAX(sync_id) as last_id FROM data_sync_log"
            )
            return rows[0]["last_id"] if rows and rows[0]["last_id"] else 0
        except Exception as e:
            logger.error(f"同期ログ開始エラー: {e}")
            return 0

    def _finish_sync_log(
        self,
        sync_id: int,
        status: str,
        records_added: int,
        error_message: str,
        exit_code: int | None = None,
    ) -> None:
        """同期ログを完了する。"""
        if sync_id == 0:
            return
        now = datetime.now(UTC).isoformat()
        try:
            self._ext_db.execute_write(
                """UPDATE data_sync_log
                   SET finished_at = ?, status = ?, records_added = ?,
                       error_message = ?, exit_code = ?
                   WHERE sync_id = ?""",
                (now, status, records_added, error_message, exit_code, sync_id),
            )
        except Exception as e:
            logger.error(f"同期ログ更新エラー: {e}")
