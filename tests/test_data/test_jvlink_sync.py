"""JVLink同期マネージャのテスト。"""

from pathlib import Path

import pytest

from src.data.db import DatabaseManager
from src.data.jvlink_sync import JVLinkSyncManager


def _init_sync_log(ext_db: DatabaseManager) -> None:
    """data_sync_logテーブルを初期化する。"""
    with ext_db.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_sync_log (
                sync_id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                exit_code INTEGER,
                records_added INTEGER DEFAULT 0,
                status TEXT DEFAULT 'RUNNING',
                error_message TEXT DEFAULT ''
            )
        """)


def _init_jvlink_tables(jvlink_db: DatabaseManager) -> None:
    """JVLink DBにテストテーブルを作成する。"""
    with jvlink_db.connect() as conn:
        conn.execute("""
            CREATE TABLE NL_RA_RACE (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                RaceInfoHondai TEXT
            )
        """)
        conn.execute("""
            INSERT INTO NL_RA_RACE VALUES
            ('2025', '0105', '06', '01', '01', '01', 'テストレース')
        """)
        conn.execute("""
            CREATE TABLE NL_SE_RACE_UMA (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Umaban TEXT, Bamei TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE NL_HR_PAY (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT
            )
        """)


@pytest.fixture
def jvlink_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "jvlink.db"), wal_mode=False)
    _init_jvlink_tables(db)
    return db


@pytest.fixture
def ext_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
    _init_sync_log(db)
    return db


class TestJVLinkSyncManager:
    """JVLinkSyncManagerのテスト。"""

    def test_run_sync_no_exe(self, jvlink_db, ext_db) -> None:
        """exe_path未設定で手動同期モードになること。"""
        mgr = JVLinkSyncManager(jvlink_db, ext_db, exe_path="")
        result = mgr.run_sync()
        assert result["status"] == "SKIPPED"
        assert result["records_added"] == 0
        assert "validation" in result

    def test_run_sync_missing_exe(self, jvlink_db, ext_db, tmp_path) -> None:
        """存在しないexeパスでFAILEDになること。"""
        mgr = JVLinkSyncManager(
            jvlink_db, ext_db,
            exe_path=str(tmp_path / "nonexistent.exe"),
        )
        result = mgr.run_sync()
        assert result["status"] == "FAILED"
        assert "見つかりません" in result["error_message"]

    def test_sync_log_recorded(self, jvlink_db, ext_db) -> None:
        """同期ログがDBに記録されること。"""
        mgr = JVLinkSyncManager(jvlink_db, ext_db, exe_path="")
        mgr.run_sync()
        history = mgr.get_sync_history()
        assert len(history) >= 1
        assert history[0]["status"] == "SKIPPED"

    def test_get_last_sync(self, jvlink_db, ext_db) -> None:
        """最新同期情報を取得できること。"""
        mgr = JVLinkSyncManager(jvlink_db, ext_db, exe_path="")
        mgr.run_sync()
        last = mgr.get_last_sync()
        assert last is not None
        assert last["status"] == "SKIPPED"

    def test_get_last_sync_empty(self, jvlink_db, ext_db) -> None:
        """同期履歴がない場合Noneを返すこと。"""
        mgr = JVLinkSyncManager(jvlink_db, ext_db)
        last = mgr.get_last_sync()
        assert last is None

    def test_get_sync_history_limit(self, jvlink_db, ext_db) -> None:
        """履歴件数制限が機能すること。"""
        mgr = JVLinkSyncManager(jvlink_db, ext_db, exe_path="")
        mgr.run_sync()
        mgr.run_sync()
        mgr.run_sync()
        history = mgr.get_sync_history(limit=2)
        assert len(history) == 2

    def test_no_sync_log_table(self, jvlink_db, tmp_path) -> None:
        """sync_logテーブルがなくてもエラーにならないこと。"""
        bare_db = DatabaseManager(str(tmp_path / "bare.db"), wal_mode=False)
        mgr = JVLinkSyncManager(jvlink_db, bare_db, exe_path="")
        result = mgr.run_sync()
        assert result["sync_id"] == 0
        history = mgr.get_sync_history()
        assert history == []

    def test_enable_setup_data_default_false(self, jvlink_db, ext_db) -> None:
        """enable_setup_dataのデフォルトがFalseであること。"""
        mgr = JVLinkSyncManager(jvlink_db, ext_db)
        assert mgr._enable_setup_data is False

    def test_enable_setup_data_explicit_true(self, jvlink_db, ext_db) -> None:
        """enable_setup_data=Trueが設定されること。"""
        mgr = JVLinkSyncManager(jvlink_db, ext_db, enable_setup_data=True)
        assert mgr._enable_setup_data is True


class TestSetSetupData:
    """_set_setup_dataメソッドのテスト。"""

    _SETTING_XML_TEMPLATE = """\
<?xml version="1.0" encoding="utf-8"?>
<JVLinkToSQLiteSetting>
  <JVSetupDataUpdateSetting>
    <IsEnabled>{value}</IsEnabled>
  </JVSetupDataUpdateSetting>
</JVLinkToSQLiteSetting>"""

    def _write_setting(self, path: Path, enabled: bool) -> None:
        value = "true" if enabled else "false"
        path.write_text(self._SETTING_XML_TEMPLATE.format(value=value), encoding="utf-8")

    def test_enable_setup_data(self, tmp_path) -> None:
        """false → trueに変更されること。"""
        xml = tmp_path / "setting.xml"
        self._write_setting(xml, enabled=False)

        JVLinkSyncManager._set_setup_data(xml, enabled=True)

        text = xml.read_text(encoding="utf-8")
        assert "<IsEnabled>true</IsEnabled>" in text

    def test_disable_setup_data(self, tmp_path) -> None:
        """true → falseに変更されること。"""
        xml = tmp_path / "setting.xml"
        self._write_setting(xml, enabled=True)

        JVLinkSyncManager._set_setup_data(xml, enabled=False)

        text = xml.read_text(encoding="utf-8")
        assert "<IsEnabled>false</IsEnabled>" in text

    def test_already_enabled_no_change(self, tmp_path) -> None:
        """既にtrueの場合は変更なしで正常終了すること。"""
        xml = tmp_path / "setting.xml"
        self._write_setting(xml, enabled=True)
        original = xml.read_text(encoding="utf-8")

        JVLinkSyncManager._set_setup_data(xml, enabled=True)

        assert xml.read_text(encoding="utf-8") == original

    def test_already_disabled_no_change(self, tmp_path) -> None:
        """既にfalseの場合は変更なしで正常終了すること。"""
        xml = tmp_path / "setting.xml"
        self._write_setting(xml, enabled=False)
        original = xml.read_text(encoding="utf-8")

        JVLinkSyncManager._set_setup_data(xml, enabled=False)

        assert xml.read_text(encoding="utf-8") == original

    def test_missing_file_no_error(self, tmp_path) -> None:
        """ファイルが存在しない場合エラーにならないこと。"""
        xml = tmp_path / "nonexistent.xml"
        JVLinkSyncManager._set_setup_data(xml, enabled=True)  # エラーなし
