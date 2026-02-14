"""config_loaderモジュールのテスト。"""

from pathlib import Path

from src.dashboard.config_loader import get_db_managers, load_config


class TestLoadConfig:
    """load_config関数のテスト。"""

    def test_load_existing_yaml(self, tmp_path: Path) -> None:
        """存在するYAMLファイルを読み込む。"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "database:\n  jvlink_db_path: test.db\n",
            encoding="utf-8",
        )
        result = load_config(config_file)
        assert result["database"]["jvlink_db_path"] == "test.db"

    def test_load_nonexistent_returns_empty(self, tmp_path: Path) -> None:
        """存在しないファイルの場合は空dictを返す。"""
        result = load_config(tmp_path / "nonexistent.yaml")
        assert result == {}

    def test_load_empty_yaml(self, tmp_path: Path) -> None:
        """空のYAMLファイルは空dictを返す。"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("", encoding="utf-8")
        result = load_config(config_file)
        assert result == {}


class TestGetDbManagers:
    """get_db_managers関数のテスト。"""

    def test_fallback_when_no_config(self, tmp_path: Path) -> None:
        """config未指定の場合はフォールバックDBを使用する。"""
        # フォールバックDBが存在しない場合でもエラーにならない
        jvlink, ext = get_db_managers({})
        assert jvlink is not None
        assert ext is not None

    def test_custom_db_paths(self, tmp_path: Path) -> None:
        """カスタムDBパスを指定した場合。"""
        db_file = tmp_path / "test.db"
        db_file.touch()
        config = {
            "database": {
                "jvlink_db_path": str(db_file),
                "extension_db_path": str(db_file),
                "wal_mode": False,
            }
        }
        jvlink, ext = get_db_managers(config)
        assert jvlink is not None
        assert ext is not None
