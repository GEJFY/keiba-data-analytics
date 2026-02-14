"""RaceDayPipelineのテスト。"""

import json

import pytest

from src.automation.pipeline import PipelineResult, RaceDayPipeline
from src.data.db import DatabaseManager


def _init_jvlink(db: DatabaseManager) -> None:
    """JVLink DBのテストテーブル + データ。"""
    with db.connect() as conn:
        # レース情報（get_race_info / get_race_listが参照する全カラム）
        conn.execute("""
            CREATE TABLE NL_RA_RACE (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                RaceInfoHondai TEXT, Kyori TEXT, TrackCD TEXT,
                TenkoBabaTenkoCD TEXT, TenkoBabaSibaBabaCD TEXT,
                TenkoBabaDirtBabaCD TEXT,
                JyokenInfoSyubetuCD TEXT, GradeCD TEXT,
                HassoTime TEXT, TorokuTosu TEXT, SyussoTosu TEXT,
                NyusenTosu TEXT, HaronTimeL3 TEXT, HaronTimeL4 TEXT
            )
        """)
        # テストレース 2025/01/05 東京 1R
        conn.execute("""
            INSERT INTO NL_RA_RACE VALUES
            ('2025','0105','05','01','01','01',
             'テストレース','1600','10',
             '1','1','0','11',' ','1000','03','03','03','345','345')
        """)

        # 出走馬
        conn.execute("""
            CREATE TABLE NL_SE_RACE_UMA (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Umaban TEXT, Bamei TEXT, KakuteiJyuni TEXT,
                Wakuban TEXT, SexCD TEXT, Barei TEXT, Futan TEXT,
                KisyuRyakusyo TEXT, ChokyosiRyakusyo TEXT,
                BaTaijyu TEXT, ZogenFugo TEXT,
                ZogenSa TEXT, Ninki TEXT, Odds TEXT, Time TEXT,
                HaronTimeL3 TEXT, HaronTimeL4 TEXT,
                DMJyuni TEXT, KyakusituKubun TEXT,
                Jyuni1c TEXT, Jyuni2c TEXT, Jyuni3c TEXT, Jyuni4c TEXT,
                KettoNum TEXT
            )
        """)
        for uma in ["01", "03", "07"]:
            conn.execute(
                """INSERT INTO NL_SE_RACE_UMA VALUES
                   ('2025','0105','05','01','01','01',?,?||'号馬','0',
                    '1','1','4','55.0','テスト騎手','テスト調教師',
                    '480','+','4','1',
                    '0500','1350','345','345',
                    '1','1','1','1','1','1','000000000')""",
                (uma, uma),
            )

        # オッズ（NL_O1_ODDS_TANFUKUWAKU — provider.get_oddsが使用）
        # OddsTansyoInfo{i}Umaban / OddsTansyoInfo{i}Odds 形式（0〜27）
        cols = ["idYear TEXT", "idMonthDay TEXT", "idJyoCD TEXT",
                "idKaiji TEXT", "idNichiji TEXT", "idRaceNum TEXT"]
        for i in range(28):
            cols.append(f"OddsTansyoInfo{i}Umaban TEXT")
            cols.append(f"OddsTansyoInfo{i}Odds TEXT")
        conn.execute(f"CREATE TABLE NL_O1_ODDS_TANFUKUWAKU ({', '.join(cols)})")

        # 馬番01=3.0倍, 03=5.0倍, 07=12.0倍
        vals = ["'2025'", "'0105'", "'05'", "'01'", "'01'", "'01'"]
        odds_data = {0: ("01", "0300"), 1: ("03", "0500"), 2: ("07", "1200")}
        for i in range(28):
            if i in odds_data:
                vals.append(f"'{odds_data[i][0]}'")
                vals.append(f"'{odds_data[i][1]}'")
            else:
                vals.append("'00'")
                vals.append("'0000'")
        conn.execute(
            f"INSERT INTO NL_O1_ODDS_TANFUKUWAKU VALUES ({', '.join(vals)})"
        )

        # 払戻金
        conn.execute("""
            CREATE TABLE NL_HR_PAY (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                PayTansyo0Umaban TEXT, PayTansyo0Pay TEXT, PayTansyo0Ninki TEXT,
                PayFukusyo0Umaban TEXT, PayFukusyo0Pay TEXT, PayFukusyo0Ninki TEXT,
                PayFukusyo1Umaban TEXT, PayFukusyo1Pay TEXT, PayFukusyo1Ninki TEXT,
                PayFukusyo2Umaban TEXT, PayFukusyo2Pay TEXT, PayFukusyo2Ninki TEXT
            )
        """)
        conn.execute("""
            INSERT INTO NL_HR_PAY VALUES
            ('2025','0105','05','01','01','01',
             '03','500','01',
             '03','200','01','01','350','02','07','800','03')
        """)


def _init_ext(db: DatabaseManager) -> None:
    """拡張DBのテストテーブル。"""
    with db.connect() as conn:
        # ファクタールール（init_db.pyと同じスキーマ）
        conn.execute("""
            CREATE TABLE factor_rules (
                rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_name TEXT NOT NULL,
                category TEXT DEFAULT '',
                description TEXT DEFAULT '',
                sql_expression TEXT DEFAULT '',
                weight REAL DEFAULT 1.0,
                validation_score REAL,
                is_active INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source TEXT DEFAULT 'manual',
                effective_from TEXT,
                effective_to TEXT,
                decay_rate REAL,
                min_sample_size INTEGER DEFAULT 100,
                review_status TEXT DEFAULT 'DRAFT',
                reviewed_at TEXT
            )
        """)
        conn.execute("""
            INSERT INTO factor_rules
            (rule_name, category, description, sql_expression, weight,
             is_active, created_at, updated_at, review_status)
            VALUES ('テストルール', 'speed', 'テスト用',
                    'CASE WHEN CAST(Odds AS REAL)/10.0 <= 5.0 THEN 10 ELSE 0 END',
                    1.0, 1, '2025-01-01T00:00:00', '2025-01-01T00:00:00', 'APPROVED')
        """)

        # betsテーブル
        conn.execute("""
            CREATE TABLE bets (
                bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
                race_key TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT '',
                bet_type TEXT NOT NULL,
                selection TEXT NOT NULL,
                stake_yen INTEGER NOT NULL,
                est_prob REAL,
                odds_at_bet REAL,
                est_ev REAL,
                status TEXT DEFAULT 'PENDING',
                executed_at TEXT,
                result TEXT,
                payout_yen INTEGER DEFAULT 0,
                settled_at TEXT,
                factor_details TEXT DEFAULT '{}'
            )
        """)


def _make_config(betting_method: str = "dryrun", enabled: bool = True) -> dict:
    """テスト用設定。"""
    return {
        "database": {
            "jvlink_db_path": "./data/jvlink.db",
            "extension_db_path": "./data/extension.db",
        },
        "scoring": {
            "base_score": 100,
            "ev_threshold": 1.05,
        },
        "bankroll": {
            "initial_balance": 1_000_000,
            "betting_method": "quarter_kelly",
        },
        "betting": {
            "method": betting_method,
            "approval_required": False,
            "csv_output_dir": "./data/ipatgo",
            "max_consecutive_losses": 20,
        },
        "automation": {
            "enabled": enabled,
            "race_days": [],
            "max_races_per_day": 36,
            "auto_reconcile": True,
        },
        "jvlink": {
            "exe_path": "",
            "retry_count": 1,
            "sync_timeout_sec": 10,
        },
        "notification": {
            "min_level": "ERROR",
            "slack_webhook_url": "",
            "slack_channel": "",
            "smtp_host": "",
            "smtp_port": 587,
            "smtp_user": "",
            "smtp_password": "",
            "email_from": "",
            "email_to": [],
        },
    }


@pytest.fixture
def jvlink_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "jvlink.db"), wal_mode=False)
    _init_jvlink(db)
    return db


@pytest.fixture
def ext_db(tmp_path) -> DatabaseManager:
    db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
    _init_ext(db)
    return db


class TestRaceDayPipeline:
    """RaceDayPipelineの単体テスト。"""

    def test_run_full_dryrun(self, jvlink_db, ext_db) -> None:
        """dryrunモードでフルパイプラインが正常完了すること。"""
        config = _make_config("dryrun")
        pipeline = RaceDayPipeline(jvlink_db, ext_db, config)
        result = pipeline.run_full(target_date="20250105")

        assert result.status in ("SUCCESS", "PARTIAL")
        assert result.run_id > 0
        assert result.run_date == "20250105"
        assert result.completed_at != ""

    def test_step_score_and_bet(self, jvlink_db, ext_db) -> None:
        """当日レースのスコアリング+投票が動作すること。"""
        config = _make_config("dryrun")
        pipeline = RaceDayPipeline(jvlink_db, ext_db, config)
        pipeline._ensure_pipeline_table()

        score_result = pipeline.step_score_and_bet("20250105")
        assert score_result["races_found"] == 1
        assert score_result["races_scored"] >= 0

    def test_step_reconcile(self, jvlink_db, ext_db) -> None:
        """結果照合が動作すること。"""
        config = _make_config("dryrun")
        pipeline = RaceDayPipeline(jvlink_db, ext_db, config)

        reconcile_result = pipeline.step_reconcile()
        assert "reconciled" in reconcile_result
        assert reconcile_result["reconciled"] >= 0

    def test_safety_guard_blocks(self, jvlink_db, ext_db) -> None:
        """安全チェック不合格時に投票が停止すること。"""
        config = _make_config("dryrun")
        config["betting"]["max_consecutive_losses"] = 0
        pipeline = RaceDayPipeline(jvlink_db, ext_db, config)
        pipeline._ensure_pipeline_table()

        # 安全機構を手動で緊急停止状態にする
        pipeline._safety._state.is_emergency_stopped = True

        score_result = pipeline.step_score_and_bet("20250105")
        # 安全チェック不合格なので投票なし
        assert score_result["total_bets"] == 0

    def test_no_races_today(self, jvlink_db, ext_db) -> None:
        """当日レースなしの場合にエラーなく完了すること。"""
        config = _make_config("dryrun")
        pipeline = RaceDayPipeline(jvlink_db, ext_db, config)
        result = pipeline.run_full(target_date="20991231")

        assert result.status in ("SUCCESS", "PARTIAL")
        assert result.races_found == 0
        assert result.total_bets == 0

    def test_pipeline_run_recorded(self, jvlink_db, ext_db) -> None:
        """pipeline_runsテーブルに実行記録が保存されること。"""
        config = _make_config("dryrun")
        pipeline = RaceDayPipeline(jvlink_db, ext_db, config)
        result = pipeline.run_full(target_date="20250105")

        rows = ext_db.execute_query(
            "SELECT * FROM pipeline_runs WHERE run_id = ?",
            (result.run_id,),
        )
        assert len(rows) == 1
        assert rows[0]["run_date"] == "20250105"
        assert rows[0]["status"] in ("SUCCESS", "PARTIAL", "FAILED")
        assert rows[0]["completed_at"] is not None

    def test_sync_step_no_exe(self, jvlink_db, ext_db) -> None:
        """JVLinkToSQLite.exe不在時にスキップされること。"""
        config = _make_config("dryrun")
        config["jvlink"]["exe_path"] = ""
        pipeline = RaceDayPipeline(jvlink_db, ext_db, config)

        sync_result = pipeline.step_sync()
        assert sync_result["status"] == "SKIPPED"

    def test_automation_disabled_forces_dryrun(self, jvlink_db, ext_db) -> None:
        """automation.enabled=false時にdryrunモードが強制されること。"""
        config = _make_config("ipatgo", enabled=False)
        pipeline = RaceDayPipeline(jvlink_db, ext_db, config)
        pipeline.run_full(target_date="20250105")

        # dryrunに切り替わっているはず
        assert pipeline._config["betting"]["method"] == "dryrun"

    def test_pipeline_result_errors_json(self, jvlink_db, ext_db) -> None:
        """エラーがJSON形式で保存されること。"""
        config = _make_config("dryrun")
        pipeline = RaceDayPipeline(jvlink_db, ext_db, config)
        result = pipeline.run_full(target_date="20250105")

        rows = ext_db.execute_query(
            "SELECT errors FROM pipeline_runs WHERE run_id = ?",
            (result.run_id,),
        )
        assert len(rows) == 1
        errors = json.loads(rows[0]["errors"])
        assert isinstance(errors, list)


class TestPipelineResult:
    """PipelineResultデータクラスのテスト。"""

    def test_default_values(self) -> None:
        """デフォルト値の確認。"""
        result = PipelineResult()
        assert result.run_id == 0
        assert result.status == "RUNNING"
        assert result.errors == []
        assert result.total_bets == 0

    def test_errors_independent(self) -> None:
        """errorsリストがインスタンス間で独立していること。"""
        r1 = PipelineResult()
        r2 = PipelineResult()
        r1.errors.append("error1")
        assert len(r2.errors) == 0
