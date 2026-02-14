"""校正トレーナーのテスト。"""

import numpy as np
import pytest

from src.data.db import DatabaseManager
from src.scoring.calibration_trainer import CalibrationTrainer


def _setup_dbs(tmp_path):
    """テスト用DB群を構築する。"""
    jvlink_db = DatabaseManager(str(tmp_path / "jvlink.db"), wal_mode=False)
    ext_db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)

    # JVLink DB: 出走馬データ
    with jvlink_db.connect() as conn:
        conn.execute("""
            CREATE TABLE NL_SE_RACE_UMA (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Umaban TEXT, Bamei TEXT, KakuteiJyuni TEXT
            )
        """)
        # 100頭分のテストデータ
        for i in range(100):
            race_num = f"{(i // 12) + 1:02d}"
            umaban = f"{(i % 12) + 1:02d}"
            jyuni = str((i % 12) + 1)
            conn.execute(
                "INSERT INTO NL_SE_RACE_UMA VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("2025", "0105", "06", "01", "01", race_num, umaban, f"馬{i}", jyuni),
            )

    # 拡張DB: horse_scores
    with ext_db.connect() as conn:
        conn.execute("""
            CREATE TABLE horse_scores (
                score_id INTEGER PRIMARY KEY AUTOINCREMENT,
                race_key TEXT, umaban TEXT, total_score REAL,
                factor_details TEXT, estimated_prob REAL,
                fair_odds REAL, actual_odds REAL, expected_value REAL,
                strategy_version TEXT, calculated_at TEXT
            )
        """)
        for i in range(100):
            race_num = f"{(i // 12) + 1:02d}"
            umaban = f"{(i % 12) + 1:02d}"
            race_key = f"20250105060101{race_num}"
            # 1着馬ほどスコアが高い傾向をつける
            jyuni = (i % 12) + 1
            score = 120.0 - jyuni * 3.0 + np.random.normal(0, 5)
            conn.execute(
                """INSERT INTO horse_scores
                   (race_key, umaban, total_score, factor_details, calculated_at)
                   VALUES (?, ?, ?, '{}', '2025-01-05T00:00:00')""",
                (race_key, umaban, score),
            )

    return jvlink_db, ext_db


@pytest.fixture
def dbs(tmp_path):
    return _setup_dbs(tmp_path)


class TestCalibrationTrainer:
    """CalibrationTrainerのテスト。"""

    def test_build_training_data(self, dbs) -> None:
        """訓練データ構築。"""
        jvlink_db, ext_db = dbs
        trainer = CalibrationTrainer(jvlink_db, ext_db)
        scores, labels = trainer.build_training_data(target_jyuni=1, min_samples=10)
        assert len(scores) > 0
        assert len(labels) == len(scores)
        assert set(np.unique(labels)).issubset({0, 1})

    def test_build_training_data_insufficient(self, tmp_path) -> None:
        """サンプル不足時のエラー。"""
        jvlink_db = DatabaseManager(str(tmp_path / "jv.db"), wal_mode=False)
        ext_db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
        with ext_db.connect() as conn:
            conn.execute("""
                CREATE TABLE horse_scores (
                    score_id INTEGER PRIMARY KEY, race_key TEXT,
                    umaban TEXT, total_score REAL, factor_details TEXT,
                    estimated_prob REAL, fair_odds REAL, actual_odds REAL,
                    expected_value REAL, strategy_version TEXT, calculated_at TEXT
                )
            """)
        trainer = CalibrationTrainer(jvlink_db, ext_db)
        with pytest.raises(ValueError, match="サンプル数不足"):
            trainer.build_training_data(min_samples=50)

    def test_train_platt(self, dbs) -> None:
        """Plattスケーリングの訓練。"""
        jvlink_db, ext_db = dbs
        trainer = CalibrationTrainer(jvlink_db, ext_db)
        calibrator = trainer.train(method="platt", target_jyuni=3, min_samples=10)
        prob = calibrator.predict_proba(110.0)
        assert 0.0 < prob < 1.0

    def test_train_isotonic(self, dbs) -> None:
        """Isotonic Regressionの訓練。"""
        jvlink_db, ext_db = dbs
        trainer = CalibrationTrainer(jvlink_db, ext_db)
        calibrator = trainer.train(method="isotonic", target_jyuni=3, min_samples=10)
        prob = calibrator.predict_proba(110.0)
        assert 0.0 <= prob <= 1.0

    def test_train_invalid_method(self, dbs) -> None:
        """不明な校正方法でエラー。"""
        jvlink_db, ext_db = dbs
        trainer = CalibrationTrainer(jvlink_db, ext_db)
        with pytest.raises(ValueError, match="不明な校正方法"):
            trainer.train(method="invalid", min_samples=10)

    def test_evaluate_calibration(self, dbs) -> None:
        """校正モデル評価。"""
        jvlink_db, ext_db = dbs
        trainer = CalibrationTrainer(jvlink_db, ext_db)
        calibrator = trainer.train(method="platt", target_jyuni=3, min_samples=10)
        metrics = trainer.evaluate_calibration(calibrator, target_jyuni=3)
        assert "brier_score" in metrics
        assert "calibration_error" in metrics
        assert 0.0 <= metrics["brier_score"] <= 1.0
        assert 0.0 <= metrics["calibration_error"] <= 1.0
        assert metrics["total_samples"] > 0

    def test_no_horse_scores_table(self, tmp_path) -> None:
        """horse_scoresテーブルなしでエラー。"""
        jvlink_db = DatabaseManager(str(tmp_path / "jv.db"), wal_mode=False)
        ext_db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
        trainer = CalibrationTrainer(jvlink_db, ext_db)
        with pytest.raises(ValueError, match="horse_scoresテーブルが存在しません"):
            trainer.build_training_data()
