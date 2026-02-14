"""FeatureImportanceAnalyzerのテスト。"""

import numpy as np
import pytest

from src.data.db import DatabaseManager
from src.scoring.feature_importance import FeatureImportanceAnalyzer


def _setup_dbs(tmp_path):
    """テスト用DB群を構築する。"""
    jvlink_db = DatabaseManager(str(tmp_path / "jvlink.db"), wal_mode=False)
    ext_db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)

    with jvlink_db.connect() as conn:
        conn.execute("""
            CREATE TABLE NL_RA_RACE (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                RaceInfoHondai TEXT, Kyori TEXT, TrackCD TEXT,
                TenkoBabaTenkoCD TEXT, TenkoBabaSibaBabaCD TEXT,
                TenkoBabaDirtBabaCD TEXT, JyokenInfoSyubetuCD TEXT,
                GradeCD TEXT, HassoTime TEXT, TorokuTosu TEXT,
                SyussoTosu TEXT, NyusenTosu TEXT,
                HaronTimeL3 TEXT, HaronTimeL4 TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE NL_SE_RACE_UMA (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Wakuban TEXT, Umaban TEXT, KettoNum TEXT, Bamei TEXT,
                SexCD TEXT, Barei TEXT, Futan TEXT,
                KisyuRyakusyo TEXT, ChokyosiRyakusyo TEXT,
                BaTaijyu TEXT, ZogenFugo TEXT, ZogenSa TEXT,
                KakuteiJyuni TEXT, Ninki TEXT, Odds TEXT, Time TEXT,
                HaronTimeL3 TEXT, HaronTimeL4 TEXT,
                DMJyuni TEXT, KyakusituKubun TEXT,
                Jyuni1c TEXT, Jyuni2c TEXT, Jyuni3c TEXT, Jyuni4c TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE NL_O1_ODDS_TANFUKUWAKU (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                OddsTansyoInfo0Umaban TEXT, OddsTansyoInfo0Odds TEXT,
                OddsTansyoInfo1Umaban TEXT, OddsTansyoInfo1Odds TEXT,
                OddsTansyoInfo2Umaban TEXT, OddsTansyoInfo2Odds TEXT,
                OddsTansyoInfo3Umaban TEXT, OddsTansyoInfo3Odds TEXT,
                OddsTansyoInfo4Umaban TEXT, OddsTansyoInfo4Odds TEXT,
                OddsTansyoInfo5Umaban TEXT, OddsTansyoInfo5Odds TEXT,
                OddsTansyoInfo6Umaban TEXT, OddsTansyoInfo6Odds TEXT,
                OddsTansyoInfo7Umaban TEXT, OddsTansyoInfo7Odds TEXT
            )
        """)

        # 20レース x 8頭 = 160サンプル
        for race_num in range(1, 21):
            rn = f"{race_num:02d}"
            day = f"01{race_num:02d}" if race_num <= 28 else "0128"
            conn.execute(
                "INSERT INTO NL_RA_RACE VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("2025", day, "06", "01", "01", rn,
                 f"R{race_num}", "1600", "11", "", "", "", "", "", "",
                 "8", "8", "8", "", ""),
            )
            odds_values = ["01", "0030", "02", "0050", "03", "0080", "04", "0120",
                           "05", "0200", "06", "0350", "07", "0500", "08", "0800"]
            conn.execute(
                f"INSERT INTO NL_O1_ODDS_TANFUKUWAKU VALUES ({','.join(['?'] * 22)})",
                ("2025", day, "06", "01", "01", rn, *odds_values),
            )
            for umaban in range(1, 9):
                ub = f"{umaban:02d}"
                jyuni = str(umaban)
                dm = str(umaban)
                ninki = str(umaban)
                conn.execute(
                    "INSERT INTO NL_SE_RACE_UMA VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("2025", day, "06", "01", "01", rn,
                     ub, ub, "", f"馬{umaban}",
                     "1", "4", "550",
                     "J", "T",
                     "480", "+", "2",
                     jyuni, ninki, "100", "1400",
                     "350", "480",
                     dm, "0",
                     "0", "0", "0", "0"),
                )

    from scripts.init_db import EXTENSION_TABLES
    with ext_db.connect() as conn:
        for ddl in EXTENSION_TABLES:
            conn.execute(ddl)
        conn.execute("""
            INSERT INTO factor_rules
            (rule_name, category, description, sql_expression, weight,
             is_active, created_at, updated_at, review_status)
            VALUES ('DM予想上位', 'dm', 'test', '1 if dm_rank <= 3 else 0', 1.5,
                    1, '2025-01-01', '2025-01-01', 'APPROVED')
        """)
        conn.execute("""
            INSERT INTO factor_rules
            (rule_name, category, description, sql_expression, weight,
             is_active, created_at, updated_at, review_status)
            VALUES ('前走上位着順減点', 'form', 'test', '-1 if KakuteiJyuni <= 3 else 0', 1.0,
                    1, '2025-01-01', '2025-01-01', 'APPROVED')
        """)

    return jvlink_db, ext_db


@pytest.fixture
def dbs(tmp_path):
    return _setup_dbs(tmp_path)


class TestFeatureImportanceAnalyzer:
    """FeatureImportanceAnalyzerのテスト。"""

    def test_analyze(self, dbs) -> None:
        """分析が正常に実行されること。"""
        jvlink_db, ext_db = dbs
        analyzer = FeatureImportanceAnalyzer(jvlink_db, ext_db)
        result = analyzer.analyze()

        assert "factors" in result
        assert "n_samples" in result
        assert "baseline_accuracy" in result

        assert result["n_samples"] > 0
        assert 0.0 <= result["baseline_accuracy"] <= 1.0
        assert len(result["factors"]) == 2

    def test_factor_structure(self, dbs) -> None:
        """各ファクターの結果構造が正しいこと。"""
        jvlink_db, ext_db = dbs
        analyzer = FeatureImportanceAnalyzer(jvlink_db, ext_db)
        result = analyzer.analyze()

        for f in result["factors"]:
            assert "rule_name" in f
            assert "category" in f
            assert "current_weight" in f
            assert "permutation_importance" in f
            assert "hit_rate_with" in f
            assert "hit_rate_without" in f
            assert "lift" in f
            assert "activation_rate" in f
            assert "correlation" in f

            # 値範囲チェック
            assert 0.0 <= f["hit_rate_with"] <= 1.0
            assert 0.0 <= f["hit_rate_without"] <= 1.0
            assert 0.0 <= f["activation_rate"] <= 1.0
            assert f["lift"] >= 0.0

    def test_hit_rate_calc(self) -> None:
        """ヒット率計算のユニットテスト。"""
        factor_col = np.array([1.0, 0.0, 1.0, 0.0, 1.0, 0.0])
        labels = np.array([1, 0, 0, 0, 1, 0])

        result = FeatureImportanceAnalyzer._calc_hit_rate(factor_col, labels)

        # 発火時(1.0の3件): 2的中/3 = 0.667
        assert abs(result["hit_rate_with"] - 2 / 3) < 0.01
        # 非発火時(0.0の3件): 0的中/3 = 0.0
        assert result["hit_rate_without"] == 0.0
        # 発火率: 3/6 = 0.5
        assert abs(result["activation_rate"] - 0.5) < 0.01

    def test_hit_rate_no_activation(self) -> None:
        """全て非発火の場合。"""
        factor_col = np.array([0.0, 0.0, 0.0])
        labels = np.array([1, 0, 0])

        result = FeatureImportanceAnalyzer._calc_hit_rate(factor_col, labels)
        assert result["hit_rate_with"] == 0.0
        assert result["activation_rate"] == 0.0
