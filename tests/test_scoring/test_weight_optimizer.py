"""WeightOptimizerのテスト。"""

import pytest

from src.data.db import DatabaseManager
from src.scoring.weight_optimizer import WeightOptimizer


def _setup_dbs(tmp_path):
    """テスト用DB群を構築する（十分なサンプル数）。"""
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

        # 20レース x 8頭 = 160サンプル（十分な数）
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


class TestWeightOptimizer:
    """WeightOptimizerのテスト。"""

    def test_optimize(self, dbs) -> None:
        """最適化が正常に実行されること。"""
        jvlink_db, ext_db = dbs
        optimizer = WeightOptimizer(jvlink_db, ext_db)
        result = optimizer.optimize()

        assert "weights" in result
        assert "current_weights" in result
        assert "accuracy" in result
        assert "log_loss" in result
        assert "n_samples" in result
        assert "feature_coefs" in result

        assert result["n_samples"] > 0
        assert 0.0 <= result["accuracy"] <= 1.0
        assert result["log_loss"] >= 0.0

        # Weightの範囲チェック
        for _name, w in result["weights"].items():
            assert 0.0 <= w <= WeightOptimizer.MAX_WEIGHT

    def test_optimize_returns_all_factors(self, dbs) -> None:
        """全ファクターのWeightが返ること。"""
        jvlink_db, ext_db = dbs
        optimizer = WeightOptimizer(jvlink_db, ext_db)
        result = optimizer.optimize()

        assert "DM予想上位" in result["weights"]
        assert "前走上位着順減点" in result["weights"]

    def test_apply_weights(self, dbs) -> None:
        """Weight適用がDBに反映されること。"""
        jvlink_db, ext_db = dbs
        optimizer = WeightOptimizer(jvlink_db, ext_db)
        result = optimizer.optimize()

        updated = optimizer.apply_weights(result["weights"])
        assert updated >= 0

        # DB上のweightが更新されていること
        from src.factors.registry import FactorRegistry
        registry = FactorRegistry(ext_db)
        rules = registry.get_active_rules()
        for rule in rules:
            if rule["rule_name"] in result["weights"]:
                expected = result["weights"][rule["rule_name"]]
                assert abs(rule["weight"] - expected) < 0.01 or updated == 0

    def test_optimize_returns_training_period(self, dbs) -> None:
        """optimize()が訓練期間を返すこと。"""
        jvlink_db, ext_db = dbs
        optimizer = WeightOptimizer(jvlink_db, ext_db)
        # テストデータは2025年のレースなので範囲を合わせる
        result = optimizer.optimize(date_from="20250101", date_to="20250630")
        assert result["training_from"] == "20250101"
        assert result["training_to"] == "20250630"

    def test_apply_weights_records_training_period(self, dbs) -> None:
        """apply_weights時にtraining_from/toがDBに記録されること。"""
        jvlink_db, ext_db = dbs
        optimizer = WeightOptimizer(jvlink_db, ext_db)
        result = optimizer.optimize()

        # 差分があるWeightに大きな変更を加えて確実に更新
        modified_weights = {k: 2.5 for k in result["weights"]}
        updated = optimizer.apply_weights(
            modified_weights,
            training_from="20240101",
            training_to="20240630",
        )
        assert updated > 0

        # DBに訓練期間が記録されていること
        rows = ext_db.execute_query(
            "SELECT training_from, training_to FROM factor_rules WHERE training_from IS NOT NULL"
        )
        assert len(rows) > 0
        assert rows[0]["training_from"] == "2024-01-01"
        assert rows[0]["training_to"] == "2024-06-30"

    def test_normalize_date(self) -> None:
        """YYYYMMDD → YYYY-MM-DD 正規化のテスト。"""
        assert WeightOptimizer._normalize_date("20240101") == "2024-01-01"
        assert WeightOptimizer._normalize_date("2024-01-01") == "2024-01-01"
        assert WeightOptimizer._normalize_date("") == ""
