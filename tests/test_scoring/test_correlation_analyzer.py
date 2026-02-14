"""CorrelationAnalyzerのテスト。"""

import pytest

from src.data.db import DatabaseManager
from src.scoring.correlation_analyzer import CorrelationAnalyzer


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
                conn.execute(
                    "INSERT INTO NL_SE_RACE_UMA VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("2025", day, "06", "01", "01", rn,
                     ub, ub, "", f"馬{umaban}",
                     "1", "4", "550", "J", "T",
                     "480", "+", "2",
                     jyuni, str(umaban), "100", "1400",
                     "350", "480", str(umaban), "0",
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


class TestCorrelationAnalyzer:
    """CorrelationAnalyzerのテスト。"""

    def test_analyze_correlations(self, dbs) -> None:
        """相関分析が正常に実行されること。"""
        jvlink_db, ext_db = dbs
        analyzer = CorrelationAnalyzer(jvlink_db, ext_db)
        result = analyzer.analyze_correlations()

        assert "factor_names" in result
        assert "correlation_matrix" in result
        assert "redundant_pairs" in result
        assert "n_samples" in result

        assert len(result["factor_names"]) == 2
        assert len(result["correlation_matrix"]) == 2
        assert len(result["correlation_matrix"][0]) == 2
        assert result["n_samples"] > 0

        # 対角要素は1.0
        assert abs(result["correlation_matrix"][0][0] - 1.0) < 0.01
        assert abs(result["correlation_matrix"][1][1] - 1.0) < 0.01

    def test_redundant_pairs_detected(self, dbs) -> None:
        """冗長ペアの検出結果がリスト形式であること。"""
        jvlink_db, ext_db = dbs
        analyzer = CorrelationAnalyzer(jvlink_db, ext_db)
        result = analyzer.analyze_correlations()
        assert isinstance(result["redundant_pairs"], list)

    def test_sensitivity_analysis(self, dbs) -> None:
        """感度分析が正常に実行されること。"""
        jvlink_db, ext_db = dbs
        analyzer = CorrelationAnalyzer(jvlink_db, ext_db)
        result = analyzer.sensitivity_analysis()

        assert "factor_names" in result
        assert "deltas" in result
        assert "sensitivity_matrix" in result
        assert "n_samples" in result

        assert len(result["factor_names"]) == 2
        assert len(result["deltas"]) == 4
        assert len(result["sensitivity_matrix"]) == 2
        assert len(result["sensitivity_matrix"][0]) == 4
        assert result["n_samples"] > 0

    def test_sensitivity_custom_deltas(self, dbs) -> None:
        """カスタム変動幅で感度分析ができること。"""
        jvlink_db, ext_db = dbs
        analyzer = CorrelationAnalyzer(jvlink_db, ext_db)
        result = analyzer.sensitivity_analysis(weight_deltas=[-0.3, 0.3])

        assert len(result["deltas"]) == 2
        assert len(result["sensitivity_matrix"][0]) == 2
