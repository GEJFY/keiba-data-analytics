"""BatchScorerのテスト。"""

import numpy as np
import pytest

from src.data.db import DatabaseManager
from src.scoring.batch_scorer import BatchScorer


def _setup_dbs(tmp_path):
    """テスト用DB群を構築する。

    2日間のレースに同一馬が再出走するデータを作成し、
    前走キャッシュが正しく機能することを検証する。
    """
    jvlink_db = DatabaseManager(str(tmp_path / "jvlink.db"), wal_mode=False)
    ext_db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)

    # JVLink DB: レース + 出走馬
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
                OddsTansyoInfo2Umaban TEXT, OddsTansyoInfo2Odds TEXT
            )
        """)

        # Day 1 (0105): 3レース x 3頭（KettoNum=K001,K002,K003）
        for race_num in range(1, 4):
            rn = f"{race_num:02d}"
            conn.execute(
                "INSERT INTO NL_RA_RACE VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("2025", "0105", "06", "01", "01", rn,
                 f"Day1-R{race_num}", "1600", "11", "", "", "", "", "", "",
                 "3", "3", "3", "", ""),
            )
            for umaban in range(1, 4):
                ub = f"{umaban:02d}"
                jyuni = str(umaban)
                ketto = f"K{umaban:03d}"
                conn.execute(
                    "INSERT INTO NL_SE_RACE_UMA VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("2025", "0105", "06", "01", "01", rn,
                     ub, ub, ketto, f"馬{umaban}",
                     "1", "4", "550",
                     "騎手", "調教師",
                     "480", "+", "2",
                     jyuni, str(umaban), "100", "1400",
                     "350", "480",
                     str(umaban), str(umaban),
                     "0", "0", "0", str(umaban)),
                )
            conn.execute(
                "INSERT INTO NL_O1_ODDS_TANFUKUWAKU VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                ("2025", "0105", "06", "01", "01", rn,
                 "01", "0050", "02", "0100", "03", "0200"),
            )

        # Day 2 (0106): 1レース x 3頭（同じKettoNum=K001,K002,K003が再出走）
        conn.execute(
            "INSERT INTO NL_RA_RACE VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("2025", "0106", "06", "01", "02", "01",
             "Day2-R1", "1600", "11", "", "", "", "", "", "",
             "3", "3", "3", "", ""),
        )
        for umaban in range(1, 4):
            ub = f"{umaban:02d}"
            jyuni = str(4 - umaban)  # 逆順: 1→3, 2→2, 3→1
            ketto = f"K{umaban:03d}"
            conn.execute(
                "INSERT INTO NL_SE_RACE_UMA VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("2025", "0106", "06", "01", "02", "01",
                 ub, ub, ketto, f"馬{umaban}",
                 "1", "4", "550",
                 "騎手", "調教師",
                 "480", "+", "2",
                 jyuni, str(umaban), "100", "1400",
                 "340", "480",
                 str(umaban), "2",
                 "0", "0", "0", str(umaban)),
            )
        conn.execute(
            "INSERT INTO NL_O1_ODDS_TANFUKUWAKU VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("2025", "0106", "06", "01", "02", "01",
             "01", "0050", "02", "0100", "03", "0200"),
        )

    # 拡張DB: factor_rules
    from scripts.init_db import EXTENSION_TABLES
    with ext_db.connect() as conn:
        for ddl in EXTENSION_TABLES:
            conn.execute(ddl)
        # ファクタールール: prev_変数を使用するものと使用しないもの
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
            VALUES ('前走上位着順減点', 'form', 'test',
                    '-1 if prev_jyuni > 0 and prev_jyuni <= 3 else 0', 1.0,
                    1, '2025-01-01', '2025-01-01', 'APPROVED')
        """)

    return jvlink_db, ext_db


@pytest.fixture
def dbs(tmp_path):
    return _setup_dbs(tmp_path)


class TestBatchScorer:
    """BatchScorerのテスト。"""

    def test_build_factor_matrix(self, dbs) -> None:
        """ファクター行列の正常構築。"""
        jvlink_db, ext_db = dbs
        scorer = BatchScorer(jvlink_db, ext_db)
        matrix = scorer.build_factor_matrix()

        assert "X" in matrix
        assert "y" in matrix
        assert "scores" in matrix
        assert "factor_names" in matrix
        assert "jyuni" in matrix
        assert "odds" in matrix
        assert "race_keys" in matrix

        # Day1: 3レース x 3頭 = 9, Day2: 1レース x 3頭 = 3, 計12頭
        assert matrix["X"].shape[0] == 12
        assert matrix["X"].shape[1] == 2
        assert len(matrix["y"]) == 12
        assert len(matrix["scores"]) == 12
        assert len(matrix["factor_names"]) == 2

    def test_build_factor_matrix_date_filter(self, dbs) -> None:
        """範囲外の日付でValueError。"""
        jvlink_db, ext_db = dbs
        scorer = BatchScorer(jvlink_db, ext_db)

        # NL_RA_RACEは2025年のみなので2026年は空
        with pytest.raises(ValueError, match="レースが見つかりません"):
            scorer.build_factor_matrix(date_from="20260101")

    def test_build_factor_matrix_empty_db(self, tmp_path) -> None:
        """空DBでのエラー。"""
        jvlink_db = DatabaseManager(str(tmp_path / "jv.db"), wal_mode=False)
        ext_db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
        from scripts.init_db import EXTENSION_TABLES
        with ext_db.connect() as conn:
            for ddl in EXTENSION_TABLES:
                conn.execute(ddl)
        scorer = BatchScorer(jvlink_db, ext_db)
        with pytest.raises(ValueError):
            scorer.build_factor_matrix()

    def test_scores_reflect_weights(self, dbs) -> None:
        """スコアがweight反映されたtotal_scoreになること。"""
        jvlink_db, ext_db = dbs
        scorer = BatchScorer(jvlink_db, ext_db)
        matrix = scorer.build_factor_matrix()

        # BASE_SCORE=100ベースのスコア
        for score in matrix["scores"]:
            assert score != 0  # 何かしらのスコアが付いている

    def test_jyuni_values(self, dbs) -> None:
        """確定着順が正しく取得されること。"""
        jvlink_db, ext_db = dbs
        scorer = BatchScorer(jvlink_db, ext_db)
        matrix = scorer.build_factor_matrix()

        unique_jyuni = set(matrix["jyuni"].tolist())
        assert unique_jyuni == {1, 2, 3}

    def test_prev_entry_cache(self, dbs) -> None:
        """Day2のレースでprev_contextが前走データを参照していること。

        Day1でKakuteiJyuni=1の馬がDay2で再出走する場合、
        前走着順減点ファクターが発火（-1）するはず。
        """
        jvlink_db, ext_db = dbs
        scorer = BatchScorer(jvlink_db, ext_db)
        matrix = scorer.build_factor_matrix()

        # Day2のレースは最後の3エントリ（ASC順で処理されるため）
        # factor_names[1] = '前走上位着順減点'
        # Day1: prev_jyuniが0（前走なし）→ ファクター=0
        # Day2: prev_jyuniがDay1の着順 → K001の着順1,2,3が前走データとして参照される
        day2_factor_values = matrix["X"][-3:, 1]  # 最後の3頭の前走ファクター

        # Day1のR03（最後のレース）の着順: K001=1, K002=2, K003=3
        # Day2: K001はprev_jyuni=1(<=3) → -1, K002はprev_jyuni=2(<=3) → -1,
        #        K003はprev_jyuni=3(<=3) → -1
        # 全馬が前走3着以内なので全て-1
        assert all(v == -1.0 for v in day2_factor_values)

    def test_day1_no_prev_context(self, dbs) -> None:
        """Day1のレースではprev_contextがなく、前走ファクターが非発火であること。"""
        jvlink_db, ext_db = dbs
        scorer = BatchScorer(jvlink_db, ext_db)
        matrix = scorer.build_factor_matrix()

        # Day1の最初のレース（最初の3エントリ）のprev_jyuniファクター
        day1_r1_factor_values = matrix["X"][:3, 1]  # 最初の3頭の前走ファクター

        # Day1 R1では全馬が初出走（キャッシュ空）→ prev_jyuni=0 → ファクター=0
        assert all(v == 0.0 for v in day1_r1_factor_values)
