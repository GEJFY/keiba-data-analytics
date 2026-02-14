"""データドリブンファクター発見のテスト。"""

import pytest

from src.scoring.factor_discovery import (
    FactorDiscovery,
    _auc_from_labels,
    _point_biserial,
    _safe_float,
    _safe_int,
)


class TestUtilities:
    """ユーティリティ関数のテスト。"""

    def test_safe_float(self) -> None:
        assert _safe_float("3.14") == 3.14
        assert _safe_float(None) == 0.0
        assert _safe_float("abc", 1.0) == 1.0

    def test_safe_int(self) -> None:
        assert _safe_int("42") == 42
        assert _safe_int(None) == 0
        assert _safe_int("abc", -1) == -1


class TestAUC:
    """AUC計算のテスト。"""

    def test_perfect_auc(self) -> None:
        """完全分離でAUC=1.0。"""
        scores = [0.1, 0.2, 0.3, 0.8, 0.9, 1.0]
        labels = [0, 0, 0, 1, 1, 1]
        assert _auc_from_labels(scores, labels) == 1.0

    def test_random_auc(self) -> None:
        """ランダムでAUC≈0.5。"""
        scores = [0.5, 0.5, 0.5, 0.5]
        labels = [0, 1, 0, 1]
        assert _auc_from_labels(scores, labels) == 0.5

    def test_inverse_auc(self) -> None:
        """逆順でAUC=0.0。"""
        scores = [0.9, 0.8, 0.7, 0.1, 0.2, 0.3]
        labels = [0, 0, 0, 1, 1, 1]
        assert _auc_from_labels(scores, labels) == 0.0

    def test_empty(self) -> None:
        assert _auc_from_labels([], []) == 0.5

    def test_no_positives(self) -> None:
        assert _auc_from_labels([0.5], [0]) == 0.5

    def test_no_negatives(self) -> None:
        assert _auc_from_labels([0.5], [1]) == 0.5


class TestPointBiserial:
    """点双列相関係数のテスト。"""

    def test_positive_correlation(self) -> None:
        scores = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        labels = [0, 0, 0, 0, 0, 1, 1, 1, 1, 1]
        r = _point_biserial(scores, labels)
        assert r > 0.5  # 正の相関

    def test_no_correlation(self) -> None:
        scores = [1, 2, 3, 4, 5, 6, 7, 8]
        labels = [1, 0, 1, 0, 1, 0, 1, 0]
        r = _point_biserial(scores, labels)
        assert abs(r) < 0.3  # ほぼ無相関

    def test_insufficient_data(self) -> None:
        assert _point_biserial([1], [0]) == 0.0

    def test_constant_scores(self) -> None:
        """全て同じ値の場合。"""
        assert _point_biserial([5, 5, 5], [0, 1, 0]) == 0.0


class TestFactorDiscovery:
    """FactorDiscoveryクラスのテスト。"""

    @pytest.fixture()
    def discovery(self, tmp_path) -> FactorDiscovery:
        """テスト用のFactorDiscoveryインスタンスを作成する。"""
        from src.data.db import DatabaseManager

        db = DatabaseManager(str(tmp_path / "test.db"), wal_mode=False)

        # テーブル作成
        db.execute_write("""
            CREATE TABLE NL_RA_RACE (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Kyori TEXT, TrackCD TEXT, SyussoTosu TEXT
            )
        """)
        db.execute_write("""
            CREATE TABLE NL_SE_RACE_UMA (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Umaban TEXT, Wakuban TEXT, SexCD TEXT,
                Barei TEXT, Futan TEXT, Ninki TEXT,
                KakuteiJyuni TEXT, Odds TEXT,
                BaTaijyu TEXT, ZogenFugo TEXT, ZogenSa TEXT,
                DMJyuni TEXT, HaronTimeL3 TEXT, HaronTimeL4 TEXT,
                KyakusituKubun TEXT, Jyuni4c TEXT
            )
        """)

        # テストデータ投入（20レース × 6頭 = 120サンプル）
        for day_idx in range(4):
            month_day = f"01{day_idx + 1:02d}"
            for race_num in range(1, 6):
                db.execute_write(
                    "INSERT INTO NL_RA_RACE VALUES (?,?,?,?,?,?,?,?,?)",
                    ("2025", month_day, "05", "01", "01", f"{race_num:02d}",
                     "1600", "10", "6"),
                )
                for uma in range(1, 7):
                    # 人気順が低い（＝数値が小さい）ほど着順が良い傾向を作る
                    ninki = uma
                    jyuni = uma  # 人気通り決着（テスト用）
                    odds = 2.0 + uma * 3.0
                    dm_rank = uma
                    db.execute_write(
                        "INSERT INTO NL_SE_RACE_UMA VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        ("2025", month_day, "05", "01", "01", f"{race_num:02d}",
                         f"{uma:02d}", f"{(uma + 1) // 2}", "1",
                         "3", "55.0", str(ninki),
                         str(jyuni), str(odds * 10),
                         "480", "+", str(uma), str(dm_rank),
                         "34.5", "46.0", "2", str(uma)),
                    )

        return FactorDiscovery(db, None)

    def test_discover_returns_dict(self, discovery: FactorDiscovery) -> None:
        """discover()がdict構造を返すこと。"""
        result = discovery.discover(max_races=100)
        assert "n_samples" in result
        assert "candidates" in result
        assert "interactions" in result
        assert result["n_samples"] > 0

    def test_discover_finds_candidates(self, discovery: FactorDiscovery) -> None:
        """候補ファクターが検出されること。"""
        result = discovery.discover(max_races=100, min_auc=0.50)
        assert len(result["candidates"]) > 0

    def test_candidate_structure(self, discovery: FactorDiscovery) -> None:
        """候補ファクターが必要なフィールドを持つこと。"""
        result = discovery.discover(max_races=100, min_auc=0.50)
        if result["candidates"]:
            c = result["candidates"][0]
            assert "name" in c
            assert "auc" in c
            assert "correlation" in c
            assert "direction" in c
            assert "quintile_rates" in c
            assert "suggested_expression" in c
            assert "category" in c

    def test_candidates_sorted_by_auc(self, discovery: FactorDiscovery) -> None:
        """候補がAUC降順でソートされること。"""
        result = discovery.discover(max_races=100, min_auc=0.50)
        aucs = [c["auc"] for c in result["candidates"]]
        assert aucs == sorted(aucs, reverse=True)

    def test_quintile_rates(self, discovery: FactorDiscovery) -> None:
        """五分位分析の結果が含まれること。"""
        result = discovery.discover(max_races=100, min_auc=0.50)
        for c in result["candidates"]:
            if c["quintile_rates"]:
                q = c["quintile_rates"][0]
                assert "quintile" in q
                assert "win_rate" in q
                assert "count" in q

    def test_empty_db(self, tmp_path) -> None:
        """空DBではサンプル0件で返ること。"""
        from src.data.db import DatabaseManager

        db = DatabaseManager(str(tmp_path / "empty.db"), wal_mode=False)
        fd = FactorDiscovery(db, None)
        result = fd.discover()
        assert result["n_samples"] == 0
        assert result["candidates"] == []

    def test_ninki_has_high_auc(self, discovery: FactorDiscovery) -> None:
        """人気順通り決着のデータでは、Ninkiが高いAUCを持つこと。"""
        result = discovery.discover(max_races=100, min_auc=0.50)
        ninki_candidates = [c for c in result["candidates"] if c["name"] == "Ninki"]
        assert len(ninki_candidates) > 0
        # 人気通り決着なのでAUCが高いはず
        assert ninki_candidates[0]["auc"] >= 0.60
        assert ninki_candidates[0]["direction"] == "lower_is_better"

    def test_base_rate(self, discovery: FactorDiscovery) -> None:
        """ベースレート（基準的中率）が正しく計算されること。"""
        result = discovery.discover(max_races=100)
        # 6頭立て × target_jyuni=1 → 基準的中率 ≈ 1/6
        assert 0.1 <= result["base_rate"] <= 0.25
