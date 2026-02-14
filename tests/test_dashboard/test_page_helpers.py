"""ダッシュボードページのヘルパー関数テスト。

Streamlit依存のページ本体はテスト対象外とし、
ページ内のヘルパー関数（データ取得・計算ロジック）をテストする。
"""

import pandas as pd
import pytest

from src.data.db import DatabaseManager


@pytest.fixture
def jvlink_db(tmp_path) -> DatabaseManager:
    """JVLink DBを模擬するフィクスチャ。"""
    db = DatabaseManager(str(tmp_path / "jvlink.db"), wal_mode=False)
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE NL_RA_RACE (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                RaceInfoHondai TEXT, Kyori TEXT, TrackCD TEXT,
                SyussoTosu INTEGER
            )
        """)
        conn.execute("""
            INSERT INTO NL_RA_RACE VALUES
            ('2025', '0105', '06', '01', '01', '01', 'テストレース', '1600', '10', 12)
        """)
        conn.execute("""
            CREATE TABLE NL_SE_RACE_UMA (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Umaban TEXT, Bamei TEXT
            )
        """)
        conn.execute("""
            INSERT INTO NL_SE_RACE_UMA VALUES
            ('2025', '0105', '06', '01', '01', '01', '01', 'テスト馬')
        """)
    return db


@pytest.fixture
def ext_db(tmp_path) -> DatabaseManager:
    """拡張DBを模擬するフィクスチャ。"""
    db = DatabaseManager(str(tmp_path / "ext.db"), wal_mode=False)
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE factor_rules (
                rule_id INTEGER PRIMARY KEY, rule_name TEXT, category TEXT,
                weight REAL, validation_score REAL, is_active INTEGER,
                review_status TEXT, decay_rate REAL, description TEXT,
                source TEXT, updated_at TEXT, sql_expression TEXT
            )
        """)
        conn.execute("""
            INSERT INTO factor_rules VALUES
            (1, 'テストルール', 'form', 1.5, 0.8, 1, 'APPROVED', 0.0, '説明',
             'gy_initial', '2025-01-01', '1')
        """)
        conn.execute("""
            CREATE TABLE factor_review_log (
                log_id INTEGER PRIMARY KEY, rule_id INTEGER, action TEXT,
                old_weight REAL, new_weight REAL, reason TEXT,
                backtest_roi REAL, changed_at TEXT, changed_by TEXT
            )
        """)
        conn.execute("""
            INSERT INTO factor_review_log VALUES
            (1, 1, 'weight_change', 1.0, 1.5, 'テスト変更', 0.05, '2025-01-01', 'test')
        """)
        conn.execute("""
            CREATE TABLE bankroll_log (
                date TEXT, opening_balance INTEGER, total_stake INTEGER,
                total_payout INTEGER, closing_balance INTEGER,
                pnl INTEGER, roi REAL
            )
        """)
        conn.execute("""
            INSERT INTO bankroll_log VALUES
            ('2025-01-05', 1000000, 50000, 80000, 1030000, 30000, 0.6),
            ('2025-01-06', 1030000, 60000, 40000, 1010000, -20000, -0.333),
            ('2025-01-07', 1010000, 45000, 90000, 1055000, 45000, 1.0)
        """)
    return db


class TestPageDataHelpers:
    """page_data.pyのヘルパー関数テスト。"""

    def test_get_table_counts(self, jvlink_db: DatabaseManager) -> None:
        """テーブルレコード数の取得。"""
        from src.dashboard.pages.page_data import _get_table_counts

        counts = _get_table_counts(jvlink_db)
        assert isinstance(counts, list)
        # NL_RA_RACEが存在し1件
        ra = next(r for r in counts if r["テーブル"] == "NL_RA_RACE")
        assert ra["レコード数"] == 1
        assert ra["状態"] == "OK"
        # 存在しないテーブルは「未作成」
        hr = next(r for r in counts if r["テーブル"] == "NL_HR_PAY")
        assert hr["レコード数"] == 0
        assert hr["状態"] == "未作成"

    def test_get_race_list(self, jvlink_db: DatabaseManager) -> None:
        """レース一覧の取得。"""
        from src.dashboard.pages.page_data import _get_race_list

        df = _get_race_list(jvlink_db)
        assert not df.empty
        assert "日付" in df.columns
        assert "競馬場" in df.columns
        assert "レース名" in df.columns
        assert df.iloc[0]["競馬場"] == "中山"  # JyoCD 06

    def test_get_race_list_empty(self, tmp_path) -> None:
        """テーブルが存在しない場合は空DataFrame。"""
        from src.dashboard.pages.page_data import _get_race_list

        db = DatabaseManager(str(tmp_path / "empty.db"), wal_mode=False)
        df = _get_race_list(db)
        assert df.empty


class TestPageFactorsHelpers:
    """page_factors.pyのヘルパー関数テスト。"""

    def test_load_all_rules(self, ext_db: DatabaseManager) -> None:
        """全ルールの取得。"""
        from src.dashboard.pages.page_factors import _load_all_rules

        df = _load_all_rules(ext_db)
        assert not df.empty
        assert "rule_name" in df.columns
        assert df.iloc[0]["rule_name"] == "テストルール"

    def test_load_all_rules_empty(self, tmp_path) -> None:
        """テーブルなしの場合は空DataFrame。"""
        from src.dashboard.pages.page_factors import _load_all_rules

        db = DatabaseManager(str(tmp_path / "empty.db"), wal_mode=False)
        df = _load_all_rules(db)
        assert df.empty

    def test_load_change_log(self, ext_db: DatabaseManager) -> None:
        """変更履歴の取得。"""
        from src.dashboard.pages.page_factors import _load_change_log

        df = _load_change_log(ext_db)
        assert not df.empty
        assert "action" in df.columns

    def test_load_change_log_by_rule_id(self, ext_db: DatabaseManager) -> None:
        """特定ルールIDの変更履歴。"""
        from src.dashboard.pages.page_factors import _load_change_log

        df = _load_change_log(ext_db, rule_id=1)
        assert not df.empty
        assert all(df["rule_id"] == 1)

    def test_load_change_log_empty(self, tmp_path) -> None:
        """テーブルなしの場合は空DataFrame。"""
        from src.dashboard.pages.page_factors import _load_change_log

        db = DatabaseManager(str(tmp_path / "empty.db"), wal_mode=False)
        df = _load_change_log(db)
        assert df.empty


class TestPagePnlHelpers:
    """page_pnl.pyのヘルパー関数テスト。"""

    def test_load_bankroll_log(self, ext_db: DatabaseManager) -> None:
        """bankroll_logの読み込み。"""
        from src.dashboard.pages.page_pnl import _load_bankroll_log

        df = _load_bankroll_log(ext_db)
        assert not df.empty
        assert len(df) == 3
        assert "date" in df.columns

    def test_load_bankroll_log_empty(self, tmp_path) -> None:
        from src.dashboard.pages.page_pnl import _load_bankroll_log

        db = DatabaseManager(str(tmp_path / "empty.db"), wal_mode=False)
        df = _load_bankroll_log(db)
        assert df.empty

    def test_compute_cumulative(self, ext_db: DatabaseManager) -> None:
        """累積P&Lとドローダウンの計算。"""
        from src.dashboard.pages.page_pnl import (
            _compute_cumulative,
            _load_bankroll_log,
        )

        df = _load_bankroll_log(ext_db)
        dates, cum_pnl, drawdowns = _compute_cumulative(df)

        assert len(dates) == 3
        assert len(cum_pnl) == 3
        assert len(drawdowns) == 3

        # 累積PnL: 30000, 10000, 55000
        assert cum_pnl[0] == 30000
        assert cum_pnl[1] == 10000  # 30000 + (-20000)
        assert cum_pnl[2] == 55000  # 10000 + 45000

        # ドローダウンは負の値
        assert drawdowns[0] == 0.0  # ピーク = 30000なのでDDなし
        assert drawdowns[1] < 0  # 30000 → 10000 = DD
        assert drawdowns[2] == 0.0  # 新たなピーク55000


class TestPagePnlMonthlyHelpers:
    """page_pnl.pyの月次集計ヘルパーテスト。"""

    def test_build_monthly_table(self, ext_db: DatabaseManager) -> None:
        """月次集計テーブルが正しく構築されること。"""
        from src.dashboard.pages.page_pnl import (
            _build_monthly_table,
            _load_bankroll_log,
        )

        df = _load_bankroll_log(ext_db)
        monthly = _build_monthly_table(df)
        assert not monthly.empty
        # 全レコードが2025-01なので1行
        assert len(monthly) == 1
        assert monthly.iloc[0]["pnl"] == 55000  # 30000 + (-20000) + 45000

    def test_build_heatmap_data(self, ext_db: DatabaseManager) -> None:
        """ヒートマップデータが正しく構築されること。"""
        from src.dashboard.pages.page_pnl import (
            _build_heatmap_data,
            _build_monthly_table,
            _load_bankroll_log,
        )

        df = _load_bankroll_log(ext_db)
        monthly = _build_monthly_table(df)
        years, months, values = _build_heatmap_data(monthly)

        assert len(years) == 1
        assert years[0] == 2025
        assert len(months) == 12
        assert len(values) == 1
        assert len(values[0]) == 12
        # 1月のP&L = 55000
        assert values[0][0] == 55000
        # 2月以降は0
        assert all(v == 0 for v in values[0][1:])

    def test_build_heatmap_empty(self) -> None:
        """空DataFrameで空データが返ること。"""
        from src.dashboard.pages.page_pnl import _build_heatmap_data

        years, months, values = _build_heatmap_data(pd.DataFrame())
        assert years == []
        assert months == []
        assert values == []

    def test_build_bet_type_stats(self, tmp_path) -> None:
        """券種別統計が正しく集計されること。"""
        from src.dashboard.pages.page_pnl import _build_bet_type_stats

        df = pd.DataFrame([
            {"bet_id": 1, "bet_type": "WIN", "stake_yen": 1000, "payout_yen": 5000, "status": "SETTLED", "result": "WIN"},
            {"bet_id": 2, "bet_type": "WIN", "stake_yen": 1000, "payout_yen": 0, "status": "SETTLED", "result": "LOSE"},
            {"bet_id": 3, "bet_type": "PLACE", "stake_yen": 2000, "payout_yen": 3000, "status": "SETTLED", "result": "WIN"},
        ])
        stats = _build_bet_type_stats(df)
        assert not stats.empty
        assert len(stats) == 2  # WIN, PLACE

        win_row = stats[stats["bet_type"] == "WIN"].iloc[0]
        assert win_row["n_bets"] == 2
        assert win_row["n_wins"] == 1
        assert win_row["total_stake"] == 2000
        assert win_row["total_payout"] == 5000

    def test_build_bet_type_stats_empty(self) -> None:
        """空DataFrameで空統計が返ること。"""
        from src.dashboard.pages.page_pnl import _build_bet_type_stats

        stats = _build_bet_type_stats(pd.DataFrame())
        assert stats.empty


class TestPageBacktestHelpers:
    """page_backtest.pyのヘルパー関数テスト。"""

    def test_load_backtest_results_empty(self, tmp_path) -> None:
        """テーブルなしの場合は空DataFrame。"""
        from src.dashboard.pages.page_backtest import _load_backtest_results

        db = DatabaseManager(str(tmp_path / "empty.db"), wal_mode=False)
        df = _load_backtest_results(db)
        assert df.empty

    def test_load_backtest_results_with_data(self, ext_db: DatabaseManager) -> None:
        """backtest_resultsがある場合。"""
        from src.dashboard.pages.page_backtest import _load_backtest_results

        # テーブル作成＆データ挿入
        with ext_db.connect() as conn:
            conn.execute("""
                CREATE TABLE backtest_results (
                    bt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_version TEXT, date_from TEXT, date_to TEXT,
                    total_races INTEGER, total_bets INTEGER,
                    total_stake INTEGER, total_payout INTEGER,
                    pnl INTEGER, roi REAL, win_rate REAL,
                    max_drawdown REAL, sharpe_ratio REAL,
                    params_json TEXT, executed_at TEXT
                )
            """)
            conn.execute("""
                INSERT INTO backtest_results
                (strategy_version, date_from, date_to, total_races, total_bets,
                 total_stake, total_payout, pnl, roi, win_rate,
                 max_drawdown, sharpe_ratio, params_json, executed_at)
                VALUES ('GY_VALUE v1.0.0', '2025-01-01', '2025-01-31', 36, 15,
                        150000, 180000, 30000, 0.2, 0.25,
                        0.08, 1.5, '{}', '2025-02-01T00:00:00')
            """)

        df = _load_backtest_results(ext_db)
        assert not df.empty
        assert df.iloc[0]["strategy_version"] == "GY_VALUE v1.0.0"
