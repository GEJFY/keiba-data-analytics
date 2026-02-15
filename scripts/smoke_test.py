"""スモークテスト — 全モジュールの基本動作確認。

ダミーデータなしでも実行可能。各コンポーネントのimport・初期化が
正常に動くかを高速に検証する。

Usage:
    python scripts/smoke_test.py
"""

import sys
import tempfile
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

PASS = 0
FAIL = 0


def _check(label: str, fn) -> None:
    """テスト項目を実行し結果を表示する。"""
    global PASS, FAIL
    try:
        fn()
        print(f"  [OK] {label}")
        PASS += 1
    except Exception as e:
        print(f"  [NG] {label}: {e}")
        FAIL += 1


def test_imports():
    """全モジュールのimportテスト。"""
    print("\n=== 1. モジュールimport ===")

    _check("src.data.db", lambda: __import__("src.data.db"))
    _check("src.data.provider", lambda: __import__("src.data.provider"))
    _check("src.data.validator", lambda: __import__("src.data.validator"))
    _check("src.data.jvlink_sync", lambda: __import__("src.data.jvlink_sync"))
    _check("src.factors.registry", lambda: __import__("src.factors.registry"))
    _check("src.factors.rules.gy_factors", lambda: __import__("src.factors.rules.gy_factors"))
    _check("src.scoring.engine", lambda: __import__("src.scoring.engine"))
    _check("src.scoring.weight_optimizer", lambda: __import__("src.scoring.weight_optimizer"))
    _check("src.scoring.calibration", lambda: __import__("src.scoring.calibration"))
    _check("src.scoring.calibration_trainer", lambda: __import__("src.scoring.calibration_trainer"))
    _check("src.scoring.feature_importance", lambda: __import__("src.scoring.feature_importance"))
    _check("src.scoring.correlation_analyzer", lambda: __import__("src.scoring.correlation_analyzer"))
    _check("src.scoring.factor_discovery", lambda: __import__("src.scoring.factor_discovery"))
    _check("src.scoring.batch_scorer", lambda: __import__("src.scoring.batch_scorer"))
    _check("src.scoring.evaluator", lambda: __import__("src.scoring.evaluator"))
    _check("src.backtest.engine", lambda: __import__("src.backtest.engine"))
    _check("src.backtest.metrics", lambda: __import__("src.backtest.metrics"))
    _check("src.backtest.monte_carlo", lambda: __import__("src.backtest.monte_carlo"))
    _check("src.backtest.walk_forward", lambda: __import__("src.backtest.walk_forward"))
    _check("src.strategy.plugins.gy_value", lambda: __import__("src.strategy.plugins.gy_value"))
    _check("src.strategy.plugins.fixed_stake", lambda: __import__("src.strategy.plugins.fixed_stake"))
    _check("src.betting.executor", lambda: __import__("src.betting.executor"))
    _check("src.betting.result_collector", lambda: __import__("src.betting.result_collector"))
    _check("src.automation.pipeline", lambda: __import__("src.automation.pipeline"))
    _check("src.search.config", lambda: __import__("src.search.config"))
    _check("src.search.trial_runner", lambda: __import__("src.search.trial_runner"))
    _check("src.search.orchestrator", lambda: __import__("src.search.orchestrator"))
    _check("src.search.result_store", lambda: __import__("src.search.result_store"))
    _check("src.agents.base", lambda: __import__("src.agents.base"))
    _check("src.llm_gateway.azure_provider", lambda: __import__("src.llm_gateway.azure_provider"))
    _check("src.notifications.notifier", lambda: __import__("src.notifications.notifier"))
    _check("src.reporting.tax_report", lambda: __import__("src.reporting.tax_report"))
    _check("src.dashboard.config_loader", lambda: __import__("src.dashboard.config_loader"))
    _check("src.dashboard.task_manager", lambda: __import__("src.dashboard.task_manager"))


def test_db_operations():
    """DB操作の基本テスト。"""
    print("\n=== 2. DB操作 ===")
    from src.data.db import DatabaseManager

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = DatabaseManager(db_path, wal_mode=False)

    _check("テーブル作成", lambda: db.execute_write(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"
    ))
    _check("INSERT", lambda: db.execute_write(
        "INSERT INTO test (name) VALUES (?)", ("hello",)
    ))
    def select_test():
        rows = db.execute_query("SELECT * FROM test")
        if len(rows) != 1 or rows[0]["name"] != "hello":
            raise AssertionError(f"Unexpected rows: {rows}")

    def table_exists_test():
        if not db.table_exists("test"):
            raise AssertionError("test table should exist")
        if db.table_exists("nonexistent"):
            raise AssertionError("nonexistent table should not exist")

    _check("SELECT", select_test)
    _check("table_exists", table_exists_test)

    Path(db_path).unlink(missing_ok=True)


def test_schema_migration():
    """スキーマ自動マイグレーションのテスト。"""
    print("\n=== 3. スキーマ自動マイグレーション ===")
    import sqlite3

    from src.dashboard.config_loader import _ensure_ext_schema

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # training_from/to なしの古いスキーマを作成
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE factor_rules (
            rule_id INTEGER PRIMARY KEY,
            rule_name TEXT NOT NULL,
            weight REAL DEFAULT 1.0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

    def run_migration():
        _ensure_ext_schema(Path(db_path))
        conn2 = sqlite3.connect(db_path)
        cols = [r[1] for r in conn2.execute("PRAGMA table_info(factor_rules)").fetchall()]
        assert "training_from" in cols, f"training_from missing: {cols}"
        assert "training_to" in cols, f"training_to missing: {cols}"
        tables = [r[0] for r in conn2.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "rule_set_snapshots" in tables
        assert "factor_rules_archive" in tables
        conn2.close()

    _check("training_from/to追加 + バージョン管理テーブル作成", run_migration)

    Path(db_path).unlink(missing_ok=True)


def test_factor_lifecycle():
    """ファクターライフサイクルのテスト。"""
    print("\n=== 4. ファクターライフサイクル ===")
    from scripts.init_db import init_extension_tables
    from src.data.db import DatabaseManager
    from src.factors.registry import FactorRegistry

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    init_extension_tables(db_path)
    db = DatabaseManager(db_path, wal_mode=False)
    registry = FactorRegistry(db)

    _check("ルール作成", lambda: registry.create_rule({
        "rule_name": "test_factor", "category": "test", "weight": 1.5,
    }))

    def lifecycle():
        rules = registry.get_rules_by_status("DRAFT")
        assert len(rules) >= 1
        rid = rules[0]["rule_id"]
        registry.transition_status(rid, "TESTING", reason="test")
        registry.transition_status(rid, "APPROVED", reason="test")
        active = registry.get_active_rules()
        assert len(active) >= 1

    _check("DRAFT→TESTING→APPROVED遷移", lifecycle)

    Path(db_path).unlink(missing_ok=True)


def test_scoring_engine():
    """スコアリングエンジンの基本テスト。"""
    print("\n=== 5. スコアリングエンジン ===")
    from scripts.init_db import init_extension_tables
    from src.data.db import DatabaseManager
    from src.scoring.engine import ScoringEngine

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    init_extension_tables(db_path)
    db = DatabaseManager(db_path, wal_mode=False)

    _check("ScoringEngine初期化", lambda: ScoringEngine(db, calibrator=None))

    Path(db_path).unlink(missing_ok=True)


def test_search_config():
    """モデル探索設定のテスト。"""
    print("\n=== 6. モデル探索 ===")
    from src.search.config import SearchConfig, SearchSpace

    def validate_config():
        config = SearchConfig(
            date_from="20240101",
            date_to="20241231",
            n_trials=10,
        )
        assert config.n_trials == 10
        space = SearchSpace()
        assert len(space.get_dimensions()) > 0

    _check("SearchConfig + SearchSpace構築", validate_config)


def test_calibration():
    """キャリブレーションのテスト。"""
    print("\n=== 7. キャリブレーション ===")
    import numpy as np

    from src.scoring.calibration import IsotonicCalibrator, PlattCalibrator

    def platt_test():
        cal = PlattCalibrator()
        scores = np.array([0.1, 0.3, 0.5, 0.7, 0.9, 0.2, 0.4, 0.6, 0.8, 1.0])
        labels = np.array([0, 0, 0, 1, 1, 0, 0, 1, 1, 1])
        cal.fit(scores, labels)
        prob = cal.predict_proba(0.5)
        assert 0.0 <= prob <= 1.0

    def isotonic_test():
        cal = IsotonicCalibrator()
        scores = np.array([0.1, 0.3, 0.5, 0.7, 0.9, 0.2, 0.4, 0.6, 0.8, 1.0])
        labels = np.array([0, 0, 0, 1, 1, 0, 0, 1, 1, 1])
        cal.fit(scores, labels)
        prob = cal.predict_proba(0.5)
        assert 0.0 <= prob <= 1.0

    _check("PlattCalibrator", platt_test)
    _check("IsotonicCalibrator", isotonic_test)


def test_bankroll():
    """資金管理のテスト。"""
    print("\n=== 8. 資金管理 ===")
    from src.betting.bankroll import BankrollManager, BettingMethod

    def bankroll_test():
        bm = BankrollManager(
            initial_balance=1_000_000,
            method=BettingMethod.QUARTER_KELLY,
        )
        stake = bm.calculate_stake(estimated_prob=0.2, odds=6.0)
        assert stake > 0
        bm.record_bet(stake)
        assert bm.current_balance == 1_000_000 - stake

    _check("BankrollManager (Quarter Kelly)", bankroll_test)


def main():
    print("=" * 60)
    print("  Keiba Data Analytics — Smoke Test")
    print("=" * 60)

    test_imports()
    test_db_operations()
    test_schema_migration()
    test_factor_lifecycle()
    test_scoring_engine()
    test_search_config()
    test_calibration()
    test_bankroll()

    print(f"\n{'=' * 60}")
    print(f"  結果: {PASS} passed, {FAIL} failed")
    print(f"{'=' * 60}")

    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    main()
