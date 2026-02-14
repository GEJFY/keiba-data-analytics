"""GY初期ファクターをDBに登録するスクリプト。

GY_INITIAL_FACTORSの25ファクターをfactor_rulesテーブルに登録し、
DRAFT → TESTING → APPROVED のステータス遷移を実行する。

Usage:
    python scripts/register_factors.py [ext_db_path]
"""

import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.data.db import DatabaseManager
from src.factors.registry import FactorRegistry
from src.factors.rules.gy_factors import GY_INITIAL_FACTORS


def register_all_factors(ext_db_path: str) -> None:
    """GY初期ファクターを全件登録してAPPROVEDにする。"""
    db = DatabaseManager(ext_db_path, wal_mode=False)
    registry = FactorRegistry(db)

    # 既存ルール数を確認
    existing = db.execute_query("SELECT COUNT(*) as cnt FROM factor_rules")
    existing_count = existing[0]["cnt"] if existing else 0

    if existing_count > 0:
        print(f"既存ルール: {existing_count} 件。追加登録をスキップします。")
        print("再登録する場合は factor_rules テーブルをクリアしてください。")
        return

    registered = 0
    for factor in GY_INITIAL_FACTORS:
        try:
            rule_id = registry.create_rule({
                **factor,
                "changed_by": "register_factors.py",
            })
            # DRAFT → TESTING → APPROVED
            registry.transition_status(
                rule_id, "TESTING",
                reason="初期登録・自動テスト遷移",
                changed_by="register_factors.py",
            )
            registry.transition_status(
                rule_id, "APPROVED",
                reason="初期登録・自動承認",
                changed_by="register_factors.py",
            )
            registered += 1
            print(f"  [{registered:2d}] {factor['rule_name']} (ID: {rule_id}) → APPROVED")
        except Exception as e:
            print(f"  ERROR: {factor['rule_name']}: {e}", file=sys.stderr)

    print(f"\n登録完了: {registered}/{len(GY_INITIAL_FACTORS)} ファクター")


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "./data/extension.db"
    print(f"DB: {db_path}")
    register_all_factors(db_path)
