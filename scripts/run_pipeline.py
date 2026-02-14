"""レースデー自動化パイプラインCLI。

Usage:
    python scripts/run_pipeline.py --full
    python scripts/run_pipeline.py --full --date 20250215
    python scripts/run_pipeline.py --sync-only
    python scripts/run_pipeline.py --score-only --date 20250215
    python scripts/run_pipeline.py --reconcile-only
    python scripts/run_pipeline.py --full --dry-run

Examples:
    # 当日のフルパイプライン実行（設定に従う）
    python scripts/run_pipeline.py --full

    # 特定日をdryrunモードで実行
    python scripts/run_pipeline.py --full --date 20250215 --dry-run

    # データ同期のみ
    python scripts/run_pipeline.py --sync-only

    # 結果照合のみ（翌日にレース結果を取り込み後に実行）
    python scripts/run_pipeline.py --reconcile-only
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.automation.pipeline import RaceDayPipeline
from src.dashboard.config_loader import get_db_managers, load_config


def _today() -> str:
    """当日日付をYYYYMMDD形式で返す。"""
    return datetime.now().strftime("%Y%m%d")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="レースデー自動化パイプライン",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Windows Task Schedulerで毎週末に --full を実行する想定です。",
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--full", action="store_true", help="全ステップ実行")
    mode.add_argument("--sync-only", action="store_true", help="データ同期のみ")
    mode.add_argument("--score-only", action="store_true", help="スコアリング+投票のみ")
    mode.add_argument("--reconcile-only", action="store_true", help="結果照合のみ")

    parser.add_argument(
        "--date", default="", help="対象日 YYYYMMDD（デフォルト: 当日）"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="dryrunモード強制（投票を実行しない）"
    )
    args = parser.parse_args()

    # 設定読込
    config = load_config()
    if args.dry_run:
        config.setdefault("betting", {})["method"] = "dryrun"

    jvlink_db, ext_db = get_db_managers(config)
    pipeline = RaceDayPipeline(jvlink_db, ext_db, config)

    target_date = args.date or _today()

    print()
    print("=" * 60)
    print("  レースデー自動化パイプライン")
    print("=" * 60)
    print(f"  対象日: {target_date}")
    print(f"  投票方式: {config.get('betting', {}).get('method', 'dryrun')}")
    if args.dry_run:
        print("  ※ dry-runモード（投票は実行されません）")
    print()

    if args.full:
        result = pipeline.run_full(target_date=target_date)
        _print_result(result)
    elif args.sync_only:
        sync_result = pipeline.step_sync()
        print(f"  同期結果: {sync_result.get('status', 'UNKNOWN')}")
        print(f"  終了コード: {sync_result.get('exit_code', -1)}")
    elif args.score_only:
        score_result = pipeline.step_score_and_bet(target_date)
        print(f"  レース数: {score_result.get('races_found', 0)}")
        print(f"  投票数: {score_result.get('total_bets', 0)}")
        print(f"  合計投票額: {score_result.get('total_stake', 0):,}円")
    elif args.reconcile_only:
        reconcile_result = pipeline.step_reconcile()
        print(f"  照合件数: {reconcile_result.get('reconciled', 0)}")

    print()
    print("=" * 60)
    print("  完了")
    print("=" * 60)
    print()


def _print_result(result) -> None:
    """PipelineResultを表示する。"""
    print(f"  ステータス: {result.status}")
    print(f"  レース数: {result.races_found}")
    print(f"  スコアリング済: {result.races_scored}")
    print(f"  投票数: {result.total_bets}")
    print(f"  合計投票額: {result.total_stake:,}円")
    print(f"  照合件数: {result.reconciled}")
    if result.errors:
        print(f"  エラー: {len(result.errors)}件")
        for err in result.errors:
            print(f"    - {err}")


if __name__ == "__main__":
    main()
