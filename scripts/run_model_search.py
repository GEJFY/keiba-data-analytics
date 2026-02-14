"""自律モデル探索 CLI。

使用方法:
  python scripts/run_model_search.py --date-from 20240101 --date-to 20250101
  python scripts/run_model_search.py --resume <session_id>
  python scripts/run_model_search.py --date-from 20240101 --date-to 20250101 --n-trials 1000
"""

import argparse
import sys
from pathlib import Path

from loguru import logger


def main() -> None:
    parser = argparse.ArgumentParser(description="自律モデル探索")
    parser.add_argument("--date-from", help="探索対象期間の開始日 (YYYYMMDD)")
    parser.add_argument("--date-to", help="探索対象期間の終了日 (YYYYMMDD)")
    parser.add_argument("--n-trials", type=int, default=500, help="トライアル数 (default: 500)")
    parser.add_argument("--bankroll", type=int, default=1_000_000, help="初期資金 (default: 1,000,000)")
    parser.add_argument("--seed", type=int, default=42, help="乱数シード (default: 42)")
    parser.add_argument("--mc-sims", type=int, default=1000, help="Monte Carloシミュレーション回数")
    parser.add_argument("--resume", help="中断セッションの再開 (session_id)")
    parser.add_argument("--jvlink-db", help="JVLink DBパス")
    parser.add_argument("--ext-db", help="拡張DBパス")
    args = parser.parse_args()

    # ログ設定
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add(
        "logs/model_search_{time:YYYYMMDD_HHmmss}.log",
        level="DEBUG",
        rotation="100 MB",
    )

    # DB設定
    jvlink_path = args.jvlink_db
    ext_path = args.ext_db
    if not jvlink_path or not ext_path:
        try:
            import yaml
            config_path = Path("config/config.yaml")
            if config_path.exists():
                with open(config_path) as f:
                    cfg = yaml.safe_load(f)
                db_cfg = cfg.get("database", {})
                jvlink_path = jvlink_path or db_cfg.get("jvlink_db_path", "./data/jvlink.db")
                ext_path = ext_path or db_cfg.get("extension_db_path", "./data/extension.db")
        except Exception:
            pass
    jvlink_path = jvlink_path or "./data/jvlink.db"
    ext_path = ext_path or "./data/extension.db"

    if not Path(jvlink_path).exists():
        logger.error(f"JVLink DBが見つかりません: {jvlink_path}")
        sys.exit(1)

    from src.data.db import DatabaseManager
    from src.search.config import SearchConfig
    from src.search.orchestrator import ModelSearchOrchestrator

    jvlink_db = DatabaseManager(jvlink_path)
    ext_db = DatabaseManager(ext_path)

    if args.resume:
        # セッション再開
        from src.search.result_store import ResultStore

        store = ResultStore(ext_db)
        session = store.get_session(args.resume)
        if not session:
            logger.error(f"セッション {args.resume} が見つかりません")
            sys.exit(1)

        search_config = SearchConfig(
            session_id=args.resume,
            date_from=session["date_from"],
            date_to=session["date_to"],
            n_trials=session["n_trials"],
            initial_bankroll=session["initial_bankroll"],
            random_seed=session.get("random_seed", 42),
            mc_simulations=args.mc_sims,
        )
        orchestrator = ModelSearchOrchestrator(jvlink_db, ext_db, search_config)
        summary = orchestrator.resume(args.resume)
    else:
        # 新規探索
        if not args.date_from or not args.date_to:
            logger.error("--date-from と --date-to を指定してください")
            sys.exit(1)

        search_config = SearchConfig(
            date_from=args.date_from,
            date_to=args.date_to,
            n_trials=args.n_trials,
            initial_bankroll=args.bankroll,
            random_seed=args.seed,
            mc_simulations=args.mc_sims,
        )

        orchestrator = ModelSearchOrchestrator(jvlink_db, ext_db, search_config)
        summary = orchestrator.run()

    # 最終結果
    if summary.best_trial:
        logger.info(
            f"最優秀構成: composite_score={summary.best_trial.get('composite_score', 0):.1f}"
        )
    logger.info(f"探索完了: {summary.completed_trials}/{summary.total_trials}トライアル")


if __name__ == "__main__":
    main()
