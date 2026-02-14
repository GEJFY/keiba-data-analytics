"""ファクターWeight最適化CLIスクリプト。

Usage:
    python scripts/optimize_weights.py [--date-from YYYYMMDD] [--date-to YYYYMMDD]
                                       [--max-races 5000] [--apply] [--train-calibrator]
                                       [--importance]

Examples:
    # Weight最適化の確認（適用なし）
    python scripts/optimize_weights.py --date-from 20240101 --date-to 20241231

    # 最適化結果をDBに適用
    python scripts/optimize_weights.py --date-from 20240101 --date-to 20241231 --apply

    # キャリブレーター学習
    python scripts/optimize_weights.py --date-from 20240101 --train-calibrator

    # 特徴量重要度分析
    python scripts/optimize_weights.py --date-from 20240101 --importance
"""

import argparse
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.dashboard.config_loader import load_config, get_db_managers


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ファクターWeight最適化・キャリブレーター学習"
    )
    parser.add_argument("--date-from", default="", help="開始日 YYYYMMDD")
    parser.add_argument("--date-to", default="", help="終了日 YYYYMMDD")
    parser.add_argument("--max-races", type=int, default=5000, help="最大レース数")
    parser.add_argument("--target-jyuni", type=int, default=1, help="的中着順 (1=1着)")
    parser.add_argument("--apply", action="store_true", help="最適化結果をDBに反映")
    parser.add_argument("--train-calibrator", action="store_true", help="キャリブレーター学習")
    parser.add_argument("--calibrator-method", default="platt", help="platt or isotonic")
    parser.add_argument("--importance", action="store_true", help="特徴量重要度分析")
    args = parser.parse_args()

    config = load_config()
    jvlink_db, ext_db = get_db_managers(config)

    if args.importance:
        _run_importance(jvlink_db, ext_db, args)
    elif args.train_calibrator:
        _run_train_calibrator(jvlink_db, ext_db, args)
    else:
        _run_optimize(jvlink_db, ext_db, args)


def _run_optimize(jvlink_db, ext_db, args) -> None:
    """Weight最適化を実行する。"""
    from src.scoring.weight_optimizer import WeightOptimizer

    optimizer = WeightOptimizer(jvlink_db, ext_db)

    print("\n" + "=" * 60)
    print("  Weight最適化")
    print("=" * 60)
    print(f"  期間: {args.date_from or '全期間'} - {args.date_to or '全期間'}")
    print(f"  最大レース数: {args.max_races}")
    print(f"  対象着順: {args.target_jyuni}着以内")
    print()

    result = optimizer.optimize(
        date_from=args.date_from,
        date_to=args.date_to,
        max_races=args.max_races,
        target_jyuni=args.target_jyuni,
    )

    print(f"  サンプル数: {result['n_samples']} ({result['n_positive']}的中)")
    print(f"  Accuracy: {result['accuracy']:.4f}")
    print(f"  Log Loss: {result['log_loss']:.4f}")
    print()

    # Weight比較テーブル
    print(f"  {'ファクター名':<24} {'現在':>8} {'最適':>8} {'変化':>8}")
    print("  " + "-" * 52)
    for name in result["weights"]:
        current = result["current_weights"].get(name, 1.0)
        optimized = result["weights"][name]
        diff = optimized - current
        sign = "+" if diff > 0 else ""
        print(f"  {name:<24} {current:>8.2f} {optimized:>8.2f} {sign}{diff:>7.2f}")

    print()

    if args.apply:
        updated = optimizer.apply_weights(result["weights"])
        print(f"  [OK] {updated}ルールのWeightをDBに反映しました")
    else:
        print("  --apply を指定すると結果をDBに反映します")

    print()


def _run_train_calibrator(jvlink_db, ext_db, args) -> None:
    """キャリブレーター学習を実行する。"""
    from src.scoring.calibration_trainer import CalibrationTrainer

    trainer = CalibrationTrainer(jvlink_db, ext_db)

    print("\n" + "=" * 60)
    print("  キャリブレーター学習")
    print("=" * 60)
    print(f"  方式: {args.calibrator_method}")
    print()

    calibrator = trainer.train(
        method=args.calibrator_method,
        target_jyuni=args.target_jyuni,
        min_samples=50,
        use_batch=True,
        date_from=args.date_from,
        date_to=args.date_to,
        max_races=args.max_races,
    )

    # 評価
    scores, labels = trainer.build_training_data_from_batch(
        args.date_from, args.date_to, args.max_races,
        args.target_jyuni, min_samples=10,
    )
    import numpy as np
    probs = np.array([calibrator.predict_proba(s) for s in scores])
    brier = float(np.mean((probs - labels) ** 2))
    print(f"  Brier Score: {brier:.4f}")

    # 保存
    save_path = project_root / "data" / "calibrator.joblib"
    calibrator.save(save_path)
    print(f"  [OK] 保存先: {save_path}")
    print()


def _run_importance(jvlink_db, ext_db, args) -> None:
    """特徴量重要度分析を実行する。"""
    from src.scoring.feature_importance import FeatureImportanceAnalyzer

    analyzer = FeatureImportanceAnalyzer(jvlink_db, ext_db)

    print("\n" + "=" * 60)
    print("  特徴量重要度分析")
    print("=" * 60)
    print()

    result = analyzer.analyze(
        date_from=args.date_from,
        date_to=args.date_to,
        max_races=args.max_races,
        target_jyuni=args.target_jyuni,
    )

    print(f"  サンプル数: {result['n_samples']}")
    print(f"  ベースライン精度: {result['baseline_accuracy']:.4f}")
    print()

    print(f"  {'ファクター名':<24} {'PI':>8} {'HitRate':>8} {'Lift':>6} {'Act%':>6} {'Corr':>6}")
    print("  " + "-" * 62)
    for f in result["factors"]:
        print(
            f"  {f['rule_name']:<24} "
            f"{f['permutation_importance']:>8.4f} "
            f"{f['hit_rate_with']:>7.1%} "
            f"{f['lift']:>6.2f} "
            f"{f['activation_rate']:>5.1%} "
            f"{f['correlation']:>6.3f}"
        )

    print()


if __name__ == "__main__":
    main()
