"""デモシナリオスクリプト。

ダミーデータを使用して、システムの全レイヤーを通して動作するデモを実行する。

シナリオ:
    1. データ品質チェック（JVLinkテーブル検証）
    2. ファクタールール登録・ライフサイクル遷移
    3. スコアリング（全レースのGY指数計算）
    4. バックテスト（模擬戦略で過去検証）
    5. 投票シミュレーション（Quarter Kelly + 安全機構）

Usage:
    python scripts/seed_dummy_data.py           # まずダミーデータを生成
    python scripts/demo_scenario.py             # シナリオ実行
    python scripts/demo_scenario.py --db-path ./data/demo.db
"""

import sys
from pathlib import Path
from typing import Any

# プロジェクトルートをsys.pathに追加（直接実行対応）
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from loguru import logger

# ログ設定: INFOレベル以上を表示
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level:<8} | {message}")


def _separator(title: str) -> None:
    """セクション区切りを表示する。"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def step1_data_validation(db_path: str) -> bool:
    """Step 1: データ品質チェック。"""
    _separator("Step 1: データ品質チェック")

    from src.data.db import DatabaseManager
    from src.data.validator import DataValidator

    db = DatabaseManager(db_path, wal_mode=False)
    validator = DataValidator(db)

    # 必須テーブル存在確認
    missing = validator.check_required_tables()
    if missing:
        print(f"[NG] 不足テーブル: {missing}")
        return False
    print("[OK] 全必須テーブル存在確認")

    # テーブル品質チェック
    report = validator.run_full_check()
    for table, info in report["table_validations"].items():
        status = "OK" if info["is_valid"] else "NG"
        print(f"  [{status}] {table}: {info['total_records']:,}件")

    return True


def step2_factor_management(db_path: str) -> list[int]:
    """Step 2: ファクタールール登録・ライフサイクル。"""
    _separator("Step 2: ファクタールール管理")

    from src.data.db import DatabaseManager
    from src.factors.registry import FactorRegistry

    db = DatabaseManager(db_path, wal_mode=False)
    registry = FactorRegistry(db)

    # ファクタールール作成
    rules = [
        {"rule_name": "前走着順加点", "category": "form", "weight": 1.5,
         "description": "前走3着以内の馬に加点"},
        {"rule_name": "距離適性", "category": "course", "weight": 1.2,
         "description": "適距離範囲内の馬に加点"},
        {"rule_name": "騎手リーディング", "category": "jockey", "weight": 1.0,
         "description": "リーディング上位騎手に加点"},
        {"rule_name": "スピード指数", "category": "speed", "weight": 2.0,
         "description": "スピード指数80以上の馬に加点"},
        {"rule_name": "馬場適性", "category": "track", "weight": 0.8,
         "description": "良馬場での成績が良い馬に加点"},
    ]

    rule_ids = []
    for rule_data in rules:
        rule_id = registry.create_rule(rule_data)
        rule_ids.append(rule_id)
        print(f"  [作成] {rule_data['rule_name']} (ID: {rule_id}, weight: {rule_data['weight']})")

    # DRAFT → TESTING → APPROVED のライフサイクル遷移
    for rule_id in rule_ids:
        registry.transition_status(rule_id, "TESTING", reason="デモ: テスト開始")
        registry.transition_status(rule_id, "APPROVED", reason="デモ: 承認")

    active = registry.get_active_rules()
    print(f"\n  [結果] アクティブルール: {len(active)}件")

    # 1件の重み更新デモ
    registry.update_weight(rule_ids[0], 1.8, reason="バックテスト結果に基づく調整")
    print(f"  [更新] ルール {rule_ids[0]} weight: 1.5 → 1.8")

    return rule_ids


def step3_scoring(db_path: str) -> list[dict[str, Any]]:
    """Step 3: スコアリング（全レースのGY指数計算）。"""
    _separator("Step 3: GY指数スコアリング")

    from src.data.db import DatabaseManager
    from src.data.provider import JVLinkDataProvider
    from src.scoring.engine import ScoringEngine

    db = DatabaseManager(db_path, wal_mode=False)
    provider = JVLinkDataProvider(db)
    engine = ScoringEngine(db, calibrator=None, ev_threshold=1.05)

    # 全レース取得（providerの正規化API経由）
    races = provider.get_race_list(limit=500)
    print(f"対象レース数: {len(races)}レース\n")

    all_value_bets = []
    value_bet_count = 0

    for race_row in races[:12]:  # デモは1日分（12R）のみ
        race_key = JVLinkDataProvider.build_race_key(race_row)
        entries = provider.get_race_entries(race_key)
        odds_map = provider.get_odds(race_key)

        # スコアリング実行
        results = engine.score_race(dict(race_row), entries, odds_map)

        # バリューベット表示
        value_bets = [r for r in results if r.get("is_value_bet")]
        value_bet_count += len(value_bets)
        all_value_bets.extend(value_bets)

        race_name = race_row.get("RaceName", "")
        race_num = race_row.get("RaceNum", "")
        print(f"  {race_num}R {race_name:<12} "
              f"出走{len(entries):2d}頭 / EV>1.05: {len(value_bets):2d}頭 "
              f"/ 最高EV={results[0]['expected_value']:.3f}" if results else "")

    print(f"\n  [結果] バリューベット候補合計: {value_bet_count}頭")
    return all_value_bets


def step4_backtest(db_path: str) -> None:
    """Step 4: バックテスト（模擬戦略）。"""
    _separator("Step 4: バックテスト")

    from src.data.db import DatabaseManager
    from src.data.provider import JVLinkDataProvider
    from src.backtest.engine import BacktestConfig, BacktestEngine
    from src.strategy.base import Bet, Strategy

    class DemoStrategy(Strategy):
        """デモ用のシンプル戦略。EV > 1.0 かつオッズ3.0以上の馬に投票。"""

        def name(self) -> str:
            return "demo_value_strategy"

        def version(self) -> str:
            return "0.1.0"

        def run(
            self,
            race_data: dict[str, Any],
            entries: list[dict[str, Any]],
            odds: dict[str, float],
            bankroll: int,
            params: dict[str, Any],
        ) -> list[Bet]:
            bets = []
            for entry in entries:
                umaban = entry.get("Umaban", "")
                entry_odds = odds.get(umaban, 0.0)
                if entry_odds >= 3.0:
                    # DM予想順位ベースのEV推定
                    dm_rank = int(entry.get("DMJyuni", 10) or 10)
                    est_prob = max(0.05, min(0.4, (20 - dm_rank) / 50.0))
                    ev = est_prob * entry_odds
                    if ev > 1.0:
                        stake = min(bankroll // 50, 5000)
                        stake = (stake // 100) * 100
                        if stake >= 100:
                            bets.append(Bet(
                                race_key=race_data.get("race_key", ""),
                                bet_type="WIN",
                                selection=umaban,
                                stake_yen=stake,
                                est_prob=est_prob,
                                odds_at_bet=entry_odds,
                                est_ev=ev,
                                factor_details={"dm_rank": dm_rank},
                            ))
            return bets[:3]  # 1レース最大3頭

    # レースデータ構築
    db = DatabaseManager(db_path, wal_mode=False)
    provider = JVLinkDataProvider(db)
    races_raw = provider.get_race_list(limit=500)

    backtest_races = []
    for race_row in races_raw:
        race_key = JVLinkDataProvider.build_race_key(race_row)
        entries = provider.get_race_entries(race_key)
        odds_map = provider.get_odds(race_key)

        race_info = dict(race_row)
        race_info["race_key"] = race_key

        backtest_races.append({
            "race_info": race_info,
            "entries": [dict(e) for e in entries],
            "odds": odds_map,
        })

    # バックテスト実行
    strategy = DemoStrategy()
    engine = BacktestEngine(strategy)
    config = BacktestConfig(
        date_from="2025-01-05",
        date_to="2025-01-12",
        initial_bankroll=1_000_000,
        strategy_version="demo_0.1.0",
    )

    result = engine.run(backtest_races, config)

    print(f"  戦略: {strategy.name()} v{strategy.version()}")
    print(f"  期間: {config.date_from} 〜 {config.date_to}")
    print(f"  初期資金: {config.initial_bankroll:,}円")
    print(f"  対象レース: {result.total_races}レース")
    print(f"  総ベット数: {result.total_bets}件")
    print(f"  総投票額: {result.metrics.total_stake:,}円")
    print(f"  ROI: {result.metrics.roi:.2%}")


def step5_betting_simulation(db_path: str) -> None:
    """Step 5: 投票シミュレーション（Quarter Kelly + 安全機構）。"""
    _separator("Step 5: 投票シミュレーション")

    import random
    from src.betting.bankroll import BankrollManager, BettingMethod
    from src.betting.safety import SafetyGuard

    random.seed(42)

    # 初期設定
    bankroll = BankrollManager(
        initial_balance=1_000_000,
        method=BettingMethod.QUARTER_KELLY,
        max_daily_rate=0.20,
        max_per_race_rate=0.05,
        drawdown_cutoff=0.30,
    )
    safety = SafetyGuard(
        max_consecutive_losses=20,
        max_daily_loss=200_000,
        odds_deviation_threshold=0.30,
    )

    print(f"  初期資金: {bankroll.current_balance:,}円")
    print(f"  方式: Quarter Kelly")
    print(f"  ドローダウン閾値: 30%")
    print()

    # 12レース分のシミュレーション
    total_stake = 0
    total_payout = 0

    for race_num in range(1, 13):
        # 安全チェック
        can_bet, reason = safety.check_can_bet()
        if not can_bet:
            print(f"  {race_num:2d}R: [停止] {reason}")
            break

        # 擬似投票対象（EV = prob × odds > 1.05）
        est_prob = random.uniform(0.10, 0.35)
        odds = round(random.uniform(2.0, 15.0), 1)
        ev = est_prob * odds

        if ev <= 1.05:
            print(f"  {race_num:2d}R: [見送り] EV={ev:.3f} <= 1.05")
            continue

        # 投票額算出
        stake = bankroll.calculate_stake(est_prob, odds)
        if stake == 0:
            print(f"  {race_num:2d}R: [見送り] 投票額=0円")
            continue

        # 二重投票チェック
        race_key = f"2025010506010{race_num:02d}"
        selection = f"{random.randint(1, 18):02d}"
        if safety.check_duplicate_bet(race_key, selection):
            continue

        # 投票実行
        bankroll.record_bet(stake)
        safety.register_bet(race_key, selection)
        total_stake += stake

        # 結果判定（30%の確率で的中）
        is_win = random.random() < 0.30
        if is_win:
            payout = int(stake * odds)
            bankroll.record_payout(payout)
            total_payout += payout
            safety.record_result(True, payout - stake)
            result_str = f"的中! 払戻={payout:,}円"
        else:
            safety.record_result(False, -stake)
            result_str = "不的中"

        print(
            f"  {race_num:2d}R: 投票{stake:,}円 (odds={odds:.1f}, EV={ev:.3f}) → {result_str} "
            f"[残高: {bankroll.current_balance:,}円, DD: {bankroll.current_drawdown:.1%}]"
        )

    # 結果サマリー
    pnl = total_payout - total_stake
    roi = pnl / total_stake if total_stake > 0 else 0.0
    print(f"\n  --- シミュレーション結果 ---")
    print(f"  最終残高: {bankroll.current_balance:,}円")
    print(f"  総投票額: {total_stake:,}円")
    print(f"  総払戻額: {total_payout:,}円")
    print(f"  損益: {pnl:+,}円")
    print(f"  ROI: {roi:+.2%}")
    print(f"  最大ドローダウン: {bankroll.current_drawdown:.2%}")
    print(f"  緊急停止: {'あり' if safety.is_stopped else 'なし'}")


def main(db_path: str = "./data/demo.db") -> None:
    """全シナリオを順次実行する。"""
    print("\n" + "=" * 60)
    print("  Keiba Data Analytics — デモシナリオ")
    print("  GY指数方式バリュー投資戦略 (by Go Yoshizawa)")
    print("=" * 60)

    path = Path(db_path)
    if not path.exists():
        print(f"\n[エラー] DBファイルが見つかりません: {path}")
        print("先にダミーデータを生成してください:")
        print("  python scripts/seed_dummy_data.py")
        sys.exit(1)

    # Step 1: データ品質チェック
    if not step1_data_validation(db_path):
        print("\n[中断] データ品質チェックに失敗しました")
        sys.exit(1)

    # Step 2: ファクタールール管理
    step2_factor_management(db_path)

    # Step 3: スコアリング
    step3_scoring(db_path)

    # Step 4: バックテスト
    step4_backtest(db_path)

    # Step 5: 投票シミュレーション
    step5_betting_simulation(db_path)

    _separator("デモ完了")
    print("全シナリオが正常に完了しました。")
    print(f"DB: {Path(db_path).resolve()}")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "./data/demo.db"
    main(target)
