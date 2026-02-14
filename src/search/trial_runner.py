"""トライアル実行エンジン。

1つのパラメータ構成に対して、Walk-Forward検証 + Monte Carloシミュレーションを
実行し、複合スコアを算出する。**本番DBを変更しない。**
"""

import time
from typing import Any

import numpy as np
from loguru import logger
from numpy.typing import NDArray

from src.backtest.engine import BacktestConfig, BacktestEngine, BacktestResult
from src.backtest.metrics import BacktestMetrics, calculate_metrics
from src.backtest.monte_carlo import MonteCarloSimulator
from src.backtest.walk_forward import WalkForwardEngine, _filter_races, _parse_date
from src.betting.bankroll import BankrollManager, BettingMethod
from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider
from src.factors.registry import FactorRegistry
from src.scoring.batch_scorer import BatchScorer
from src.scoring.calibration import (
    IsotonicCalibrator,
    PlattCalibrator,
    ProbabilityCalibrator,
)
from src.scoring.engine import ScoringEngine
from src.scoring.evaluator import evaluate_rule
from src.search.config import (
    SearchConfig,
    TrialConfig,
    TrialResult,
    calculate_composite_score,
)
from src.strategy.base import Bet, Strategy


class TrialScoringEngine:
    """インメモリweightsでスコアリングするエンジン。

    ScoringEngineと同じロジックだが、DBではなくインメモリのルール・校正器を使用する。
    """

    BASE_SCORE = 100

    def __init__(
        self,
        rules: list[dict[str, Any]],
        calibrator: ProbabilityCalibrator | None = None,
        ev_threshold: float = 1.05,
        jvlink_provider: JVLinkDataProvider | None = None,
    ) -> None:
        self._rules = rules
        self._calibrator = calibrator
        self._ev_threshold = ev_threshold
        self._provider = jvlink_provider
        self._fallback_warned = False

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def score_race(
        self,
        race: dict[str, Any],
        entries: list[dict[str, Any]],
        odds_map: dict[str, float],
        race_key: str = "",
    ) -> list[dict[str, Any]]:
        """1レース全馬のスコアとEVを計算する（インメモリルール使用）。"""
        results = []

        # 前走データ取得
        prev_contexts: list[dict[str, Any] | None] = []
        all_prev_l3f: list[float] | None = None
        if self._provider and race_key:
            l3f_list: list[float] = []
            for horse in entries:
                ketto = str(horse.get("KettoNum", ""))
                prev = (
                    self._provider.get_previous_race_entry(ketto, race_key)
                    if ketto else None
                )
                prev_contexts.append(prev)
                l3f_list.append(
                    self._safe_float(prev.get("HaronTimeL3", 0)) if prev else 0.0
                )
            all_prev_l3f = l3f_list
        else:
            prev_contexts = [None] * len(entries)

        for i, horse in enumerate(entries):
            total_score = self.BASE_SCORE
            factor_details: dict[str, float] = {}
            for rule in self._rules:
                expression = rule.get("sql_expression", "")
                raw = evaluate_rule(
                    expression, horse, race, entries,
                    prev_context=prev_contexts[i],
                    all_prev_l3f=all_prev_l3f,
                )
                weighted = raw * rule.get("weight", 1.0)
                total_score += weighted
                factor_details[rule["rule_name"]] = weighted

            umaban = str(horse.get("Umaban", ""))
            actual_odds = odds_map.get(umaban, 0.0)
            if actual_odds <= 0:
                continue

            # 確率校正
            if self._calibrator:
                estimated_prob = self._calibrator.predict_proba(total_score)
            else:
                if not self._fallback_warned:
                    self._fallback_warned = True
                estimated_prob = max(0.01, min(0.99, total_score / 200.0))

            fair_odds = 1.0 / estimated_prob if estimated_prob > 0 else float("inf")
            expected_value = estimated_prob * actual_odds

            results.append({
                "umaban": umaban,
                "total_score": total_score,
                "factor_details": factor_details,
                "estimated_prob": estimated_prob,
                "fair_odds": fair_odds,
                "actual_odds": actual_odds,
                "expected_value": expected_value,
                "is_value_bet": expected_value > self._ev_threshold,
            })

        return sorted(results, key=lambda x: x["expected_value"], reverse=True)


class TrialStrategy(Strategy):
    """探索用の一時戦略。DBを変更しない。"""

    def __init__(
        self,
        rules: list[dict[str, Any]],
        calibrator: ProbabilityCalibrator | None = None,
        ev_threshold: float = 1.05,
        max_bets_per_race: int = 3,
        betting_method: str = "quarter_kelly",
        jvlink_provider: JVLinkDataProvider | None = None,
    ) -> None:
        self._engine = TrialScoringEngine(
            rules, calibrator, ev_threshold, jvlink_provider,
        )
        self._ev_threshold = ev_threshold
        self._max_bets = max_bets_per_race
        method_map = {
            "quarter_kelly": BettingMethod.QUARTER_KELLY,
            "equal": BettingMethod.EQUAL,
        }
        self._method = method_map.get(betting_method, BettingMethod.QUARTER_KELLY)

    def name(self) -> str:
        return "TRIAL_GY_VALUE"

    def version(self) -> str:
        return "search"

    def run(
        self,
        race_data: dict[str, Any],
        entries: list[dict[str, Any]],
        odds: dict[str, float],
        bankroll: int,
        params: dict[str, Any],
    ) -> list[Bet]:
        if not entries or not odds:
            return []

        race_key = self._build_race_key(race_data)
        scored = self._engine.score_race(race_data, entries, odds, race_key)
        if not scored:
            return []

        value_bets = [r for r in scored if r.get("expected_value", 0) > self._ev_threshold]
        if not value_bets:
            return []

        value_bets = value_bets[:self._max_bets]

        bm = BankrollManager(
            initial_balance=bankroll,
            method=self._method,
            max_per_race_rate=0.05,
        )

        bets: list[Bet] = []
        for vb in value_bets:
            stake = bm.calculate_stake(
                estimated_prob=vb["estimated_prob"],
                odds=vb["actual_odds"],
            )
            if stake <= 0:
                continue
            bets.append(Bet(
                race_key=race_key,
                bet_type="WIN" if params.get("target_jyuni", 1) == 1 else "PLACE",
                selection=str(vb["umaban"]),
                stake_yen=stake,
                est_prob=vb["estimated_prob"],
                odds_at_bet=vb["actual_odds"],
                est_ev=vb["expected_value"],
                factor_details=vb.get("factor_details", {}),
            ))
            bm.record_bet(stake)

        return bets

    @staticmethod
    def _build_race_key(race_data: dict[str, Any]) -> str:
        parts = [
            str(race_data.get("Year", "")),
            str(race_data.get("MonthDay", "")),
            str(race_data.get("JyoCD", "")),
            str(race_data.get("Kaiji", "")),
            str(race_data.get("Nichiji", "")),
            str(race_data.get("RaceNum", "")),
        ]
        return "".join(parts)


class TrialRunner:
    """1トライアルを実行し、スコアを返す。"""

    def __init__(
        self,
        jvlink_db: DatabaseManager,
        ext_db: DatabaseManager,
    ) -> None:
        self._jvlink_db = jvlink_db
        self._ext_db = ext_db
        self._provider = JVLinkDataProvider(jvlink_db)
        self._registry = FactorRegistry(ext_db)
        self._batch_scorer = BatchScorer(jvlink_db, ext_db)

    def run(
        self,
        config: TrialConfig,
        search_config: SearchConfig,
        preloaded_races: list[dict[str, Any]] | None = None,
    ) -> TrialResult:
        """1トライアルを実行する。

        Args:
            config: トライアルパラメータ
            search_config: 探索全体設定
            preloaded_races: 事前ロード済みレースデータ（Noneで毎回取得）
        """
        t_start = time.perf_counter()
        result = TrialResult(config=config)

        try:
            # 1. ファクター選択
            rules = self._select_factors(
                config, search_config.date_from, search_config.date_to,
            )
            if not rules:
                result.error = "有効なファクターなし"
                result.elapsed_seconds = time.perf_counter() - t_start
                return result
            result.n_factors_used = len(rules)

            # 2. Walk-Forward窓生成
            from datetime import timedelta

            d_from = _parse_date(search_config.date_from)
            d_to = _parse_date(search_config.date_to)

            try:
                windows = WalkForwardEngine.generate_windows(
                    search_config.date_from, search_config.date_to,
                    n_windows=config.wf_n_windows,
                    train_ratio=0.7,
                )
            except ValueError as e:
                result.error = f"窓生成エラー: {e}"
                result.elapsed_seconds = time.perf_counter() - t_start
                return result

            # 3. レースデータ取得
            if preloaded_races is not None:
                all_races = preloaded_races
            else:
                all_races = self._load_races(
                    search_config.date_from, search_config.date_to,
                )

            if not all_races:
                result.error = "レースデータなし"
                result.elapsed_seconds = time.perf_counter() - t_start
                return result

            # 4. Walk-Forward実行
            all_test_bets: list[Bet] = []
            train_rois: list[float] = []
            test_rois: list[float] = []

            for window in windows:
                train_races = _filter_races(all_races, window.train_from, window.train_to)
                test_races = _filter_races(all_races, window.test_from, window.test_to)

                if not train_races or not test_races:
                    continue

                # 4a. 訓練期間でWeight最適化（インメモリ）
                optimized_rules = self._optimize_weights_inmemory(
                    rules, config, window.train_from, window.train_to,
                )

                # 4b. 校正器学習（インメモリ）
                calibrator = self._train_calibrator_inmemory(
                    optimized_rules, config, window.train_from, window.train_to,
                )

                # 4c. 訓練期間バックテスト
                train_strategy = TrialStrategy(
                    optimized_rules, calibrator,
                    ev_threshold=config.ev_threshold,
                    max_bets_per_race=config.max_bets_per_race,
                    betting_method=config.betting_method,
                    jvlink_provider=self._provider,
                )
                train_engine = BacktestEngine(train_strategy)
                train_config = BacktestConfig(
                    date_from=window.train_from,
                    date_to=window.train_to,
                    initial_bankroll=search_config.initial_bankroll,
                )
                train_result = train_engine.run(train_races, train_config)
                train_rois.append(train_result.metrics.roi)

                # 4d. テスト期間バックテスト
                test_strategy = TrialStrategy(
                    optimized_rules, calibrator,
                    ev_threshold=config.ev_threshold,
                    max_bets_per_race=config.max_bets_per_race,
                    betting_method=config.betting_method,
                    jvlink_provider=self._provider,
                )
                test_engine = BacktestEngine(test_strategy)
                test_config = BacktestConfig(
                    date_from=window.test_from,
                    date_to=window.test_to,
                    initial_bankroll=search_config.initial_bankroll,
                )
                test_result = test_engine.run(test_races, test_config)
                test_rois.append(test_result.metrics.roi)
                all_test_bets.extend(test_result.bets)

            # 5. 集約メトリクス計算
            if not all_test_bets:
                result.error = "テスト期間にベットなし"
                result.elapsed_seconds = time.perf_counter() - t_start
                return result

            metrics = calculate_metrics(all_test_bets, search_config.initial_bankroll)
            result.total_bets = len(all_test_bets)
            result.roi = metrics.roi
            result.sharpe_ratio = metrics.sharpe_ratio
            result.max_drawdown = metrics.max_drawdown
            result.win_rate = metrics.win_rate
            result.profit_factor = metrics.profit_factor
            result.calmar_ratio = metrics.calmar_ratio
            result.edge = metrics.edge

            # Walk-Forward統計
            result.wf_avg_train_roi = (
                sum(train_rois) / len(train_rois) if train_rois else 0.0
            )
            result.wf_avg_test_roi = (
                sum(test_rois) / len(test_rois) if test_rois else 0.0
            )
            if result.wf_avg_test_roi != 0:
                result.wf_overfitting_ratio = (
                    result.wf_avg_train_roi / result.wf_avg_test_roi
                )
            else:
                result.wf_overfitting_ratio = (
                    float("inf") if result.wf_avg_train_roi > 0 else 0.0
                )

            # 6. Monte Carloシミュレーション
            bet_pnls = []
            for bet in all_test_bets:
                # 推定PnL: (prob * odds - 1) * stake
                pnl = (bet.est_prob * bet.odds_at_bet - 1) * bet.stake_yen
                bet_pnls.append(pnl)

            if len(bet_pnls) >= 5:
                mc = MonteCarloSimulator(seed=42)
                mc_result = mc.run(
                    bet_pnls,
                    n_simulations=search_config.mc_simulations,
                    initial_bankroll=search_config.initial_bankroll,
                )
                result.mc_roi_5th = mc_result.roi_5th
                result.mc_roi_95th = mc_result.roi_95th
                result.mc_ruin_probability = mc_result.ruin_probability

            # 7. 複合スコア算出
            result.composite_score = calculate_composite_score(result)

        except Exception as e:
            result.error = str(e)
            logger.warning(f"Trial {config.trial_id} error: {e}")

        result.elapsed_seconds = time.perf_counter() - t_start
        return result

    def _select_factors(
        self,
        config: TrialConfig,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        """ファクター選択ロジック。"""
        all_rules = self._registry.get_active_rules()
        if not all_rules:
            return []

        selection = config.factor_selection

        if selection == "all":
            return all_rules

        if selection == "category_filtered":
            priority = {"odds", "speed", "pace", "weight", "form"}
            return [r for r in all_rules if r.get("category", "") in priority] or all_rules

        if selection in ("top10_auc", "top15_auc"):
            n_top = 10 if selection == "top10_auc" else 15
            try:
                from src.scoring.factor_discovery import FactorDiscovery

                fd = FactorDiscovery(self._jvlink_db, self._ext_db)
                disc = fd.discover(
                    date_from=date_from, date_to=date_to,
                    max_races=2000, min_auc=0.50,
                )
                top_names = {c["name"] for c in disc["candidates"][:n_top]}
                filtered = [r for r in all_rules if r["rule_name"] in top_names]
                return filtered if filtered else all_rules
            except Exception:
                return all_rules

        return all_rules

    def _optimize_weights_inmemory(
        self,
        rules: list[dict[str, Any]],
        config: TrialConfig,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        """インメモリでWeight最適化し、weightを更新したルールのコピーを返す。"""
        from sklearn.linear_model import LogisticRegression

        try:
            matrix = self._batch_scorer.build_factor_matrix(
                date_from.replace("-", ""),
                date_to.replace("-", ""),
                max_races=2000,
            )
        except ValueError:
            return rules

        X = matrix["X"]
        jyuni = matrix["jyuni"]
        factor_names = matrix["factor_names"]
        y = (jyuni <= config.target_jyuni).astype(np.int64)

        if int(y.sum()) < 10:
            return rules

        # 使用するファクターだけ列を選択
        rule_name_set = {r["rule_name"] for r in rules}
        col_indices = [i for i, n in enumerate(factor_names) if n in rule_name_set]
        selected_names = [factor_names[i] for i in col_indices]

        if not col_indices:
            return rules

        X_selected = X[:, col_indices]

        model = LogisticRegression(
            C=config.regularization,
            max_iter=500,
            solver="lbfgs",
            class_weight="balanced",
        )
        model.fit(X_selected, y)

        # 係数からweightを算出
        coefs = model.coef_[0]
        abs_coefs = np.abs(coefs)
        max_abs = abs_coefs.max() if abs_coefs.max() > 0 else 1.0
        weight_map = {}
        for name, coef in zip(selected_names, coefs):
            normalized = abs(coef) / max_abs * 3.0
            weight_map[name] = round(max(0.1, normalized), 2)

        # ルールのコピーを作成してweight更新
        updated_rules = []
        for rule in rules:
            r = dict(rule)
            if r["rule_name"] in weight_map:
                r["weight"] = weight_map[r["rule_name"]]
            updated_rules.append(r)

        return updated_rules

    def _train_calibrator_inmemory(
        self,
        rules: list[dict[str, Any]],
        config: TrialConfig,
        date_from: str,
        date_to: str,
    ) -> ProbabilityCalibrator | None:
        """インメモリで校正器を学習する。"""
        if config.calibration_method == "none":
            return None

        try:
            matrix = self._batch_scorer.build_factor_matrix(
                date_from.replace("-", ""),
                date_to.replace("-", ""),
                max_races=2000,
            )
        except ValueError:
            return None

        # スコア計算
        factor_names = matrix["factor_names"]
        X = matrix["X"]
        jyuni = matrix["jyuni"]
        y = (jyuni <= config.target_jyuni).astype(np.int64)

        rule_weight_map = {r["rule_name"]: r.get("weight", 1.0) for r in rules}
        scores = np.full(len(y), 100.0)
        for i, name in enumerate(factor_names):
            w = rule_weight_map.get(name, 0.0)
            scores += X[:, i] * w

        if config.calibration_method == "platt":
            cal = PlattCalibrator()
        else:
            cal = IsotonicCalibrator()

        try:
            cal.fit(scores, y)
            return cal
        except Exception:
            return None

    def _load_races(
        self,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        """レースデータをロードする（バッチクエリで高速化）。"""
        d_from = date_from.replace("-", "")
        d_to = date_to.replace("-", "")
        with self._jvlink_db.session():
            races = self._provider.fetch_races_batch(
                date_from=d_from,
                date_to=d_to,
                max_races=10000,
                include_payouts=True,
            )
        # オッズなしレースを除外
        return [r for r in races if r["odds"]]
