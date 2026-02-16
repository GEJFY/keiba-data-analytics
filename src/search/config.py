"""探索空間とトライアル構成の定義。"""

import uuid
from dataclasses import dataclass, field
from typing import Any

import numpy as np

# 探索空間の各次元定義
TRAIN_WINDOW_MONTHS = [3, 6, 9, 12, 18, 24]
EV_THRESHOLDS = [1.05, 1.10, 1.15, 1.20, 1.25, 1.30]
REGULARIZATIONS = [0.01, 0.1, 1.0, 10.0]
TARGET_JYUNI_OPTIONS = [1, 3]
CALIBRATION_METHODS = ["platt", "isotonic", "none"]
BETTING_METHODS = ["quarter_kelly", "equal"]
WF_N_WINDOWS_OPTIONS = [3, 5, 7]
MAX_BETS_PER_RACE_OPTIONS = [1, 2, 3]
FACTOR_SELECTIONS = ["all", "top10_auc", "top15_auc", "category_filtered"]


@dataclass
class TrialConfig:
    """1トライアルのパラメータ構成。"""

    trial_id: str
    train_window_months: int
    ev_threshold: float
    regularization: float
    target_jyuni: int
    calibration_method: str
    betting_method: str
    wf_n_windows: int
    max_bets_per_race: int
    factor_selection: str

    def to_dict(self) -> dict[str, Any]:
        """dict変換（DB保存用）。"""
        return {
            "trial_id": self.trial_id,
            "train_window_months": self.train_window_months,
            "ev_threshold": self.ev_threshold,
            "regularization": self.regularization,
            "target_jyuni": self.target_jyuni,
            "calibration_method": self.calibration_method,
            "betting_method": self.betting_method,
            "wf_n_windows": self.wf_n_windows,
            "max_bets_per_race": self.max_bets_per_race,
            "factor_selection": self.factor_selection,
        }


@dataclass
class TrialResult:
    """トライアル結果。"""

    config: TrialConfig
    # Walk-Forward結果
    wf_avg_test_roi: float = 0.0
    wf_avg_train_roi: float = 0.0
    wf_overfitting_ratio: float = 0.0
    # 集約メトリクス（テスト期間のみ）
    total_bets: int = 0
    roi: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    calmar_ratio: float = 0.0
    edge: float = 0.0
    # Monte Carlo
    mc_roi_5th: float = 0.0
    mc_roi_95th: float = 0.0
    mc_ruin_probability: float = 1.0
    # 複合スコア
    composite_score: float = 0.0
    # メタ
    n_factors_used: int = 0
    elapsed_seconds: float = 0.0
    error: str = ""


@dataclass
class SearchConfig:
    """探索全体の設定。"""

    session_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    date_from: str = ""
    date_to: str = ""
    n_trials: int = 500
    initial_bankroll: int = 1_000_000
    mc_simulations: int = 1000
    random_seed: int = 42
    early_stop_threshold: float = 0.5


class SearchSpace:
    """探索空間定義 + ランダムサンプリング。"""

    def __init__(self) -> None:
        self._dimensions = {
            "train_window_months": TRAIN_WINDOW_MONTHS,
            "ev_threshold": EV_THRESHOLDS,
            "regularization": REGULARIZATIONS,
            "target_jyuni": TARGET_JYUNI_OPTIONS,
            "calibration_method": CALIBRATION_METHODS,
            "betting_method": BETTING_METHODS,
            "wf_n_windows": WF_N_WINDOWS_OPTIONS,
            "max_bets_per_race": MAX_BETS_PER_RACE_OPTIONS,
            "factor_selection": FACTOR_SELECTIONS,
        }

    @property
    def total_combinations(self) -> int:
        """全組合せ数。"""
        result = 1
        for values in self._dimensions.values():
            result *= len(values)
        return result

    def get_dimensions(self) -> dict[str, list]:
        """探索空間の各次元を返す。"""
        return dict(self._dimensions)

    def sample(self, rng: np.random.Generator) -> TrialConfig:
        """ランダムに1構成をサンプリングする。"""
        return TrialConfig(
            trial_id=uuid.uuid4().hex[:12],
            train_window_months=int(rng.choice(TRAIN_WINDOW_MONTHS)),
            ev_threshold=float(rng.choice(EV_THRESHOLDS)),
            regularization=float(rng.choice(REGULARIZATIONS)),
            target_jyuni=int(rng.choice(TARGET_JYUNI_OPTIONS)),
            calibration_method=str(rng.choice(CALIBRATION_METHODS)),
            betting_method=str(rng.choice(BETTING_METHODS)),
            wf_n_windows=int(rng.choice(WF_N_WINDOWS_OPTIONS)),
            max_bets_per_race=int(rng.choice(MAX_BETS_PER_RACE_OPTIONS)),
            factor_selection=str(rng.choice(FACTOR_SELECTIONS)),
        )


def calculate_composite_score(result: TrialResult) -> float:
    """多目的評価を1つのスコアに集約する（100点満点）。

    配点:
    - Sharpe ratio (OOS):     30点  (0以下=0, 0.5=15, 1.0=30)
    - ROI (OOS):              25点  (0%以下=0, 5%=12.5, 10%=25)
    - Max drawdown:           15点  (30%以上=0, 15%=7.5, 0%=15)
    - 過学習抑制:             15点  (ratio>3.0=0, 1.0=15)
    - Monte Carlo安定性:      10点  (ruin>10%=0, 0%=10)
    - ベット数十分性:          5点  (0件=0, 100件以上=5)
    """
    score = 0.0

    # Sharpe ratio: 0→0点, 0.5→15点, 1.0→30点
    sharpe = max(0.0, result.sharpe_ratio)
    score += min(30.0, sharpe * 30.0)

    # ROI: 0%→0点, 5%→12.5点, 10%→25点
    roi_pct = max(0.0, result.roi * 100)
    score += min(25.0, roi_pct * 2.5)

    # Max drawdown: 0%→15点, 15%→7.5点, 30%→0点
    dd_pct = min(0.30, max(0.0, result.max_drawdown))
    score += 15.0 * (1.0 - dd_pct / 0.30)

    # 過学習抑制: ratio=1.0→15点, ratio=3.0→0点, inf→0点
    raw_of = result.wf_overfitting_ratio
    of_ratio = max(1.0, min(3.0, raw_of)) if raw_of != float("inf") else 3.0
    score += 15.0 * (1.0 - (of_ratio - 1.0) / 2.0)

    # Monte Carlo安定性: ruin=0%→10点, ruin=10%→0点
    ruin = min(0.10, max(0.0, result.mc_ruin_probability))
    score += 10.0 * (1.0 - ruin / 0.10)

    # ベット数十分性: 0件→0点, 100件以上→5点
    bets = min(100, max(0, result.total_bets))
    score += 5.0 * (bets / 100.0)

    return round(score, 2)
