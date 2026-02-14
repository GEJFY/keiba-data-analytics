"""Walk-Forward バックテストエンジン。

仕様書 Section 9.2 に基づくWalk-Forward検証:
1. 全期間を N 個の Window に分割
2. 各 Window で train → test を順次実行
3. Window ごとに Weight最適化 → テスト期間でバックテスト
4. オーバーフィッティング検出のため train vs test の ROI を比較
"""

from dataclasses import dataclass
from typing import Any

from loguru import logger

from src.backtest.engine import BacktestConfig, BacktestEngine, BacktestResult
from src.backtest.metrics import BacktestMetrics, calculate_metrics
from src.strategy.base import Strategy


@dataclass
class WalkForwardWindow:
    """Walk-Forwardの1ウィンドウ。"""
    window_id: int
    train_from: str  # YYYYMMDD or YYYY-MM-DD
    train_to: str
    test_from: str
    test_to: str
    train_result: BacktestResult | None = None
    test_result: BacktestResult | None = None

    @property
    def train_roi(self) -> float:
        return self.train_result.metrics.roi if self.train_result else 0.0

    @property
    def test_roi(self) -> float:
        return self.test_result.metrics.roi if self.test_result else 0.0

    @property
    def overfitting_ratio(self) -> float:
        """過学習度合い。train_roi / test_roi が大きいほど過学習。"""
        if self.test_roi == 0:
            return float('inf') if self.train_roi > 0 else 0.0
        return self.train_roi / self.test_roi if self.test_roi != 0 else 0.0


@dataclass
class WalkForwardResult:
    """Walk-Forward検証の全体結果。"""
    windows: list[WalkForwardWindow]
    aggregate_metrics: BacktestMetrics | None = None
    avg_train_roi: float = 0.0
    avg_test_roi: float = 0.0
    avg_overfitting_ratio: float = 0.0
    total_train_bets: int = 0
    total_test_bets: int = 0

    @property
    def is_overfitting(self) -> bool:
        """過学習の兆候があるかどうか。train_roi が test_roi の2倍以上なら警告。"""
        return self.avg_overfitting_ratio > 2.0 if self.avg_overfitting_ratio != float('inf') else True


class WalkForwardEngine:
    """Walk-Forwardバックテストエンジン。"""

    def __init__(self, strategy: Strategy) -> None:
        self._strategy = strategy

    @staticmethod
    def generate_windows(
        date_from: str,
        date_to: str,
        n_windows: int = 5,
        train_ratio: float = 0.7,
    ) -> list[WalkForwardWindow]:
        """期間をN個のWalk-Forwardウィンドウに分割する。

        Args:
            date_from: 全体開始日 (YYYYMMDD)
            date_to: 全体終了日 (YYYYMMDD)
            n_windows: ウィンドウ数
            train_ratio: 各ウィンドウ内のtrain期間比率

        Returns:
            WalkForwardWindowのリスト
        """
        # YYYYMMDD → date オブジェクトに変換
        from datetime import timedelta

        d_from = _parse_date(date_from)
        d_to = _parse_date(date_to)

        if not (0 < train_ratio < 1.0):
            raise ValueError(
                f"train_ratioは0〜1の範囲(両端除く)で指定してください: {train_ratio}"
            )

        total_days = (d_to - d_from).days
        if total_days < n_windows * 30:
            raise ValueError(
                f"期間が短すぎます: {total_days}日 (最低{n_windows * 30}日必要)"
            )

        # Expanding window方式: 各ウィンドウでテスト期間を前にずらす
        test_days = total_days // (n_windows + int(n_windows * train_ratio))
        if test_days < 7:
            test_days = 7

        windows = []
        for i in range(n_windows):
            # テスト期間を等分配置
            test_end = d_to - timedelta(days=i * test_days)
            test_start = test_end - timedelta(days=test_days - 1)

            # 訓練期間はテスト開始日の前日まで
            train_end = test_start - timedelta(days=1)
            train_days_count = int(test_days / (1 - train_ratio) * train_ratio)
            train_start = max(d_from, train_end - timedelta(days=train_days_count))

            if train_start >= train_end:
                continue

            windows.append(WalkForwardWindow(
                window_id=i + 1,
                train_from=train_start.strftime("%Y%m%d"),
                train_to=train_end.strftime("%Y%m%d"),
                test_from=test_start.strftime("%Y%m%d"),
                test_to=test_end.strftime("%Y%m%d"),
            ))

        # 古い順にソート
        windows.sort(key=lambda w: w.test_from)
        for i, w in enumerate(windows):
            w.window_id = i + 1

        return windows

    def run(
        self,
        races: list[dict[str, Any]],
        windows: list[WalkForwardWindow],
        initial_bankroll: int = 1_000_000,
    ) -> WalkForwardResult:
        """Walk-Forwardバックテストを実行する。

        Args:
            races: 全レースデータ
            windows: Walk-Forwardウィンドウリスト
            initial_bankroll: 初期資金

        Returns:
            WalkForwardResult
        """
        all_test_bets = []

        for window in windows:
            logger.info(
                f"Window {window.window_id}: "
                f"train={window.train_from}~{window.train_to}, "
                f"test={window.test_from}~{window.test_to}"
            )

            # レースを期間でフィルタ
            train_races = _filter_races(races, window.train_from, window.train_to)
            test_races = _filter_races(races, window.test_from, window.test_to)

            # 訓練期間バックテスト
            if train_races:
                engine = BacktestEngine(self._strategy)
                config = BacktestConfig(
                    date_from=window.train_from,
                    date_to=window.train_to,
                    initial_bankroll=initial_bankroll,
                )
                window.train_result = engine.run(train_races, config)

            # テスト期間バックテスト（訓練期間の重複ファクターを除外）
            if test_races:
                engine = BacktestEngine(self._strategy)
                config = BacktestConfig(
                    date_from=window.test_from,
                    date_to=window.test_to,
                    initial_bankroll=initial_bankroll,
                    exclude_overlapping_factors=True,
                )
                window.test_result = engine.run(test_races, config)
                all_test_bets.extend(window.test_result.bets)

            logger.info(
                f"  train: {window.train_result.total_bets if window.train_result else 0}bets, "
                f"ROI={window.train_roi:+.1%} | "
                f"test: {window.test_result.total_bets if window.test_result else 0}bets, "
                f"ROI={window.test_roi:+.1%}"
            )

        # 集計
        train_rois = [w.train_roi for w in windows if w.train_result]
        test_rois = [w.test_roi for w in windows if w.test_result]
        of_ratios = [
            w.overfitting_ratio for w in windows
            if w.train_result and w.test_result and w.overfitting_ratio != float('inf')
        ]

        avg_train = sum(train_rois) / len(train_rois) if train_rois else 0.0
        avg_test = sum(test_rois) / len(test_rois) if test_rois else 0.0
        avg_of = sum(of_ratios) / len(of_ratios) if of_ratios else 0.0

        # テスト期間のベットで全体メトリクスを算出
        aggregate = calculate_metrics(all_test_bets, initial_bankroll) if all_test_bets else None

        result = WalkForwardResult(
            windows=windows,
            aggregate_metrics=aggregate,
            avg_train_roi=avg_train,
            avg_test_roi=avg_test,
            avg_overfitting_ratio=avg_of,
            total_train_bets=sum(
                w.train_result.total_bets for w in windows if w.train_result
            ),
            total_test_bets=sum(
                w.test_result.total_bets for w in windows if w.test_result
            ),
        )

        logger.info(
            f"Walk-Forward完了: {len(windows)}ウィンドウ, "
            f"avg_train_ROI={avg_train:+.1%}, avg_test_ROI={avg_test:+.1%}, "
            f"overfitting_ratio={avg_of:.2f}"
        )

        return result


def _parse_date(date_str: str):
    """YYYYMMDD or YYYY-MM-DD → date"""
    from datetime import date
    s = date_str.replace("-", "")
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def _filter_races(
    races: list[dict[str, Any]],
    date_from: str,
    date_to: str,
) -> list[dict[str, Any]]:
    """レースリストを日付範囲でフィルタする。"""
    d_from = date_from.replace("-", "")
    d_to = date_to.replace("-", "")
    result = []
    for race in races:
        info = race.get("race_info", {})
        race_date = f"{info.get('Year', '')}{info.get('MonthDay', '')}"
        if d_from <= race_date <= d_to:
            result.append(race)
    return result
