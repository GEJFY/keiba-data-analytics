"""探索結果の最終レポート生成。"""

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from src.search.result_store import ResultStore


@dataclass
class SearchSummary:
    """探索結果のサマリー。"""

    session_id: str
    total_trials: int
    completed_trials: int
    error_trials: int
    best_trial: dict[str, Any] | None
    top_10_trials: list[dict[str, Any]]
    parameter_trends: dict[str, list[dict[str, Any]]]
    elapsed_total_seconds: float
    recommendation: str


class SearchReporter:
    """探索結果の最終レポート生成。"""

    def __init__(self, result_store: ResultStore) -> None:
        self._store = result_store

    def generate(self, session_id: str) -> SearchSummary:
        """最終レポートを生成する。"""
        all_trials = self._store.get_all_trials(session_id)
        top_10 = self._store.get_top_trials(session_id, limit=10)
        session = self._store.get_session(session_id)

        completed = [t for t in all_trials if not t.get("error")]
        errors = [t for t in all_trials if t.get("error")]

        trends = self._analyze_parameter_trends(completed)
        recommendation = self._build_recommendation(top_10, trends)

        return SearchSummary(
            session_id=session_id,
            total_trials=len(all_trials),
            completed_trials=len(completed),
            error_trials=len(errors),
            best_trial=top_10[0] if top_10 else None,
            top_10_trials=top_10,
            parameter_trends=trends,
            elapsed_total_seconds=session.get("total_elapsed_seconds", 0) if session else 0,
            recommendation=recommendation,
        )

    def _analyze_parameter_trends(
        self, trials: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        """各パラメータ次元ごとに平均スコアを算出する。"""
        params_to_analyze = [
            "train_window_months", "ev_threshold", "regularization",
            "target_jyuni", "calibration_method", "betting_method",
            "wf_n_windows", "max_bets_per_race", "factor_selection",
        ]

        trends: dict[str, list[dict[str, Any]]] = {}

        for param in params_to_analyze:
            value_scores: dict[str, list[float]] = defaultdict(list)
            for t in trials:
                val = str(t.get(param, ""))
                score = t.get("composite_score", 0) or 0
                value_scores[val].append(score)

            param_trend = []
            for val, scores in sorted(value_scores.items()):
                avg = sum(scores) / len(scores) if scores else 0
                param_trend.append({
                    "value": val,
                    "avg_score": round(avg, 2),
                    "count": len(scores),
                    "max_score": round(max(scores), 2) if scores else 0,
                })
            trends[param] = sorted(param_trend, key=lambda x: x["avg_score"], reverse=True)

        return trends

    def _build_recommendation(
        self,
        top_10: list[dict[str, Any]],
        trends: dict[str, list[dict[str, Any]]],
    ) -> str:
        """推薦テキストを生成する。"""
        if not top_10:
            return "有効なトライアルがありませんでした。"

        best = top_10[0]
        lines = [
            "=== 探索結果レポート ===",
            "",
            f"最優秀構成 (composite_score: {best.get('composite_score', 0):.1f}/100):",
            f"  訓練窓:     {best.get('train_window_months')}ヶ月",
            f"  EV閾値:     {best.get('ev_threshold')}",
            f"  正則化(C):  {best.get('regularization')}",
            f"  ターゲット: {'単勝(1着)' if best.get('target_jyuni') == 1 else '複勝(3着以内)'}",
            f"  校正方式:   {best.get('calibration_method')}",
            f"  投票方式:   {best.get('betting_method')}",
            f"  WF窓数:     {best.get('wf_n_windows')}",
            f"  最大ベット: {best.get('max_bets_per_race')}/レース",
            f"  ファクター: {best.get('factor_selection')}",
            "",
            "パフォーマンス (OOS):",
            f"  ROI:        {best.get('roi', 0):+.1%}",
            f"  Sharpe:     {best.get('sharpe_ratio', 0):.3f}",
            f"  Max DD:     {best.get('max_drawdown', 0):.1%}",
            f"  勝率:       {best.get('win_rate', 0):.1%}",
            f"  PF:         {best.get('profit_factor', 0):.2f}",
            f"  総ベット:   {best.get('total_bets', 0)}件",
            "",
            f"  MC 5%ile ROI:   {best.get('mc_roi_5th', 0):+.1%}",
            f"  MC 95%ile ROI:  {best.get('mc_roi_95th', 0):+.1%}",
            f"  破産確率:       {best.get('mc_ruin_probability', 0):.1%}",
            "",
            "パラメータ傾向（最適値）:",
        ]

        for param, values in trends.items():
            if values:
                best_val = values[0]
                lines.append(
                    f"  {param}: {best_val['value']} "
                    f"(avg={best_val['avg_score']:.1f}, n={best_val['count']})"
                )

        return "\n".join(lines)

    def format_report(self, summary: SearchSummary) -> str:
        """CLI出力用の整形テキストを返す。"""
        lines = [
            summary.recommendation,
            "",
            "--- 統計 ---",
            f"完了: {summary.completed_trials}/{summary.total_trials} "
            f"(エラー: {summary.error_trials})",
            f"所要時間: {summary.elapsed_total_seconds:.0f}秒 "
            f"({summary.elapsed_total_seconds / 3600:.1f}時間)",
        ]

        if len(summary.top_10_trials) > 1:
            lines.append("")
            lines.append("--- 上位10構成 ---")
            for i, t in enumerate(summary.top_10_trials[:10], 1):
                lines.append(
                    f"  {i}. score={t.get('composite_score', 0):.1f} "
                    f"ROI={t.get('roi', 0):+.1%} "
                    f"Sharpe={t.get('sharpe_ratio', 0):.3f} "
                    f"DD={t.get('max_drawdown', 0):.1%} "
                    f"bets={t.get('total_bets', 0)} "
                    f"EV={t.get('ev_threshold')} "
                    f"C={t.get('regularization')} "
                    f"win={t.get('train_window_months')}m"
                )

        return "\n".join(lines)
