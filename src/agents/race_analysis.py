"""レース分析エージェント。

スコアリング結果を踏まえたレース分析コメントを生成する。
LLM未設定時はルールベースのフォールバック分析を返す。
"""

from typing import Any

from src.agents.base import BaseAgent


class RaceAnalysisAgent(BaseAgent):
    """レース分析エージェント。"""

    def agent_name(self) -> str:
        return "RaceAnalysis"

    def use_case(self) -> str:
        return "race_analysis"

    def build_prompt(self, context: dict[str, Any]) -> tuple[str, str]:
        system_prompt = (
            "あなたは競馬データアナリストです。"
            "提供されたスコアリングデータに基づき、レースの注目ポイントと推奨馬を分析してください。"
            "分析は簡潔に、バリューベットの根拠を中心に記述してください。"
        )

        race = context.get("race_info", {})
        scored = context.get("scored_results", [])

        race_desc = (
            f"レース: {race.get('RaceName', '不明')} "
            f"({race.get('Kyori', '?')}m, "
            f"{'芝' if str(race.get('TrackCD', '')).startswith('1') else 'ダート'})"
        )

        horses_desc = []
        for i, s in enumerate(scored[:5]):
            horses_desc.append(
                f"  {i+1}. 馬番{s['umaban']} "
                f"GY指数={s['total_score']:.1f} "
                f"EV={s.get('expected_value', 0):.3f} "
                f"推定勝率={s.get('estimated_prob', 0):.1%} "
                f"オッズ={s.get('actual_odds', 0):.1f}"
            )

        user_prompt = f"{race_desc}\n\n上位5頭のスコアリング結果:\n" + "\n".join(horses_desc)
        return system_prompt, user_prompt

    def fallback_response(self, context: dict[str, Any]) -> str:
        """ルールベースの分析コメントを生成する。"""
        scored = context.get("scored_results", [])
        race = context.get("race_info", {})

        if not scored:
            return "スコアリング結果がありません。"

        race_name = race.get("RaceName", "対象レース")
        kyori = race.get("Kyori", "?")
        track = "芝" if str(race.get("TrackCD", "")).startswith("1") else "ダート"

        lines = [f"**{race_name}** ({kyori}m {track}) の分析:", ""]

        # バリューベット抽出
        value_bets = [s for s in scored if s.get("is_value_bet", False)]
        if value_bets:
            lines.append(f"バリューベット: {len(value_bets)}頭検出")
            for vb in value_bets[:3]:
                details = vb.get("factor_details", {})
                top_factors = sorted(details.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
                factor_str = ", ".join(f"{k}({v:+.1f})" for k, v in top_factors)
                lines.append(
                    f"- 馬番{vb['umaban']}: "
                    f"EV={vb.get('expected_value', 0):.3f} "
                    f"(オッズ{vb.get('actual_odds', 0):.1f} vs 適正{vb.get('fair_odds', 0):.1f})"
                )
                if factor_str:
                    lines.append(f"  主要ファクター: {factor_str}")
        else:
            lines.append("バリューベットは検出されませんでした。見送り推奨。")

        # 上位馬コメント
        top = scored[0]
        lines.append("")
        lines.append(
            f"GY指数トップ: 馬番{top['umaban']} "
            f"(スコア {top['total_score']:.1f})"
        )

        return "\n".join(lines)
