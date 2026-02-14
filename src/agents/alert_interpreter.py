"""アラート解釈エージェント。

オッズ急変・出走取消・馬場変更などのアラートを検出し、
投資判断への影響を解釈する。
LLM未設定時はルールベースの解釈を返す。
"""

from typing import Any

from src.agents.base import BaseAgent


class AlertInterpreterAgent(BaseAgent):
    """アラート解釈エージェント。"""

    def agent_name(self) -> str:
        return "AlertInterpreter"

    def use_case(self) -> str:
        return "race_analysis"

    def build_prompt(self, context: dict[str, Any]) -> tuple[str, str]:
        system_prompt = (
            "あなたは競馬投資のリスクアナリストです。\n"
            "以下のアラートが発生しました。投資判断への影響を分析し、\n"
            "具体的なアクション提案（ベット維持/減額/見送り）を回答してください。\n"
            "分析は簡潔に、理由とともに記述してください。"
        )

        alerts = context.get("alerts", [])
        race_info = context.get("race_info", {})
        bets = context.get("current_bets", [])

        lines = [
            f"レース: {race_info.get('RaceName', '不明')} "
            f"({race_info.get('Kyori', '?')}m)",
            "",
            "発生アラート:",
        ]
        for a in alerts:
            lines.append(f"- [{a.get('type', '?')}] {a.get('message', '')}")

        if bets:
            lines.append("")
            lines.append("現在のベット:")
            for b in bets[:5]:
                lines.append(
                    f"- 馬番{b.get('selection', '?')} "
                    f"{b.get('bet_type', '?')} "
                    f"{b.get('stake_yen', 0):,}円 "
                    f"EV={b.get('est_ev', 0):.3f}"
                )

        return system_prompt, "\n".join(lines)

    def fallback_response(self, context: dict[str, Any]) -> str:
        """ルールベースのアラート解釈を返す。"""
        alerts = context.get("alerts", [])
        if not alerts:
            return "アラートはありません。"

        lines = ["**アラート分析:**", ""]

        for alert in alerts:
            alert_type = alert.get("type", "")
            message = alert.get("message", "")
            lines.append(f"### {alert_type}")
            lines.append(f"内容: {message}")

            # ルールベース解釈
            interpretation = self._interpret_alert(alert)
            lines.append(f"影響: {interpretation['impact']}")
            lines.append(f"推奨: **{interpretation['action']}**")
            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _interpret_alert(alert: dict[str, Any]) -> dict[str, str]:
        """アラートをルールベースで解釈する。"""
        alert_type = alert.get("type", "").upper()
        data = alert.get("data", {})

        if alert_type == "ODDS_DROP":
            # オッズ急落 → 内部情報の可能性、プラス要因
            drop_rate = data.get("drop_rate", 0)
            if drop_rate > 0.3:
                return {
                    "impact": f"オッズが{drop_rate:.0%}下落。内部情報の可能性あり。",
                    "action": "ベット維持（EV再計算推奨）",
                }
            return {
                "impact": "オッズの小幅変動。通常範囲内。",
                "action": "ベット維持",
            }

        if alert_type == "ODDS_SURGE":
            # オッズ急騰 → 不安要因の可能性
            surge_rate = data.get("surge_rate", 0)
            if surge_rate > 0.5:
                return {
                    "impact": f"オッズが{surge_rate:.0%}急騰。不安要因の可能性あり。",
                    "action": "ベット減額または見送り",
                }
            return {
                "impact": "オッズの小幅上昇。",
                "action": "ベット維持（注視）",
            }

        if alert_type == "SCRATCHED":
            # 出走取消
            umaban = data.get("umaban", "?")
            return {
                "impact": f"馬番{umaban}が出走取消。レース構図が変化する可能性。",
                "action": "全ベットのEV再計算推奨",
            }

        if alert_type == "TRACK_CHANGE":
            # 馬場変更
            return {
                "impact": "馬場状態が変化。脚質・適性に影響あり。",
                "action": "馬場適性ファクターに基づいてEV再計算",
            }

        if alert_type == "WEATHER_CHANGE":
            return {
                "impact": "天候変化による馬場悪化の可能性。",
                "action": "重馬場適性の低い馬のベットを見直し",
            }

        return {
            "impact": "不明なアラートタイプ。",
            "action": "状況を確認してください",
        }
