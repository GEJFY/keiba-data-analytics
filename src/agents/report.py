"""レポート生成エージェント。

バックテスト結果・収支データからパフォーマンスレポートを生成する。
LLM未設定時はテンプレートベースのレポートを返す。
"""

from typing import Any

from src.agents.base import BaseAgent


class ReportAgent(BaseAgent):
    """パフォーマンスレポート生成エージェント。"""

    def agent_name(self) -> str:
        return "Report"

    def use_case(self) -> str:
        return "report_generation"

    def build_prompt(self, context: dict[str, Any]) -> tuple[str, str]:
        system_prompt = (
            "あなたは投資パフォーマンスアナリストです。"
            "競馬投資の収支データとバックテスト結果を分析し、"
            "以下の構成でレポートを作成してください:\n"
            "1. サマリー（主要KPI）\n"
            "2. パフォーマンス分析\n"
            "3. 改善提案\n"
            "簡潔かつデータに基づいた分析を心がけてください。"
        )

        user_prompt = self._format_context(context)
        return system_prompt, user_prompt

    @staticmethod
    def _format_context(context: dict[str, Any]) -> str:
        """コンテキストをプロンプト用テキストに整形する。"""
        lines = []

        bt = context.get("backtest_results", [])
        if bt:
            lines.append("バックテスト結果:")
            for r in bt[:5]:
                lines.append(
                    f"  - {r.get('strategy_version', '?')}: "
                    f"ROI={r.get('roi', 0):.1%}, "
                    f"勝率={r.get('win_rate', 0):.1%}, "
                    f"P&L={r.get('pnl', 0):+,}円, "
                    f"最大DD={r.get('max_drawdown', 0):.1%}"
                )

        rules = context.get("active_rules", [])
        if rules:
            lines.append(f"\n有効ファクター数: {len(rules)}")

        pnl = context.get("pnl_summary", {})
        if pnl:
            lines.append(
                f"\n収支概要: 総投票={pnl.get('total_stake', 0):,}円, "
                f"総払戻={pnl.get('total_payout', 0):,}円, "
                f"ROI={pnl.get('roi', 0):.1%}"
            )

        return "\n".join(lines) if lines else "データなし"

    def fallback_response(self, context: dict[str, Any]) -> str:
        """テンプレートベースのレポートを生成する。"""
        bt = context.get("backtest_results", [])
        rules = context.get("active_rules", [])

        lines = ["# パフォーマンスレポート", ""]

        # サマリー
        lines.append("## 1. サマリー")
        if bt:
            latest = bt[0]
            lines.append(f"- 最新バックテスト: {latest.get('strategy_version', '?')}")
            lines.append(f"- ROI: {latest.get('roi', 0):+.1%}")
            lines.append(f"- 勝率: {latest.get('win_rate', 0):.1%}")
            lines.append(f"- 総レース数: {latest.get('total_races', 0)}")
            lines.append(f"- 総ベット数: {latest.get('total_bets', 0)}")
            lines.append(f"- P&L: {latest.get('pnl', 0):+,}円")
            lines.append(f"- 最大ドローダウン: {latest.get('max_drawdown', 0):.1%}")
        else:
            lines.append("- バックテスト未実行")

        lines.append("")
        lines.append(f"- 有効ファクター数: {len(rules)}")

        # パフォーマンス分析
        lines.append("")
        lines.append("## 2. パフォーマンス分析")
        if bt:
            roi = latest.get("roi", 0)
            if roi > 0:
                lines.append("プラス収支を達成しています。")
            elif roi > -0.1:
                lines.append("わずかなマイナスですが、ファクター調整で改善可能です。")
            else:
                lines.append("マイナスが大きいため、ファクター見直しが必要です。")

            dd = latest.get("max_drawdown", 0)
            if dd > 0.2:
                lines.append(f"最大DD {dd:.1%} はリスク管理の観点から要注意です。")

            win_rate = latest.get("win_rate", 0)
            if win_rate < 0.15:
                lines.append("勝率が低いため、EV閾値の引き上げを検討してください。")
        else:
            lines.append("バックテストを実行して分析データを蓄積してください。")

        # 改善提案
        lines.append("")
        lines.append("## 3. 改善提案")
        suggestions = []
        if not bt:
            suggestions.append("バックテストを実行してKPIを確認する")
        else:
            if latest.get("total_bets", 0) < 10:
                suggestions.append("対象期間を広げてサンプル数を増やす")
            if latest.get("roi", 0) < 0:
                suggestions.append("EV閾値を1.10以上に引き上げてベット精度を向上させる")
            if latest.get("max_drawdown", 0) > 0.2:
                suggestions.append("1レースあたりの投票上限率を下げてリスクを抑制する")
        if len(rules) < 10:
            suggestions.append("ファクター数が少ないため、新規ファクターを追加する")

        if suggestions:
            for s in suggestions:
                lines.append(f"- {s}")
        else:
            lines.append("- 現在の設定を維持して運用データを蓄積する")

        return "\n".join(lines)
