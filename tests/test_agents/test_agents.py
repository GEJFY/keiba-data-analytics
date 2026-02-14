"""AIエージェントのテスト。

LLM未設定時のフォールバック応答をテストする。
"""

import asyncio

from src.agents.factor_proposal import FactorProposalAgent
from src.agents.race_analysis import RaceAnalysisAgent
from src.agents.report import ReportAgent


def _run(coro):
    """asyncテスト用ヘルパー。"""
    return asyncio.run(coro)


class TestRaceAnalysisAgent:
    """レース分析エージェントのテスト。"""

    def test_agent_name(self) -> None:
        agent = RaceAnalysisAgent()
        assert agent.agent_name() == "RaceAnalysis"

    def test_fallback_with_scored_results(self) -> None:
        """スコアリング結果がある場合の分析コメント。"""
        agent = RaceAnalysisAgent()
        result = _run(agent.run({
            "race_info": {"RaceName": "テストレース", "Kyori": "1600", "TrackCD": "10"},
            "scored_results": [
                {"umaban": "03", "total_score": 105.5, "expected_value": 1.15,
                 "estimated_prob": 0.12, "actual_odds": 9.5, "fair_odds": 8.3,
                 "is_value_bet": True, "factor_details": {"DM予想上位": 1.5}},
                {"umaban": "01", "total_score": 103.0, "expected_value": 0.95,
                 "estimated_prob": 0.10, "actual_odds": 9.0, "fair_odds": 10.0,
                 "is_value_bet": False, "factor_details": {}},
            ],
        }))
        assert "テストレース" in result
        assert "馬番03" in result or "03" in result
        assert "バリューベット" in result

    def test_fallback_empty_results(self) -> None:
        """スコアリング結果がない場合。"""
        agent = RaceAnalysisAgent()
        result = _run(agent.run({"scored_results": []}))
        assert "ありません" in result


class TestFactorProposalAgent:
    """ファクター提案エージェントのテスト。"""

    def test_agent_name(self) -> None:
        agent = FactorProposalAgent()
        assert agent.agent_name() == "FactorProposal"

    def test_fallback_proposals(self) -> None:
        """フォールバックでファクター候補を提案すること。"""
        agent = FactorProposalAgent()
        result = _run(agent.run({
            "existing_rules": [],
            "backtest_summary": "ROI: -5%, 勝率: 15%",
        }))
        assert "候補" in result
        assert "sql_expression" in result or "式" in result

    def test_fallback_no_proposals(self) -> None:
        """既存ルールと全て重複する場合。"""
        agent = FactorProposalAgent()
        # 候補テンプレートと同名のルールを渡す
        result = _run(agent.run({
            "existing_rules": [
                {"rule_name": "連続好走(2連続3着内)"},
                {"rule_name": "オッズ妙味(過小評価)"},
                {"rule_name": "セン馬加点(ダート)"},
                {"rule_name": "4角先頭逃げ切り"},
                {"rule_name": "中穴DM高評価(中距離)"},
            ],
            "backtest_summary": "",
        }))
        assert "十分" in result or "ありません" in result


class TestReportAgent:
    """レポート生成エージェントのテスト。"""

    def test_agent_name(self) -> None:
        agent = ReportAgent()
        assert agent.agent_name() == "Report"

    def test_fallback_with_backtest(self) -> None:
        """バックテスト結果がある場合のレポート生成。"""
        agent = ReportAgent()
        result = _run(agent.run({
            "backtest_results": [
                {"strategy_version": "GY_VALUE v1.0.0", "roi": 0.05,
                 "win_rate": 0.2, "total_races": 30, "total_bets": 15,
                 "pnl": 50000, "max_drawdown": 0.08},
            ],
            "active_rules": [{"rule_name": "test"}] * 20,
        }))
        assert "レポート" in result
        assert "ROI" in result or "roi" in result.lower()
        assert "GY_VALUE" in result

    def test_fallback_no_data(self) -> None:
        """データなしの場合のレポート。"""
        agent = ReportAgent()
        result = _run(agent.run({
            "backtest_results": [],
            "active_rules": [],
        }))
        assert "レポート" in result
        assert "バックテスト" in result
