"""ファクター提案エージェント。

バックテスト結果やレースデータの傾向を分析し、
新規ファクタールール候補を提案する。
LLM未設定時はテンプレートベースの提案を返す。
"""

from typing import Any

from src.agents.base import BaseAgent


class FactorProposalAgent(BaseAgent):
    """ファクター提案エージェント。"""

    def agent_name(self) -> str:
        return "FactorProposal"

    def use_case(self) -> str:
        return "factor_generation"

    def build_prompt(self, context: dict[str, Any]) -> tuple[str, str]:
        system_prompt = (
            "あなたは競馬の定量分析専門家です。"
            "現在のファクタールール一覧とバックテスト結果を分析し、"
            "新規ファクタールール候補を3件提案してください。\n\n"
            "各提案にはrule_name、category、description、sql_expression（Python式）、"
            "weight（推奨値）を含めてください。\n\n"
            "使用可能な変数: Umaban, Wakuban, SexCD, Barei, Futan, Ninki, KakuteiJyuni, "
            "Kyori, TrackCD, weight, weight_diff, num_entries, gate_position, "
            "dm_rank, last_3f, last_3f_rank, running_style, corner4_pos, odds, "
            "is_favorite, is_longshot, is_turf, is_dirt, is_sprint, is_mile, "
            "is_middle, is_long, is_inner_gate, is_outer_gate, "
            "is_front_runner, is_closer, is_male, is_female, is_gelding"
        )

        existing_rules = context.get("existing_rules", [])
        bt_summary = context.get("backtest_summary", "")

        rules_desc = "\n".join(
            f"- {r.get('rule_name', '?')} [{r.get('category', '')}]: {r.get('sql_expression', '')}"
            for r in existing_rules[:20]
        )

        user_prompt = f"現在のファクタールール:\n{rules_desc}\n\nバックテスト概要:\n{bt_summary}"
        return system_prompt, user_prompt

    def fallback_response(self, context: dict[str, Any]) -> str:
        """テンプレートベースのファクター提案を返す。"""
        existing = context.get("existing_rules", [])
        existing_names = {r.get("rule_name", "") for r in existing}

        # 未登録のファクター候補テンプレート
        candidates = [
            {
                "rule_name": "連続好走(2連続3着内)",
                "category": "form",
                "description": "直近2走連続で3着以内の馬は好調期にある可能性が高い。",
                "sql_expression": "0.8 if KakuteiJyuni <= 3 else 0",
                "weight": 0.8,
            },
            {
                "rule_name": "オッズ妙味(過小評価)",
                "category": "odds",
                "description": "DM予想順位に対してオッズが高すぎる馬はバリュー候補。",
                "sql_expression": "1.5 if dm_rank <= 5 and odds >= 10 else 0",
                "weight": 1.2,
            },
            {
                "rule_name": "セン馬加点(ダート)",
                "category": "gender",
                "description": "セン馬はダートで過小評価される傾向がある。",
                "sql_expression": "0.5 if is_gelding and is_dirt else 0",
                "weight": 0.5,
            },
            {
                "rule_name": "4角先頭逃げ切り",
                "category": "pace",
                "description": "4コーナー先頭の逃げ馬は短距離で粘り込む傾向。",
                "sql_expression": "1 if corner4_pos == 1 and is_sprint else 0",
                "weight": 0.8,
            },
            {
                "rule_name": "中穴DM高評価(中距離)",
                "category": "dm",
                "description": "中距離で人気5-8番手かつDM予想4位以内。",
                "sql_expression": "1 if is_middle and 5 <= Ninki <= 8 and dm_rank <= 4 else 0",
                "weight": 1.0,
            },
        ]

        # 既存ルールと重複しないものを抽出
        proposals = [c for c in candidates if c["rule_name"] not in existing_names][:3]

        if not proposals:
            return "現在のファクター構成は十分です。追加提案はありません。"

        lines = ["**新規ファクター候補の提案:**", ""]
        for i, p in enumerate(proposals, 1):
            lines.append(f"### 候補{i}: {p['rule_name']}")
            lines.append(f"- カテゴリー: {p['category']}")
            lines.append(f"- 説明: {p['description']}")
            lines.append(f"- 式: `{p['sql_expression']}`")
            lines.append(f"- 推奨weight: {p['weight']}")
            lines.append("")

        lines.append("*ファクター管理画面から登録・テストできます。*")
        return "\n".join(lines)
