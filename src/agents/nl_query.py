"""自然言語クエリエージェント。

ユーザーの自然言語質問をSQLクエリに変換し、
JVLink DBから結果を取得して回答を生成する。
LLM未設定時はパターンマッチングによるフォールバックを使用する。
"""

import re
from typing import Any

from src.agents.base import BaseAgent
from src.data.db import DatabaseManager


class NLQueryAgent(BaseAgent):
    """自然言語クエリエージェント。"""

    # 安全なSELECTのみ許可するパターン
    _SAFE_SQL_PATTERN = re.compile(
        r"^\s*SELECT\s",
        re.IGNORECASE,
    )
    _FORBIDDEN_KEYWORDS = {"DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "EXEC", "TRUNCATE"}

    def __init__(self, gateway: Any = None, jvlink_db: DatabaseManager | None = None) -> None:
        super().__init__(gateway)
        self._jvlink_db = jvlink_db

    def agent_name(self) -> str:
        return "NLQuery"

    def use_case(self) -> str:
        return "race_analysis"

    def build_prompt(self, context: dict[str, Any]) -> tuple[str, str]:
        system_prompt = (
            "あなたは競馬データベースの分析アシスタントです。\n"
            "ユーザーの質問をSQLiteクエリに変換して回答してください。\n\n"
            "利用可能なテーブル:\n"
            "- NL_RA_RACE: レース情報 (idYear, idMonthDay, idJyoCD, idRaceNum, "
            "RaceInfoHondai, Kyori, TrackCD, SyussoTosu)\n"
            "- NL_SE_RACE_UMA: 出走馬 (idYear, idMonthDay, idJyoCD, idRaceNum, "
            "Umaban, Bamei, KakuteiJyuni, Ninki, Odds, Futan, BaTaijyu, "
            "DMJyuni, HaronTimeL3, KyakusituKubun)\n"
            "- NL_HR_PAY: 払戻 (PayTansyoUmaban1, PayTansyoPay1, etc.)\n\n"
            "回答形式:\n"
            "1. SQLクエリ（```sql ... ```）\n"
            "2. 結果の解説\n\n"
            "安全性: SELECTクエリのみ生成してください。"
        )
        question = context.get("question", "")
        user_prompt = f"質問: {question}"
        return system_prompt, user_prompt

    def fallback_response(self, context: dict[str, Any]) -> str:
        """パターンマッチングで回答する。"""
        question = context.get("question", "")
        if not question:
            return "質問を入力してください。"

        q = question.lower()

        # パターン: レース数
        if "レース" in q and ("数" in q or "件" in q or "何" in q):
            return self._query_race_count(context)

        # パターン: 勝率 / 成績
        if "勝率" in q or "成績" in q:
            return self._query_win_rate(context)

        # パターン: 馬名検索
        if "馬" in q and any(kw in q for kw in ["情報", "成績", "データ"]):
            return self._query_horse_info(context)

        # パターン: ランキング / 上位
        if "ランキング" in q or "上位" in q or "トップ" in q:
            return self._query_ranking(context)

        return (
            "申し訳ございませんが、質問を理解できませんでした。\n"
            "以下のような質問に対応しています:\n"
            "- 「今月のレース数は？」\n"
            "- 「1番人気の勝率は？」\n"
            "- 「DMJyuni上位馬のランキング」\n"
            "\nLLM設定後はより自由な質問に対応できます。"
        )

    def execute_safe_query(self, sql: str) -> list[dict[str, Any]]:
        """安全なSELECTクエリのみ実行する。

        Args:
            sql: 実行するSQLクエリ

        Returns:
            クエリ結果

        Raises:
            ValueError: 危険なSQL文が検出された場合
        """
        if not self._jvlink_db:
            raise ValueError("データベースが未設定です")

        sql_upper = sql.upper().strip()

        # SELECT以外を拒否
        if not self._SAFE_SQL_PATTERN.match(sql):
            raise ValueError("SELECTクエリのみ実行可能です")

        # 禁止キーワードチェック
        for kw in self._FORBIDDEN_KEYWORDS:
            if kw in sql_upper:
                raise ValueError(f"禁止キーワードが含まれています: {kw}")

        return self._jvlink_db.execute_query(sql)

    def _query_race_count(self, context: dict[str, Any]) -> str:
        """レース数を回答する。"""
        if not self._jvlink_db:
            return "データベースが未接続です。"

        if not self._jvlink_db.table_exists("NL_RA_RACE"):
            return "レーステーブル(NL_RA_RACE)が見つかりません。"

        rows = self._jvlink_db.execute_query(
            "SELECT COUNT(*) as cnt FROM NL_RA_RACE"
        )
        cnt = rows[0]["cnt"] if rows else 0
        return f"データベース内のレース数: **{cnt:,}件**"

    def _query_win_rate(self, context: dict[str, Any]) -> str:
        """1番人気の勝率を回答する。"""
        if not self._jvlink_db:
            return "データベースが未接続です。"

        if not self._jvlink_db.table_exists("NL_SE_RACE_UMA"):
            return "出走馬テーブル(NL_SE_RACE_UMA)が見つかりません。"

        rows = self._jvlink_db.execute_query(
            """SELECT
                 COUNT(*) as total,
                 SUM(CASE WHEN KakuteiJyuni = '1' THEN 1 ELSE 0 END) as wins
               FROM NL_SE_RACE_UMA
               WHERE Ninki = '1'"""
        )
        if rows and rows[0]["total"] > 0:
            total = rows[0]["total"]
            wins = rows[0]["wins"]
            rate = wins / total if total else 0
            return (
                f"1番人気の成績:\n"
                f"- 出走数: {total:,}件\n"
                f"- 勝利数: {wins:,}件\n"
                f"- 勝率: **{rate:.1%}**"
            )
        return "1番人気のデータがありません。"

    def _query_horse_info(self, context: dict[str, Any]) -> str:
        """馬情報を回答する。"""
        return "馬名を指定して検索するにはLLM設定が必要です。パターンマッチングでは対応できません。"

    def _query_ranking(self, context: dict[str, Any]) -> str:
        """ランキングを回答する。"""
        if not self._jvlink_db:
            return "データベースが未接続です。"

        if not self._jvlink_db.table_exists("NL_SE_RACE_UMA"):
            return "出走馬テーブルが見つかりません。"

        rows = self._jvlink_db.execute_query(
            """SELECT Bamei, COUNT(*) as wins
               FROM NL_SE_RACE_UMA
               WHERE KakuteiJyuni = '1'
               GROUP BY Bamei
               ORDER BY wins DESC
               LIMIT 10"""
        )
        if not rows:
            return "勝利データが見つかりません。"

        lines = ["**勝利数ランキング (TOP 10):**", ""]
        for i, row in enumerate(rows, 1):
            lines.append(f"{i}. {row['Bamei']}: {row['wins']}勝")
        return "\n".join(lines)
