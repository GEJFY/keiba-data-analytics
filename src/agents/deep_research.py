"""ディープリサーチエージェント。

特定の馬・騎手・コースについて、過去成績データを収集し、
詳細な調査レポートを生成する。
LLM未設定時はデータベースクエリ結果の整形レポートを返す。
"""

from typing import Any

from src.agents.base import BaseAgent
from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider


class DeepResearchAgent(BaseAgent):
    """ディープリサーチエージェント。"""

    def __init__(
        self,
        gateway: Any = None,
        jvlink_db: DatabaseManager | None = None,
    ) -> None:
        super().__init__(gateway)
        self._jvlink_db = jvlink_db
        self._provider = JVLinkDataProvider(jvlink_db) if jvlink_db else None

    def agent_name(self) -> str:
        return "DeepResearch"

    def use_case(self) -> str:
        return "race_analysis"

    def build_prompt(self, context: dict[str, Any]) -> tuple[str, str]:
        system_prompt = (
            "あなたは競馬データリサーチャーです。\n"
            "提供されたデータに基づき、指定された馬・騎手・コースについて\n"
            "詳細な分析レポートを作成してください。\n\n"
            "レポート構成:\n"
            "1. 基本情報サマリー\n"
            "2. 過去成績の傾向分析\n"
            "3. 条件適性（距離・馬場・コース）\n"
            "4. 投資判断への示唆"
        )

        research_data = self._collect_research_data(context)
        user_prompt = self._format_research_data(context, research_data)
        return system_prompt, user_prompt

    def fallback_response(self, context: dict[str, Any]) -> str:
        """データベースクエリ結果ベースのレポートを返す。"""
        research_type = context.get("type", "horse")
        research_data = self._collect_research_data(context)

        if research_type == "horse":
            return self._horse_report(context, research_data)
        elif research_type == "jockey":
            return self._jockey_report(context, research_data)
        elif research_type == "course":
            return self._course_report(context, research_data)
        else:
            return "対応していないリサーチタイプです。type: horse / jockey / course"

    def _collect_research_data(self, context: dict[str, Any]) -> dict[str, Any]:
        """コンテキストに基づいてデータを収集する。"""
        if not self._jvlink_db:
            return {}

        research_type = context.get("type", "horse")
        data: dict[str, Any] = {}

        if research_type == "horse":
            bamei = context.get("bamei", "")
            if bamei and self._jvlink_db.table_exists("NL_SE_RACE_UMA"):
                rows = self._jvlink_db.execute_query(
                    """SELECT idYear, idMonthDay, idJyoCD, idRaceNum,
                              Umaban, KakuteiJyuni, Ninki, Odds, Futan,
                              BaTaijyu, HaronTimeL3, KyakusituKubun, DMJyuni
                       FROM NL_SE_RACE_UMA
                       WHERE Bamei = ?
                       ORDER BY idYear DESC, idMonthDay DESC
                       LIMIT 30""",
                    (bamei,),
                )
                data["results"] = rows
                data["bamei"] = bamei

        elif research_type == "jockey":
            kisyu = context.get("kisyu", "")
            if kisyu and self._jvlink_db.table_exists("NL_SE_RACE_UMA"):
                rows = self._jvlink_db.execute_query(
                    """SELECT KakuteiJyuni, COUNT(*) as cnt
                       FROM NL_SE_RACE_UMA
                       WHERE KisyuRyakusyo = ?
                       GROUP BY KakuteiJyuni
                       ORDER BY CAST(KakuteiJyuni AS INTEGER)""",
                    (kisyu,),
                )
                data["jockey_stats"] = rows
                data["kisyu"] = kisyu

        elif research_type == "course":
            jyo_cd = context.get("jyo_cd", "")
            kyori = context.get("kyori", "")
            if self._jvlink_db.table_exists("NL_SE_RACE_UMA"):
                sql = """SELECT s.KyakusituKubun, s.KakuteiJyuni, COUNT(*) as cnt
                         FROM NL_SE_RACE_UMA s
                         JOIN NL_RA_RACE r ON s.idYear = r.idYear
                           AND s.idMonthDay = r.idMonthDay
                           AND s.idJyoCD = r.idJyoCD
                           AND s.idKaiji = r.idKaiji
                           AND s.idNichiji = r.idNichiji
                           AND s.idRaceNum = r.idRaceNum
                         WHERE 1=1"""
                params: list[str] = []
                if jyo_cd:
                    sql += " AND r.idJyoCD = ?"
                    params.append(jyo_cd)
                if kyori:
                    sql += " AND r.Kyori = ?"
                    params.append(kyori)
                sql += " GROUP BY s.KyakusituKubun, s.KakuteiJyuni"

                rows = self._jvlink_db.execute_query(sql, tuple(params))
                data["course_stats"] = rows

        return data

    def _format_research_data(self, context: dict[str, Any], data: dict[str, Any]) -> str:
        """リサーチデータをプロンプト用に整形する。"""
        lines = []
        research_type = context.get("type", "horse")

        if research_type == "horse":
            bamei = data.get("bamei", context.get("bamei", "不明"))
            results = data.get("results", [])
            lines.append(f"馬名: {bamei}")
            lines.append(f"過去成績データ: {len(results)}件")
            for r in results[:10]:
                jyuni = r.get("KakuteiJyuni", "?")
                ninki = r.get("Ninki", "?")
                lines.append(
                    f"  {r.get('idYear', '')}/{r.get('idMonthDay', '')} "
                    f"着順:{jyuni} 人気:{ninki} "
                    f"上がり:{r.get('HaronTimeL3', '?')}"
                )
        elif research_type == "jockey":
            kisyu = data.get("kisyu", context.get("kisyu", "不明"))
            lines.append(f"騎手: {kisyu}")
            for s in data.get("jockey_stats", []):
                lines.append(f"  {s.get('KakuteiJyuni', '?')}着: {s.get('cnt', 0)}回")
        elif research_type == "course":
            lines.append(f"コース: {context.get('jyo_cd', '?')} {context.get('kyori', '?')}m")
            for s in data.get("course_stats", []):
                lines.append(
                    f"  脚質{s.get('KyakusituKubun', '?')} "
                    f"{s.get('KakuteiJyuni', '?')}着: {s.get('cnt', 0)}回"
                )

        return "\n".join(lines) if lines else "データなし"

    def _horse_report(self, context: dict[str, Any], data: dict[str, Any]) -> str:
        """馬のリサーチレポートを生成する。"""
        bamei = data.get("bamei", context.get("bamei", "不明"))
        results = data.get("results", [])

        lines = [f"# {bamei} リサーチレポート", ""]

        if not results:
            lines.append("過去成績データが見つかりませんでした。")
            return "\n".join(lines)

        # 基本成績
        total = len(results)
        wins = sum(1 for r in results if str(r.get("KakuteiJyuni", "")) == "1")
        places = sum(1 for r in results if str(r.get("KakuteiJyuni", "")) in ("1", "2", "3"))

        lines.append("## 基本成績")
        lines.append(f"- 出走数: {total}走")
        lines.append(f"- 勝率: {wins}/{total} ({wins/total:.1%})" if total else "- 勝率: 0")
        lines.append(f"- 連対率: {places}/{total} ({places/total:.1%})" if total else "- 連対率: 0")

        # 上がりタイム分析
        l3_times = []
        for r in results:
            try:
                t = float(r.get("HaronTimeL3", 0))
                if t > 0:
                    l3_times.append(t)
            except (ValueError, TypeError):
                pass

        if l3_times:
            lines.append("")
            lines.append("## 末脚分析")
            avg_l3 = sum(l3_times) / len(l3_times) / 10.0
            best_l3 = min(l3_times) / 10.0
            lines.append(f"- 上がり3F平均: {avg_l3:.1f}秒")
            lines.append(f"- 上がり3Fベスト: {best_l3:.1f}秒")

        # 人気 vs 結果
        beaten_favs = sum(
            1 for r in results
            if str(r.get("KakuteiJyuni", "")) == "1" and int(r.get("Ninki", 99)) > 3
        )
        if beaten_favs:
            lines.append("")
            lines.append(f"## 穴馬実績: 4番人気以下での勝利 {beaten_favs}回")

        return "\n".join(lines)

    def _jockey_report(self, context: dict[str, Any], data: dict[str, Any]) -> str:
        """騎手のリサーチレポートを生成する。"""
        kisyu = data.get("kisyu", context.get("kisyu", "不明"))
        stats = data.get("jockey_stats", [])

        lines = [f"# {kisyu} 騎手リサーチ", ""]

        if not stats:
            lines.append("成績データが見つかりませんでした。")
            return "\n".join(lines)

        total = sum(s.get("cnt", 0) for s in stats)
        wins = sum(s.get("cnt", 0) for s in stats if str(s.get("KakuteiJyuni", "")) == "1")

        lines.append("## 成績サマリー")
        lines.append(f"- 総騎乗数: {total}")
        lines.append(f"- 勝率: {wins/total:.1%}" if total else "- 勝率: -")

        lines.append("")
        lines.append("## 着順分布")
        for s in stats[:5]:
            lines.append(f"- {s.get('KakuteiJyuni', '?')}着: {s.get('cnt', 0)}回")

        return "\n".join(lines)

    def _course_report(self, context: dict[str, Any], data: dict[str, Any]) -> str:
        """コースのリサーチレポートを生成する。"""
        jyo_cd = context.get("jyo_cd", "?")
        kyori = context.get("kyori", "?")
        stats = data.get("course_stats", [])

        jyo_map = {
            "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
            "05": "東京", "06": "中山", "07": "中京", "08": "京都",
            "09": "阪神", "10": "小倉",
        }
        jyo_name = jyo_map.get(jyo_cd, jyo_cd)

        lines = [f"# {jyo_name} {kyori}m コース分析", ""]

        if not stats:
            lines.append("コースデータが見つかりませんでした。")
            return "\n".join(lines)

        # 脚質別分析
        style_map = {"1": "逃げ", "2": "先行", "3": "差し", "4": "追込"}
        style_wins: dict[str, int] = {}
        style_total: dict[str, int] = {}

        for s in stats:
            style = str(s.get("KyakusituKubun", ""))
            cnt = s.get("cnt", 0)
            jyuni = str(s.get("KakuteiJyuni", ""))

            style_total[style] = style_total.get(style, 0) + cnt
            if jyuni == "1":
                style_wins[style] = style_wins.get(style, 0) + cnt

        lines.append("## 脚質別勝率")
        for code, name in style_map.items():
            total = style_total.get(code, 0)
            wins = style_wins.get(code, 0)
            if total > 0:
                lines.append(f"- {name}: {wins}/{total} ({wins/total:.1%})")

        return "\n".join(lines)
