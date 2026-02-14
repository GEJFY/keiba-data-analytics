"""新規エージェント（NLQuery, AlertInterpreter, DeepResearch）のテスト。"""

import asyncio

import pytest

from src.agents.alert_interpreter import AlertInterpreterAgent
from src.agents.deep_research import DeepResearchAgent
from src.agents.nl_query import NLQueryAgent
from src.data.db import DatabaseManager


def _run(coro):
    """asyncテスト用ヘルパー。"""
    return asyncio.run(coro)


def _make_jvlink_db(tmp_path) -> DatabaseManager:
    """テスト用JVLink DBを作成する。"""
    db = DatabaseManager(str(tmp_path / "jvlink.db"), wal_mode=False)
    with db.connect() as conn:
        conn.execute("""
            CREATE TABLE NL_RA_RACE (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                RaceInfoHondai TEXT, Kyori TEXT
            )
        """)
        conn.execute("""
            INSERT INTO NL_RA_RACE VALUES
            ('2025', '0105', '06', '01', '01', '01', 'テストレース', '1600')
        """)
        conn.execute("""
            CREATE TABLE NL_SE_RACE_UMA (
                idYear TEXT, idMonthDay TEXT, idJyoCD TEXT,
                idKaiji TEXT, idNichiji TEXT, idRaceNum TEXT,
                Umaban TEXT, Bamei TEXT, KakuteiJyuni TEXT,
                Ninki TEXT, Odds TEXT, Futan TEXT, BaTaijyu TEXT,
                HaronTimeL3 TEXT, KyakusituKubun TEXT, DMJyuni TEXT,
                KisyuRyakusyo TEXT
            )
        """)
        for uma, bamei, jyuni, ninki in [
            ("01", "テスト馬A", "2", "3"),
            ("03", "テスト馬B", "1", "1"),
            ("07", "テスト馬C", "3", "2"),
        ]:
            conn.execute(
                "INSERT INTO NL_SE_RACE_UMA VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("2025", "0105", "06", "01", "01", "01",
                 uma, bamei, jyuni, ninki, "0500", "55.0", "480",
                 "345", "2", "1", "テスト騎手"),
            )
    return db


class TestNLQueryAgent:
    """NLQueryAgentのテスト。"""

    def test_agent_name(self) -> None:
        agent = NLQueryAgent()
        assert agent.agent_name() == "NLQuery"

    def test_fallback_race_count(self, tmp_path) -> None:
        """レース数の質問に回答。"""
        db = _make_jvlink_db(tmp_path)
        agent = NLQueryAgent(jvlink_db=db)
        result = _run(agent.run({"question": "レース数は何件？"}))
        assert "1" in result

    def test_fallback_win_rate(self, tmp_path) -> None:
        """勝率の質問に回答。"""
        db = _make_jvlink_db(tmp_path)
        agent = NLQueryAgent(jvlink_db=db)
        result = _run(agent.run({"question": "1番人気の勝率は？"}))
        assert "1番人気" in result

    def test_fallback_ranking(self, tmp_path) -> None:
        """ランキングの質問。"""
        db = _make_jvlink_db(tmp_path)
        agent = NLQueryAgent(jvlink_db=db)
        result = _run(agent.run({"question": "勝利数ランキングを教えて"}))
        assert "ランキング" in result

    def test_fallback_unknown(self) -> None:
        """不明な質問。"""
        agent = NLQueryAgent()
        result = _run(agent.run({"question": "量子力学について"}))
        assert "理解できませんでした" in result

    def test_fallback_empty_question(self) -> None:
        """空の質問。"""
        agent = NLQueryAgent()
        result = _run(agent.run({"question": ""}))
        assert "入力してください" in result

    def test_execute_safe_query(self, tmp_path) -> None:
        """安全なSELECTクエリの実行。"""
        db = _make_jvlink_db(tmp_path)
        agent = NLQueryAgent(jvlink_db=db)
        rows = agent.execute_safe_query("SELECT COUNT(*) as cnt FROM NL_RA_RACE")
        assert rows[0]["cnt"] == 1

    def test_execute_unsafe_query(self, tmp_path) -> None:
        """危険なクエリは拒否。"""
        db = _make_jvlink_db(tmp_path)
        agent = NLQueryAgent(jvlink_db=db)
        with pytest.raises(ValueError, match="SELECTクエリのみ"):
            agent.execute_safe_query("DROP TABLE NL_RA_RACE")

    def test_execute_query_with_forbidden_keyword(self, tmp_path) -> None:
        """禁止キーワードを含むSELECTも拒否。"""
        db = _make_jvlink_db(tmp_path)
        agent = NLQueryAgent(jvlink_db=db)
        with pytest.raises(ValueError, match="禁止キーワード"):
            agent.execute_safe_query("SELECT * FROM NL_RA_RACE; DELETE FROM NL_RA_RACE")

    def test_execute_query_no_db(self) -> None:
        """DB未設定でエラー。"""
        agent = NLQueryAgent()
        with pytest.raises(ValueError, match="データベースが未設定"):
            agent.execute_safe_query("SELECT 1")


class TestAlertInterpreterAgent:
    """AlertInterpreterAgentのテスト。"""

    def test_agent_name(self) -> None:
        agent = AlertInterpreterAgent()
        assert agent.agent_name() == "AlertInterpreter"

    def test_fallback_odds_drop(self) -> None:
        """オッズ急落アラート。"""
        agent = AlertInterpreterAgent()
        result = _run(agent.run({
            "alerts": [
                {"type": "ODDS_DROP", "message": "馬番03のオッズが急落",
                 "data": {"drop_rate": 0.4}},
            ],
            "race_info": {"RaceName": "テストレース", "Kyori": "1600"},
        }))
        assert "内部情報" in result or "維持" in result

    def test_fallback_scratched(self) -> None:
        """出走取消アラート。"""
        agent = AlertInterpreterAgent()
        result = _run(agent.run({
            "alerts": [
                {"type": "SCRATCHED", "message": "馬番05が出走取消",
                 "data": {"umaban": "05"}},
            ],
            "race_info": {},
        }))
        assert "取消" in result or "再計算" in result

    def test_fallback_track_change(self) -> None:
        """馬場変更アラート。"""
        agent = AlertInterpreterAgent()
        result = _run(agent.run({
            "alerts": [
                {"type": "TRACK_CHANGE", "message": "馬場が良→稍重に変化",
                 "data": {}},
            ],
            "race_info": {},
        }))
        assert "馬場" in result

    def test_fallback_no_alerts(self) -> None:
        """アラートなし。"""
        agent = AlertInterpreterAgent()
        result = _run(agent.run({"alerts": []}))
        assert "ありません" in result

    def test_fallback_unknown_alert(self) -> None:
        """未知のアラートタイプ。"""
        agent = AlertInterpreterAgent()
        result = _run(agent.run({
            "alerts": [{"type": "UNKNOWN", "message": "test", "data": {}}],
            "race_info": {},
        }))
        assert "確認" in result


class TestDeepResearchAgent:
    """DeepResearchAgentのテスト。"""

    def test_agent_name(self) -> None:
        agent = DeepResearchAgent()
        assert agent.agent_name() == "DeepResearch"

    def test_horse_research(self, tmp_path) -> None:
        """馬のリサーチレポート。"""
        db = _make_jvlink_db(tmp_path)
        agent = DeepResearchAgent(jvlink_db=db)
        result = _run(agent.run({"type": "horse", "bamei": "テスト馬B"}))
        assert "テスト馬B" in result
        assert "リサーチ" in result or "基本成績" in result

    def test_horse_research_not_found(self, tmp_path) -> None:
        """存在しない馬。"""
        db = _make_jvlink_db(tmp_path)
        agent = DeepResearchAgent(jvlink_db=db)
        result = _run(agent.run({"type": "horse", "bamei": "存在しない馬"}))
        assert "見つかりません" in result

    def test_jockey_research(self, tmp_path) -> None:
        """騎手リサーチ。"""
        db = _make_jvlink_db(tmp_path)
        agent = DeepResearchAgent(jvlink_db=db)
        result = _run(agent.run({"type": "jockey", "kisyu": "テスト騎手"}))
        assert "テスト騎手" in result

    def test_course_research(self, tmp_path) -> None:
        """コースリサーチ。"""
        db = _make_jvlink_db(tmp_path)
        agent = DeepResearchAgent(jvlink_db=db)
        result = _run(agent.run({"type": "course", "jyo_cd": "06", "kyori": "1600"}))
        assert "中山" in result or "1600" in result

    def test_unknown_type(self) -> None:
        """不明なリサーチタイプ。"""
        agent = DeepResearchAgent()
        result = _run(agent.run({"type": "unknown"}))
        assert "対応していない" in result

    def test_no_db(self) -> None:
        """DB未設定のフォールバック。"""
        agent = DeepResearchAgent()
        result = _run(agent.run({"type": "horse", "bamei": "テスト"}))
        assert "見つかりません" in result
