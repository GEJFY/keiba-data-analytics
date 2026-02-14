"""GYファクター定義のテスト。

各ファクタールールの式がevaluate_rule()で正しく評価されることを検証する。
"""

import pytest

from src.factors.rules.gy_factors import GY_INITIAL_FACTORS
from src.scoring.evaluator import evaluate_rule


# --- テスト用ヘルパー ---
def _base_horse(**overrides) -> dict:
    """テスト用の馬データを生成する。"""
    horse = {
        "Umaban": "03", "Wakuban": "2", "SexCD": "1", "Barei": "4",
        "Futan": "550", "Ninki": "5", "KakuteiJyuni": "3", "Odds": "100",
        "BaTaijyu": "480", "ZogenFugo": "+", "ZogenSa": "2",
        "DMJyuni": "4", "HaronTimeL3": "350", "KyakusituKubun": "2",
        "Jyuni4c": "3",
    }
    horse.update(overrides)
    return horse


def _base_race(**overrides) -> dict:
    """テスト用のレースデータを生成する。"""
    race = {
        "Kyori": "1600", "TrackCD": "11", "TenkoCD": "1", "RaceNum": "5",
    }
    race.update(overrides)
    return race


def _entries(n: int = 12) -> list[dict]:
    """テスト用の出走馬リストを生成する。"""
    return [
        {"Umaban": f"{i:02d}", "HaronTimeL3": str(340 + i * 5)}
        for i in range(1, n + 1)
    ]


def _find_factor(name: str) -> dict:
    """名前でファクターを検索する。"""
    for f in GY_INITIAL_FACTORS:
        if f["rule_name"] == name:
            return f
    raise ValueError(f"Factor not found: {name}")


def _eval(factor_name: str, horse=None, race=None, entries=None, prev=None) -> float:
    """ファクターを評価するショートカット。"""
    f = _find_factor(factor_name)
    h = horse or _base_horse()
    r = race or _base_race()
    e = entries or _entries()
    return evaluate_rule(f["sql_expression"], h, r, e, prev)


# --- 構造テスト ---
class TestGYFactorsStructure:
    """ファクター定義の構造テスト。"""

    def test_total_count(self) -> None:
        """ファクター数が期待通りであること。"""
        assert len(GY_INITIAL_FACTORS) >= 25

    def test_required_fields(self) -> None:
        """全ファクターに必須フィールドがあること。"""
        for f in GY_INITIAL_FACTORS:
            assert "rule_name" in f, f"rule_name missing: {f}"
            assert "category" in f, f"category missing: {f}"
            assert "sql_expression" in f, f"sql_expression missing: {f}"
            assert "weight" in f, f"weight missing: {f}"

    def test_unique_names(self) -> None:
        """ファクター名が重複していないこと。"""
        names = [f["rule_name"] for f in GY_INITIAL_FACTORS]
        assert len(names) == len(set(names))

    def test_weights_positive(self) -> None:
        """全ファクターのweightが正の値であること。"""
        for f in GY_INITIAL_FACTORS:
            assert f["weight"] > 0, f"{f['rule_name']} has non-positive weight"

    def test_all_expressions_parseable(self) -> None:
        """全ファクターの式がエラーなく評価できること。"""
        horse = _base_horse()
        race = _base_race()
        entries = _entries()
        prev = _base_horse(KakuteiJyuni="5", KyakusituKubun="3", Jyuni4c="6")

        for f in GY_INITIAL_FACTORS:
            result = evaluate_rule(f["sql_expression"], horse, race, entries, prev)
            assert isinstance(result, float), f"{f['rule_name']} returned non-float"

    def test_categories_not_empty(self) -> None:
        """全ファクターのcategoryが空でないこと。"""
        for f in GY_INITIAL_FACTORS:
            assert f["category"].strip(), f"{f['rule_name']} has empty category"


# --- 個別ファクターテスト ---
class TestFormFactors:
    """前走関連ファクターのテスト。"""

    def test_prev_top_finish_penalty(self) -> None:
        """前走上位着順減点: 前走1-3着で-1。"""
        prev = _base_horse(KakuteiJyuni="2")
        assert _eval("前走上位着順減点", prev=prev) == -1.0

    def test_prev_top_finish_no_penalty(self) -> None:
        """前走上位着順減点: 前走5着で0。"""
        prev = _base_horse(KakuteiJyuni="5")
        assert _eval("前走上位着順減点", prev=prev) == 0.0

    def test_prev_bad_finish_bonus(self) -> None:
        """前走大敗加点: 前走12着で+1。"""
        prev = _base_horse(KakuteiJyuni="12")
        assert _eval("前走大敗加点", prev=prev) == 1.0

    def test_prev_bad_finish_no_bonus(self) -> None:
        """前走大敗加点: 前走5着で0。"""
        prev = _base_horse(KakuteiJyuni="5")
        assert _eval("前走大敗加点", prev=prev) == 0.0

    def test_prev_mid_stable(self) -> None:
        """前走中位安定: 前走5着で0.5。"""
        prev = _base_horse(KakuteiJyuni="5")
        assert _eval("前走中位安定", prev=prev) == 0.5


class TestDMFactors:
    """DM予想関連ファクターのテスト。"""

    def test_dm_top(self) -> None:
        """DM予想上位: DM3位以内で+1。"""
        horse = _base_horse(DMJyuni="2")
        assert _eval("DM予想上位", horse=horse) == 1.0

    def test_dm_top_outside(self) -> None:
        """DM予想上位: DM5位で0。"""
        horse = _base_horse(DMJyuni="5")
        assert _eval("DM予想上位", horse=horse) == 0.0

    def test_longshot_dm_high(self) -> None:
        """穴馬DM高評価: 人気薄+DM上位で+2。"""
        horse = _base_horse(Ninki="10", DMJyuni="3")
        assert _eval("穴馬DM高評価", horse=horse) == 2.0

    def test_longshot_dm_low(self) -> None:
        """穴馬DM高評価: 人気薄だがDM下位で0。"""
        horse = _base_horse(Ninki="10", DMJyuni="8")
        assert _eval("穴馬DM高評価", horse=horse) == 0.0

    def test_popular_dm_low(self) -> None:
        """人気馬DM低評価: 人気だがDM下位で-1。"""
        horse = _base_horse(Ninki="2", DMJyuni="10")
        entries = _entries(12)
        assert _eval("人気馬DM低評価", horse=horse, entries=entries) == -1.0


class TestGateFactors:
    """枠順関連ファクターのテスト。"""

    def test_inner_gate_sprint_turf(self) -> None:
        """内枠有利(短距離芝): 芝+短距離+内枠で+1。"""
        horse = _base_horse(Umaban="02")
        race = _base_race(Kyori="1200", TrackCD="11")
        assert _eval("内枠有利(短距離芝)", horse=horse, race=race) == 1.0

    def test_outer_gate_sprint_turf(self) -> None:
        """外枠不利(短距離芝): 芝+短距離+外枠で-0.5。"""
        horse = _base_horse(Umaban="11")
        race = _base_race(Kyori="1200", TrackCD="11")
        assert _eval("外枠不利(短距離芝)", horse=horse, race=race) == -0.5

    def test_even_gate_bonus(self) -> None:
        """偶数枠加点: 偶数枠で0.3。"""
        horse = _base_horse(Wakuban="4")
        assert _eval("偶数枠加点", horse=horse) == pytest.approx(0.3)


class TestWeightFactors:
    """馬体重・斤量関連ファクターのテスト。"""

    def test_large_weight_change(self) -> None:
        """大幅増減警戒: 体重差12kgで-1。"""
        horse = _base_horse(ZogenFugo="+", ZogenSa="12")
        assert _eval("大幅増減警戒", horse=horse) == -1.0

    def test_stable_weight(self) -> None:
        """適正体重維持: 体重差1kgで+0.5。"""
        horse = _base_horse(ZogenFugo="+", ZogenSa="1")
        assert _eval("適正体重維持", horse=horse) == 0.5

    def test_light_impost(self) -> None:
        """軽斤量加点: 52kgで+0.5。"""
        horse = _base_horse(Futan="520")
        assert _eval("軽斤量加点", horse=horse) == 0.5

    def test_heavy_impost(self) -> None:
        """重斤量減点: 59kgで-0.5。"""
        horse = _base_horse(Futan="590")
        assert _eval("重斤量減点", horse=horse) == -0.5


class TestGenderAgeFactors:
    """性別・馬齢関連ファクターのテスト。"""

    def test_female_mile(self) -> None:
        """牝馬加点(マイル以下): 牝馬+マイルで+0.5。"""
        horse = _base_horse(SexCD="2")
        race = _base_race(Kyori="1600")
        assert _eval("牝馬加点(マイル以下)", horse=horse, race=race) == 0.5

    def test_old_horse_penalty(self) -> None:
        """高齢馬減点: 8歳で-0.5。"""
        horse = _base_horse(Barei="8")
        assert _eval("高齢馬減点", horse=horse) == -0.5

    def test_young_horse_bonus(self) -> None:
        """若馬加点: 3歳で+0.5。"""
        horse = _base_horse(Barei="3")
        assert _eval("若馬加点", horse=horse) == 0.5

    def test_gelding_dirt(self) -> None:
        """セン馬安定力: セン馬+ダートで+0.5。"""
        horse = _base_horse(SexCD="3")
        race = _base_race(TrackCD="23")
        assert _eval("セン馬安定力", horse=horse, race=race) == 0.5


class TestSpeedFactors:
    """上がり3F関連ファクターのテスト。"""

    def test_prev_l3f_top(self) -> None:
        """上がり3F上位: 前走L3Fランク2位で+1。"""
        prev = _base_horse(HaronTimeL3="340")
        all_prev_l3f = [340.0, 345.0, 350.0, 355.0, 360.0]
        f = _find_factor("上がり3F上位")
        result = evaluate_rule(f["sql_expression"], _base_horse(), _base_race(), _entries(), prev, all_prev_l3f)
        assert result == 1.0

    def test_longshot_fast_finish(self) -> None:
        """穴馬末脚: 人気薄+前走L3F上位で+1.5。"""
        horse = _base_horse(Ninki="10")
        prev = _base_horse(HaronTimeL3="340")
        all_prev_l3f = [340.0, 345.0, 350.0, 355.0, 360.0]
        f = _find_factor("穴馬末脚")
        result = evaluate_rule(f["sql_expression"], horse, _base_race(), _entries(), prev, all_prev_l3f)
        assert result == 1.5


class TestPaceFactors:
    """脚質関連ファクターのテスト。"""

    def test_front_runner_sprint(self) -> None:
        """逃げ先行有利(短距離): 短距離+前走先行で+0.5。"""
        prev = _base_horse(KyakusituKubun="1")
        race = _base_race(Kyori="1200")
        assert _eval("逃げ先行有利(短距離)", race=race, prev=prev) == 0.5

    def test_closer_long_distance(self) -> None:
        """差し追込有利(長距離): 長距離+前走差しで+0.5。"""
        prev = _base_horse(KyakusituKubun="3")
        race = _base_race(Kyori="2400")
        assert _eval("差し追込有利(長距離)", race=race, prev=prev) == 0.5

    def test_corner4_good_position(self) -> None:
        """4角好位置: 前走4角3番手で+0.5。"""
        prev = _base_horse(Jyuni4c="3")
        assert _eval("4角好位置", prev=prev) == 0.5


class TestOddsFactors:
    """オッズ関連ファクターのテスト。"""

    def test_mid_odds_value(self) -> None:
        """中穴ゾーン妙味: オッズ15倍(150)で+1。"""
        horse = _base_horse(Odds="150")
        assert _eval("中穴ゾーン妙味", horse=horse) == 1.0

    def test_over_popular(self) -> None:
        """過剰人気検出: 1番人気+オッズ1.5倍(15)で-0.8。"""
        horse = _base_horse(Ninki="1", Odds="15")
        assert _eval("過剰人気検出", horse=horse) == pytest.approx(-0.8)


class TestComboFactors:
    """複合ファクターのテスト。"""

    def test_triple_signal(self) -> None:
        """穴馬三重シグナル: DM上位+末脚上位+人気薄で+2.0。"""
        horse = _base_horse(Ninki="10", DMJyuni="3")
        prev = _base_horse(HaronTimeL3="340")
        all_prev_l3f = [340.0, 345.0, 350.0, 355.0, 360.0]
        f = _find_factor("穴馬三重シグナル")
        result = evaluate_rule(f["sql_expression"], horse, _base_race(), _entries(), prev, all_prev_l3f)
        assert result == 2.0

    def test_dm_divergence_value(self) -> None:
        """DM乖離バリュー: 人気9位-DM3位=6の乖離で+1.5。"""
        horse = _base_horse(Ninki="9", DMJyuni="3")
        assert _eval("DM乖離バリュー", horse=horse) == 1.5
