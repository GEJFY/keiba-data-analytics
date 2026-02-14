"""スコアリングルール評価エンジンのテスト。

JVLink実スキーマ（BaTaijyu/ZogenFugo/ZogenSa、DMJyuni、HaronTimeL3等）に準拠。
"""

import pytest

from src.scoring.evaluator import build_eval_context, evaluate_rule


@pytest.fixture
def sample_horse():
    return {
        "Umaban": "03",
        "Wakuban": "2",
        "SexCD": "2",  # 牝馬
        "Barei": "3",
        "Futan": "540",  # 54.0kg（0.1kg単位）
        "Ninki": "8",
        "KakuteiJyuni": "5",
        "BaTaijyu": "480",
        "ZogenFugo": "+",
        "ZogenSa": "4",
        "DMJyuni": "3",
        "HaronTimeL3": "345",  # 34.5秒
        "KyakusituKubun": "3",  # 差し
        "Jyuni4c": "6",
        "Odds": "85",  # 8.5倍
    }


@pytest.fixture
def sample_race():
    return {
        "Kyori": "1600",
        "TrackCD": "10",  # 芝左
        "TenkoCD": "1",   # 晴
        "RaceNum": "06",
    }


@pytest.fixture
def sample_entries(sample_horse):
    entries = [sample_horse]
    for i in range(1, 12):
        entries.append({
            "Umaban": f"{i + 1:02d}",
            "Ninki": str(i),
            "DMJyuni": str(i + 1),
            "HaronTimeL3": str(340 + i * 3),
            "KyakusituKubun": str((i % 4) + 1),
            "Jyuni4c": str(i),
            "Odds": str(20 + i * 10),
        })
    return entries


class TestBuildEvalContext:
    """build_eval_context のテスト。"""

    def test_basic_variables(self, sample_horse, sample_race, sample_entries):
        ctx = build_eval_context(sample_horse, sample_race, sample_entries)
        assert ctx["Umaban"] == 3
        assert ctx["SexCD"] == "2"
        assert ctx["Barei"] == 3
        assert ctx["Kyori"] == 1600
        assert ctx["TrackCD"] == "10"

    def test_derived_variables(self, sample_horse, sample_race, sample_entries):
        ctx = build_eval_context(sample_horse, sample_race, sample_entries)
        assert ctx["weight"] == 480
        assert ctx["weight_diff"] == 4
        assert ctx["is_turf"] is True   # TrackCD "10" starts with "1"
        assert ctx["is_dirt"] is False
        assert ctx["is_mile"] is True   # 1600m
        assert ctx["is_female"] is True  # SexCD "2"
        assert ctx["num_entries"] == 12

    def test_jvlink_specific_variables(self, sample_horse, sample_race, sample_entries):
        """JVLink固有データ（DMJyuni, HaronTimeL3, KyakusituKubun等）。"""
        ctx = build_eval_context(sample_horse, sample_race, sample_entries)
        assert ctx["dm_rank"] == 3
        assert ctx["last_3f"] == 345.0
        assert ctx["running_style"] == 3  # 差し
        assert ctx["corner4_pos"] == 6
        assert ctx["is_front_runner"] is False  # running_style=3は差し
        assert ctx["is_closer"] is True

    def test_weight_diff_negative(self):
        """ZogenFugo='-'の場合weight_diffが負になること。"""
        horse = {"BaTaijyu": "460", "ZogenFugo": "-", "ZogenSa": "2", "Umaban": "1"}
        ctx = build_eval_context(horse, {}, [horse])
        assert ctx["weight"] == 460
        assert ctx["weight_diff"] == -2

    def test_safe_builtins(self, sample_horse, sample_race, sample_entries):
        ctx = build_eval_context(sample_horse, sample_race, sample_entries)
        assert "abs" in ctx
        assert "int" in ctx
        assert "max" in ctx


class TestEvaluateRule:
    """evaluate_rule のテスト。"""

    def test_simple_condition_true(self, sample_horse, sample_race, sample_entries):
        result = evaluate_rule(
            "1 if Barei == 3 else 0",
            sample_horse, sample_race, sample_entries,
        )
        assert result == 1.0

    def test_simple_condition_false(self, sample_horse, sample_race, sample_entries):
        result = evaluate_rule(
            "1 if Barei == 5 else 0",
            sample_horse, sample_race, sample_entries,
        )
        assert result == 0.0

    def test_complex_expression(self, sample_horse, sample_race, sample_entries):
        result = evaluate_rule(
            "0.5 if is_female and is_mile else 0",
            sample_horse, sample_race, sample_entries,
        )
        assert result == 0.5

    def test_empty_expression(self, sample_horse, sample_race, sample_entries):
        result = evaluate_rule("", sample_horse, sample_race, sample_entries)
        assert result == 0.0

    def test_forbidden_pattern(self, sample_horse, sample_race, sample_entries):
        result = evaluate_rule(
            "__import__('os')",
            sample_horse, sample_race, sample_entries,
        )
        assert result == 0.0

    def test_invalid_expression(self, sample_horse, sample_race, sample_entries):
        result = evaluate_rule(
            "undefined_variable + 1",
            sample_horse, sample_race, sample_entries,
        )
        assert result == 0.0

    def test_numeric_result(self, sample_horse, sample_race, sample_entries):
        result = evaluate_rule(
            "-1 if KakuteiJyuni <= 3 else 0.5",
            sample_horse, sample_race, sample_entries,
        )
        # KakuteiJyuni=5 なので else 分岐
        assert result == 0.5

    def test_weight_diff(self, sample_horse, sample_race, sample_entries):
        result = evaluate_rule(
            "-1 if abs(weight_diff) >= 10 else 0",
            sample_horse, sample_race, sample_entries,
        )
        # weight_diff=4 なので 0
        assert result == 0.0

    def test_longshot_detection(self, sample_horse, sample_race, sample_entries):
        result = evaluate_rule(
            "1 if is_longshot else 0",
            sample_horse, sample_race, sample_entries,
        )
        # Ninki=8, num_entries=12, threshold=max(12-3,4)=9 → 8<9 → not longshot
        assert result == 0.0

    def test_dm_rank_expression(self, sample_horse, sample_race, sample_entries):
        """DMJyuni（マイニング予想順位）を使ったルール評価。"""
        result = evaluate_rule(
            "1 if dm_rank <= 3 else 0",
            sample_horse, sample_race, sample_entries,
        )
        # dm_rank=3 なので True
        assert result == 1.0

    def test_running_style_expression(self, sample_horse, sample_race, sample_entries):
        """脚質を使ったルール評価。"""
        result = evaluate_rule(
            "1 if is_closer else 0",
            sample_horse, sample_race, sample_entries,
        )
        # running_style=3（差し）→ is_closer=True
        assert result == 1.0

    def test_corner4_expression(self, sample_horse, sample_race, sample_entries):
        """4コーナー順位を使ったルール評価。"""
        result = evaluate_rule(
            "1 if corner4_pos <= 3 else -1",
            sample_horse, sample_race, sample_entries,
        )
        # corner4_pos=6 → -1
        assert result == -1.0


class TestPrevContext:
    """前走データ（prev_context）関連のテスト。"""

    @pytest.fixture
    def prev_context(self):
        return {
            "KakuteiJyuni": "2",
            "HaronTimeL3": "340",
            "KyakusituKubun": "1",  # 逃げ
            "Jyuni4c": "3",
        }

    def test_prev_context_variables(self, sample_horse, sample_race, sample_entries, prev_context):
        """prev_context渡し時にprev_変数が正しくセットされること。"""
        ctx = build_eval_context(
            sample_horse, sample_race, sample_entries,
            prev_context=prev_context,
        )
        assert ctx["prev_jyuni"] == 2
        assert ctx["prev_last_3f"] == 340.0
        assert ctx["prev_running_style"] == 1
        assert ctx["prev_corner4_pos"] == 3
        assert ctx["prev_is_front_runner"] is True  # running_style=1(逃げ)
        assert ctx["prev_is_closer"] is False

    def test_prev_context_none(self, sample_horse, sample_race, sample_entries):
        """prev_context=None時にprev_変数が全てデフォルト値であること。"""
        ctx = build_eval_context(
            sample_horse, sample_race, sample_entries,
            prev_context=None,
        )
        assert ctx["prev_jyuni"] == 0
        assert ctx["prev_last_3f"] == 0.0
        assert ctx["prev_running_style"] == 0
        assert ctx["prev_corner4_pos"] == 0
        assert ctx["prev_is_front_runner"] is False
        assert ctx["prev_is_closer"] is False

    def test_prev_jyuni_expression(self, sample_horse, sample_race, sample_entries, prev_context):
        """前走着順を使ったルール評価。"""
        result = evaluate_rule(
            "-1 if prev_jyuni > 0 and prev_jyuni <= 3 else 0",
            sample_horse, sample_race, sample_entries,
            prev_context=prev_context,
        )
        # prev_jyuni=2 → -1
        assert result == -1.0

    def test_prev_jyuni_no_prev(self, sample_horse, sample_race, sample_entries):
        """前走なし時に前走着順ルールが非発火すること。"""
        result = evaluate_rule(
            "-1 if prev_jyuni > 0 and prev_jyuni <= 3 else 0",
            sample_horse, sample_race, sample_entries,
            prev_context=None,
        )
        # prev_jyuni=0 → ガード条件で0
        assert result == 0.0

    def test_prev_last_3f_rank(self, sample_horse, sample_race, sample_entries, prev_context):
        """前走上がり3Fランクが正しく計算されること。"""
        all_prev_l3f = [340.0, 350.0, 360.0]  # 3頭分の前走L3F
        ctx = build_eval_context(
            sample_horse, sample_race, sample_entries,
            prev_context=prev_context,
            all_prev_l3f=all_prev_l3f,
        )
        # prev_last_3f=340.0 は最速 → rank=1
        assert ctx["prev_last_3f_rank"] == 1

    def test_prev_last_3f_rank_no_data(self, sample_horse, sample_race, sample_entries):
        """前走L3Fデータなし時のランクはnum_entriesになること。"""
        ctx = build_eval_context(
            sample_horse, sample_race, sample_entries,
            prev_context=None,
            all_prev_l3f=None,
        )
        assert ctx["prev_last_3f_rank"] == len(sample_entries)

    def test_prev_is_front_runner_expression(
        self, sample_horse, sample_race, sample_entries, prev_context
    ):
        """前走脚質を使ったルール評価。"""
        result = evaluate_rule(
            "0.5 if prev_is_front_runner and is_sprint else 0",
            sample_horse, sample_race, sample_entries,
            prev_context=prev_context,
        )
        # prev_is_front_runner=True, is_sprint=False(1600m) → 0
        assert result == 0.0
