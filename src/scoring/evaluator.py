"""ファクタールール評価エンジン。

factor_rulesテーブルのsql_expressionを安全に評価し、
各馬に対するスコア（+1, 0, -1等）を返す。

評価方式:
    - Python式: 馬データ・レースデータの値を変数として参照可能
    - 条件式は制限された安全な環境（__builtins__無効化）で評価

対応データソース:
    JVLinkToSQLite の NL_RA_RACE / NL_SE_RACE_UMA テーブル。
    IDM/SpeedIndex はJVLinkに存在しないため非対応。
    代替: DMJyuni（マイニング予想順位）、HaronTimeL3（上がり3F）。
"""

import re
from typing import Any

from loguru import logger


# 許可する組み込み関数（安全なもののみ）
_SAFE_BUILTINS = {
    "abs": abs,
    "int": int,
    "float": float,
    "str": str,
    "max": max,
    "min": min,
    "len": len,
    "round": round,
    "bool": bool,
}

# 禁止するキーワード（コードインジェクション防止）
_FORBIDDEN_PATTERNS = re.compile(
    r"(__import__|import |exec\b|eval\b|compile\b|open\b|getattr\b|setattr\b|delattr\b|globals\b|locals\b|\bdir\b|vars\b)"
)


def _safe_int(value: Any, default: int = 0) -> int:
    """安全に整数変換する。"""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    """安全に浮動小数点変換する。"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def build_eval_context(
    horse: dict[str, Any],
    race: dict[str, Any],
    all_entries: list[dict[str, Any]],
    prev_context: dict[str, Any] | None = None,
    all_prev_l3f: list[float] | None = None,
) -> dict[str, Any]:
    """ルール評価用のコンテキスト変数を構築する。

    JVLinkToSQLiteの実カラム名（BaTaijyu, ZogenFugo, ZogenSa, DMJyuni等）に対応。

    Args:
        horse: 対象馬データ（NL_SE_RACE_UMAレコード、provider正規化済み）
        race: レース情報（NL_RA_RACEレコード、provider正規化済み）
        all_entries: 同レース全出走馬データ
        prev_context: 前走の出走馬データ（NL_SE_RACE_UMAレコード）。Noneで前走なし。
        all_prev_l3f: 同レース全馬の前走HaronTimeL3リスト（prev_last_3f_rank計算用）

    Returns:
        eval()で使用する変数辞書
    """
    # --- 馬体重: BaTaijyu（体重）+ ZogenFugo（符号）+ ZogenSa（差） ---
    weight_val = _safe_int(horse.get("BaTaijyu", 0))
    zogen_fugo = str(horse.get("ZogenFugo", "")).strip()
    zogen_sa = _safe_int(horse.get("ZogenSa", 0))
    weight_diff = -zogen_sa if zogen_fugo == "-" else zogen_sa

    num_entries = len(all_entries)

    # 馬番の相対位置（内枠=小, 外枠=大）
    umaban = _safe_int(horse.get("Umaban", 0))
    gate_position = umaban / max(num_entries, 1)  # 0.0〜1.0

    # --- DMJyuni（マイニング予想順位）--- JRA公式AI予測
    dm_rank = _safe_int(horse.get("DMJyuni", 0))
    if dm_rank == 0:
        dm_rank = num_entries  # 未設定時は最下位扱い

    # --- 上がり3ハロン ---
    last_3f = _safe_float(horse.get("HaronTimeL3", 0))

    # 上がり3F 順位（小さいほど速い）
    horse_l3f = last_3f
    l3f_values = sorted(
        [_safe_float(e.get("HaronTimeL3", 0)) for e in all_entries if _safe_float(e.get("HaronTimeL3", 0)) > 0],
    )
    if horse_l3f > 0 and horse_l3f in l3f_values:
        last_3f_rank = l3f_values.index(horse_l3f) + 1
    else:
        last_3f_rank = num_entries

    # 人気順
    ninki = _safe_int(horse.get("Ninki", 0))

    # 着順（過去データ参照時に利用）
    kakutei_jyuni = _safe_int(horse.get("KakuteiJyuni", 0))

    # 脚質判定（1:逃 2:先 3:差 4:追 0:初期値）
    running_style = _safe_int(horse.get("KyakusituKubun", 0))

    # 4コーナー順位
    corner4_pos = _safe_int(horse.get("Jyuni4c", 0))

    # 単勝オッズ（NL_SE_RACE_UMA内のOddsカラム）
    odds = _safe_float(horse.get("Odds", 0))

    # トラックコード判定
    track_cd = str(race.get("TrackCD", ""))

    # --- 前走データ（prev_context） ---
    prev = prev_context or {}
    prev_jyuni = _safe_int(prev.get("KakuteiJyuni", 0))
    prev_last_3f = _safe_float(prev.get("HaronTimeL3", 0))
    prev_running_style = _safe_int(prev.get("KyakusituKubun", 0))
    prev_corner4_pos = _safe_int(prev.get("Jyuni4c", 0))

    # 前走上がり3Fランク（同レース全馬の前走L3F中の順位）
    if all_prev_l3f and prev_last_3f > 0:
        sorted_l3f = sorted([v for v in all_prev_l3f if v > 0])
        if prev_last_3f in sorted_l3f:
            prev_last_3f_rank = sorted_l3f.index(prev_last_3f) + 1
        else:
            prev_last_3f_rank = num_entries
    else:
        prev_last_3f_rank = num_entries

    ctx = {
        # --- NL_SE_RACE_UMA 直接値 ---
        "Umaban": umaban,
        "Wakuban": _safe_int(horse.get("Wakuban", 0)),
        "SexCD": str(horse.get("SexCD", "")),
        "Barei": _safe_int(horse.get("Barei", 0)),
        "Futan": _safe_float(horse.get("Futan", 0)),
        "Ninki": ninki,
        "KakuteiJyuni": kakutei_jyuni,
        "Odds": odds,
        # --- NL_RA_RACE 直接値 ---
        "Kyori": _safe_int(race.get("Kyori", 0)),
        "TrackCD": track_cd,
        "TenkoCD": str(race.get("TenkoCD", "")),
        "RaceNum": _safe_int(race.get("RaceNum", 0)),
        # --- 派生変数 ---
        "weight": weight_val,
        "weight_diff": weight_diff,
        "num_entries": num_entries,
        "gate_position": gate_position,
        "is_inner_gate": umaban <= max(num_entries // 3, 1),
        "is_outer_gate": umaban > (num_entries * 2) // 3,
        # --- JRA公式AIデータ ---
        "dm_rank": dm_rank,
        "last_3f": last_3f,
        "last_3f_rank": last_3f_rank,
        # --- 脚質・位置取り ---
        "running_style": running_style,
        "corner4_pos": corner4_pos,
        "is_front_runner": running_style in (1, 2),  # 逃げ or 先行
        "is_closer": running_style in (3, 4),  # 差し or 追込
        # --- 単勝オッズ ---
        "odds": odds,
        # --- フラグ ---
        "is_favorite": ninki <= 3,
        "is_longshot": ninki >= max(num_entries - 3, 4),
        "is_turf": track_cd.startswith("1"),  # 10-19: 芝系
        "is_dirt": track_cd.startswith("2"),  # 20-29: ダート系
        "is_sprint": _safe_int(race.get("Kyori", 0)) <= 1400,
        "is_mile": 1400 < _safe_int(race.get("Kyori", 0)) <= 1800,
        "is_middle": 1800 < _safe_int(race.get("Kyori", 0)) <= 2200,
        "is_long": _safe_int(race.get("Kyori", 0)) > 2200,
        "is_male": str(horse.get("SexCD", "")) == "1",
        "is_female": str(horse.get("SexCD", "")) == "2",
        "is_gelding": str(horse.get("SexCD", "")) == "3",
        # --- 前走データ ---
        "prev_jyuni": prev_jyuni,
        "prev_last_3f": prev_last_3f,
        "prev_last_3f_rank": prev_last_3f_rank,
        "prev_running_style": prev_running_style,
        "prev_corner4_pos": prev_corner4_pos,
        "prev_is_front_runner": prev_running_style in (1, 2),
        "prev_is_closer": prev_running_style in (3, 4),
        # --- 安全な組み込み ---
        **_SAFE_BUILTINS,
    }
    return ctx


def evaluate_rule(
    expression: str,
    horse: dict[str, Any],
    race: dict[str, Any],
    all_entries: list[dict[str, Any]],
    prev_context: dict[str, Any] | None = None,
    all_prev_l3f: list[float] | None = None,
) -> float:
    """ファクタールールの式を評価し、スコアを返す。

    Args:
        expression: Python式文字列（例: '1 if is_longshot and dm_rank <= 3 else 0'）
        horse: 対象馬データ
        race: レース情報
        all_entries: 同レース全出走馬データ
        prev_context: 前走の出走馬データ（Noneで前走なし）
        all_prev_l3f: 同レース全馬の前走HaronTimeL3リスト

    Returns:
        評価結果のスコア（float）。評価失敗時は0.0。
    """
    if not expression or not expression.strip():
        return 0.0

    # セキュリティチェック
    if _FORBIDDEN_PATTERNS.search(expression):
        logger.warning(f"禁止パターンを検出: {expression[:80]}")
        return 0.0

    ctx = build_eval_context(horse, race, all_entries, prev_context, all_prev_l3f)

    try:
        result = eval(expression, {"__builtins__": {}}, ctx)  # noqa: S307
        return float(result)
    except Exception as e:
        logger.debug(f"ルール評価エラー ({expression[:50]}): {e}")
        return 0.0
