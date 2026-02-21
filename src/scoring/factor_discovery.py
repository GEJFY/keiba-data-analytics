"""データドリブンファクター発見モジュール。

仮説ベースではなく、過去の実績データから予測に有効な特徴量を
自動的に発見・評価する。

分析手法:
    1. 単変量分析: 各カラムと着順の相関・AUC
    2. レース内ランク変換: 同レース内での順位に変換して分析
    3. 区間分析: 変数の範囲別に的中率を計算
    4. 派生変数生成: 交互作用・比率・カテゴリ変換
    5. 候補ファクター式の自動生成
"""

import math
from collections import defaultdict
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (ValueError, TypeError):
        return default


def _auc_from_labels(scores: list[float], labels: list[int]) -> float:
    """AUC (Area Under ROC Curve) を計算する。

    ソートベースのランク法で O(n log n) で計算する。
    旧実装の O(n_pos × n_neg) = O(n²) から大幅に高速化。
    """
    if not scores or not labels:
        return 0.5
    n = len(scores)
    n_pos = sum(labels)
    n_neg = n - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.5

    # スコアでソート
    paired = sorted(zip(scores, labels, strict=False), key=lambda x: x[0])

    # タイ（同値）を考慮したランク割当てで正例のランク和を計算
    rank_sum = 0.0
    i = 0
    while i < n:
        j = i
        while j < n and paired[j][0] == paired[i][0]:
            j += 1
        # 同値グループの平均ランク（1-indexed）
        avg_rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            if paired[k][1] == 1:
                rank_sum += avg_rank
        i = j

    return (rank_sum - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)


def _point_biserial(scores: list[float], labels: list[int]) -> float:
    """点双列相関係数を計算する。"""
    if len(scores) < 2:
        return 0.0
    n = len(scores)
    n1 = sum(labels)
    n0 = n - n1
    if n0 == 0 or n1 == 0:
        return 0.0

    mean_1 = sum(s for s, lb in zip(scores, labels, strict=False) if lb == 1) / n1
    mean_0 = sum(s for s, lb in zip(scores, labels, strict=False) if lb == 0) / n0
    overall_mean = sum(scores) / n
    overall_var = sum((s - overall_mean) ** 2 for s in scores) / n
    if overall_var == 0:
        return 0.0
    std = math.sqrt(overall_var)
    return (mean_1 - mean_0) / std * math.sqrt(n1 * n0 / (n * n))


class FactorDiscovery:
    """データドリブンファクター発見エンジン。

    JVLink DBの実レースデータを分析し、着順予測に有効な
    変数・条件・派生特徴を自動発見する。

    Args:
        jvlink_db: JVLink DB（NL_SE_RACE_UMA, NL_RA_RACE）
        ext_db: 拡張DB（factor_rules参照用）
    """

    # 分析対象の基本カラム（NL_SE_RACE_UMA）
    _BASE_COLUMNS = [
        "BaTaijyu", "ZogenSa", "Futan", "Ninki", "Odds",
        "DMJyuni", "HaronTimeL3", "HaronTimeL4", "Barei",
        "KyakusituKubun", "Jyuni4c", "Wakuban", "Umaban",
    ]

    # 日本語カラム名
    _COL_NAMES = {
        "BaTaijyu": "馬体重",
        "ZogenSa": "増減差",
        "Futan": "斤量",
        "Ninki": "人気",
        "Odds": "単勝オッズ",
        "DMJyuni": "DM予想順位",
        "HaronTimeL3": "上がり3F",
        "HaronTimeL4": "上がり4F",
        "Barei": "馬齢",
        "KyakusituKubun": "脚質",
        "Jyuni4c": "4角順位",
        "Wakuban": "枠番",
        "Umaban": "馬番",
        "is_good_baba": "良馬場",
        "is_heavy_baba": "重馬場",
        "is_graded": "重賞",
        "position_change": "位置変化",
        "is_makuri": "まくり",
    }

    def __init__(
        self,
        jvlink_db: DatabaseManager,
        ext_db: DatabaseManager | None = None,
    ) -> None:
        self._jvlink_db = jvlink_db
        self._ext_db = ext_db

    def _load_dataset(
        self,
        date_from: str = "",
        date_to: str = "",
        max_races: int = 3000,
        target_jyuni: int = 1,
    ) -> tuple[list[dict[str, Any]], list[int]]:
        """分析用データセットを構築する。

        Returns:
            (features_list, labels) — features_listは各馬のdict、labelsは0/1
        """
        if not self._jvlink_db.table_exists("NL_SE_RACE_UMA"):
            return [], []
        if not self._jvlink_db.table_exists("NL_RA_RACE"):
            return [], []

        # レースキー取得
        where_parts = []
        params: list[Any] = []
        if date_from:
            where_parts.append("(r.idYear > ? OR (r.idYear = ? AND r.idMonthDay >= ?))")
            y, md = date_from[:4], date_from[4:]
            params.extend([y, y, md])
        if date_to:
            where_parts.append("(r.idYear < ? OR (r.idYear = ? AND r.idMonthDay <= ?))")
            y, md = date_to[:4], date_to[4:]
            params.extend([y, y, md])

        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        params.append(max_races)

        race_keys = self._jvlink_db.execute_query(
            f"""SELECT r.idYear, r.idMonthDay, r.idJyoCD, r.idKaiji,
                       r.idNichiji, r.idRaceNum,
                       r.Kyori, r.TrackCD, r.SyussoTosu
                FROM NL_RA_RACE r
                {where_clause}
                ORDER BY r.idYear DESC, r.idMonthDay DESC
                LIMIT ?""",
            tuple(params),
        )

        if not race_keys:
            return [], []

        # レースキーのセットとレース情報マップを構築
        valid_keys: set[tuple[str, ...]] = set()
        race_info_map: dict[tuple[str, ...], dict[str, Any]] = {}
        for rk in race_keys:
            key: tuple[str, ...] = (rk["idYear"], rk["idMonthDay"], rk["idJyoCD"],
                   rk["idKaiji"], rk["idNichiji"], rk["idRaceNum"])
            valid_keys.add(key)
            race_info_map[key] = rk

        # 日付範囲で一括取得（N+1 → 1クエリに削減）
        # race_keysはDESC順 → 最後が最古、最初が最新
        first_race = race_keys[-1]
        last_race = race_keys[0]
        bound_parts = [
            "(idYear > ? OR (idYear = ? AND idMonthDay >= ?))",
            "(idYear < ? OR (idYear = ? AND idMonthDay <= ?))",
        ]
        bound_params: list[Any] = [
            first_race["idYear"], first_race["idYear"], first_race["idMonthDay"],
            last_race["idYear"], last_race["idYear"], last_race["idMonthDay"],
        ]

        all_entries = self._jvlink_db.execute_query(
            f"""SELECT idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum,
                       Umaban, Wakuban, SexCD, Barei, Futan,
                       Ninki, KakuteiJyuni, Odds,
                       BaTaijyu, ZogenFugo, ZogenSa,
                       DMJyuni, HaronTimeL3, HaronTimeL4,
                       KyakusituKubun, Jyuni4c
                FROM NL_SE_RACE_UMA
                WHERE {' AND '.join(bound_parts)}
                ORDER BY idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum,
                         CAST(Umaban AS INTEGER)""",
            tuple(bound_params),
        )

        # レースキーでグルーピング
        entries_by_race: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
        for entry in all_entries:
            key = (entry["idYear"], entry["idMonthDay"], entry["idJyoCD"],
                   entry["idKaiji"], entry["idNichiji"], entry["idRaceNum"])
            if key in valid_keys:
                entries_by_race[key].append(entry)

        features_list: list[dict[str, Any]] = []
        labels: list[int] = []

        for key, rk in race_info_map.items():
            entries = entries_by_race.get(key, [])

            if not entries or len(entries) < 3:
                continue

            num_entries = len(entries)
            kyori = _safe_int(rk.get("Kyori", 0))
            track_cd = str(rk.get("TrackCD", ""))

            for horse in entries:
                jyuni = _safe_int(horse.get("KakuteiJyuni", 0))
                if jyuni <= 0:
                    continue

                label = 1 if jyuni <= target_jyuni else 0
                labels.append(label)

                feat: dict[str, Any] = {}
                # 基本カラム
                for col in self._BASE_COLUMNS:
                    feat[col] = _safe_float(horse.get(col, 0))

                # レース情報
                feat["Kyori"] = kyori
                feat["TrackCD"] = track_cd
                feat["num_entries"] = num_entries

                # 派生変数
                umaban = _safe_int(horse.get("Umaban", 0))
                feat["gate_position"] = umaban / max(num_entries, 1)
                feat["is_inner"] = 1 if umaban <= max(num_entries // 3, 1) else 0
                feat["is_outer"] = 1 if umaban > (num_entries * 2) // 3 else 0

                ninki = _safe_int(horse.get("Ninki", 0))
                feat["is_favorite"] = 1 if 1 <= ninki <= 3 else 0
                feat["is_longshot"] = 1 if ninki >= max(num_entries - 3, 4) else 0

                odds = _safe_float(horse.get("Odds", 0))
                dm = _safe_float(horse.get("DMJyuni", 0))
                feat["odds_dm_gap"] = abs(ninki - dm) if ninki > 0 and dm > 0 else 0
                feat["odds_x_dm"] = odds * dm if odds > 0 and dm > 0 else 0

                weight_diff = _safe_int(horse.get("ZogenSa", 0))
                zogen_fugo = str(horse.get("ZogenFugo", "")).strip()
                if zogen_fugo == "-":
                    weight_diff = -weight_diff
                feat["weight_diff"] = weight_diff
                feat["weight_decrease"] = 1 if weight_diff < 0 else 0
                feat["weight_increase_large"] = 1 if weight_diff >= 10 else 0

                futan = _safe_float(horse.get("Futan", 0))
                feat["futan_per_weight"] = (
                    futan / max(_safe_float(horse.get("BaTaijyu", 1)), 1)
                )

                feat["is_turf"] = 1 if track_cd.startswith("1") else 0
                feat["is_dirt"] = 1 if track_cd.startswith("2") else 0
                feat["is_sprint"] = 1 if kyori <= 1400 else 0
                feat["is_mile"] = 1 if 1400 < kyori <= 1800 else 0
                feat["is_middle"] = 1 if 1800 < kyori <= 2200 else 0
                feat["is_long"] = 1 if kyori > 2200 else 0

                sex_cd = str(horse.get("SexCD", ""))
                feat["is_female"] = 1 if sex_cd == "2" else 0
                feat["is_gelding"] = 1 if sex_cd == "3" else 0

                barei = _safe_int(horse.get("Barei", 0))
                feat["is_young"] = 1 if barei <= 3 else 0
                feat["is_old"] = 1 if barei >= 7 else 0

                style = _safe_int(horse.get("KyakusituKubun", 0))
                feat["is_front_runner"] = 1 if style in (1, 2) else 0
                feat["is_closer"] = 1 if style in (3, 4) else 0

                corner4 = _safe_float(horse.get("Jyuni4c", 0))
                feat["corner4_relative"] = corner4 / max(num_entries, 1)

                l3f = _safe_float(horse.get("HaronTimeL3", 0))
                feat["l3f_fast"] = 1 if 0 < l3f <= 34.0 else 0
                feat["l3f_slow"] = 1 if l3f >= 37.0 else 0

                # 馬場状態
                siba_baba = str(rk.get("SibaBabaCD", ""))
                dirt_baba = str(rk.get("DirtBabaCD", ""))
                feat["is_good_baba"] = 1 if siba_baba == "1" or dirt_baba == "1" else 0
                feat["is_heavy_baba"] = 1 if siba_baba in ("3", "4") or dirt_baba in ("3", "4") else 0

                # レースグレード
                grade_cd = str(rk.get("GradeCD", ""))
                feat["is_graded"] = 1 if grade_cd in ("A", "B", "C") else 0

                # コーナー位置変化（追い込み度）
                corner1 = _safe_int(horse.get("Jyuni1c", 0))
                corner4_val = _safe_int(horse.get("Jyuni4c", 0))
                feat["position_change"] = corner1 - corner4_val if corner1 > 0 and corner4_val > 0 else 0
                feat["is_makuri"] = 1 if feat["position_change"] >= 5 else 0

                features_list.append(feat)

        logger.info(
            f"FactorDiscovery: {len(features_list)}サンプル読込 "
            f"(正例率 {sum(labels)/max(len(labels),1):.1%})"
        )
        return features_list, labels

    def discover(
        self,
        date_from: str = "",
        date_to: str = "",
        max_races: int = 3000,
        target_jyuni: int = 1,
        min_auc: float = 0.52,
        progress_callback: Any = None,
    ) -> dict[str, Any]:
        """データドリブンでファクター候補を発見する。

        Args:
            date_from: 分析開始日 (YYYYMMDD)
            date_to: 分析終了日 (YYYYMMDD)
            max_races: 最大レース数
            target_jyuni: 的中着順（1=単勝、3=複勝）
            min_auc: 候補表示の最低AUC閾値

        Returns:
            {
                "n_samples": int,
                "n_positive": int,
                "base_rate": float,
                "candidates": [
                    {
                        "name": str,
                        "description": str,
                        "auc": float,
                        "correlation": float,
                        "direction": str,  "higher_is_better" or "lower_is_better"
                        "quintile_rates": list[dict],
                        "suggested_expression": str,
                        "category": str,
                    }, ...
                ],
                "interactions": [...],
            }
        """
        if progress_callback:
            progress_callback(0, 3, "データセットを読込中...")

        with self._jvlink_db.session():
            features_list, labels = self._load_dataset(
                date_from, date_to, max_races, target_jyuni,
            )

        if len(features_list) < 100:
            return {
                "n_samples": len(features_list),
                "n_positive": sum(labels),
                "base_rate": 0.0,
                "candidates": [],
                "interactions": [],
            }

        n_samples = len(features_list)
        n_pos = sum(labels)
        base_rate = n_pos / n_samples

        # === 1. 単変量分析 ===
        all_feature_names = sorted(set().union(*(f.keys() for f in features_list)))
        # 数値特徴のみ
        numeric_features = [
            name for name in all_feature_names
            if name not in ("TrackCD",)
            and any(isinstance(f.get(name), int | float) for f in features_list)
        ]

        if progress_callback:
            progress_callback(1, 3, "単変量分析中...")

        candidates: list[dict[str, Any]] = []
        for _feat_idx, feat_name in enumerate(numeric_features):
            scores = [_safe_float(f.get(feat_name, 0)) for f in features_list]
            unique_vals = set(scores)
            if len(unique_vals) <= 1:
                continue

            # AUC
            auc = _auc_from_labels(scores, labels)
            # 方向を判定: AUC < 0.5 なら反転（低いほうが良い）
            if auc < 0.5:
                direction = "lower_is_better"
                effective_auc = 1.0 - auc
            else:
                direction = "higher_is_better"
                effective_auc = auc

            # 相関
            corr = _point_biserial(scores, labels)

            # 五分位分析
            quintile_rates = self._quintile_analysis(scores, labels)

            if effective_auc < min_auc:
                continue

            # カテゴリ・説明生成
            description = self._describe_feature(feat_name, direction, effective_auc)
            category = self._categorize_feature(feat_name)
            expression = self._suggest_expression(
                feat_name, direction, scores, labels, quintile_rates,
            )

            candidates.append({
                "name": feat_name,
                "display_name": self._COL_NAMES.get(feat_name, feat_name),
                "description": description,
                "auc": round(effective_auc, 4),
                "correlation": round(corr, 4),
                "direction": direction,
                "quintile_rates": quintile_rates,
                "suggested_expression": expression,
                "category": category,
                "is_derived": feat_name not in self._BASE_COLUMNS,
            })

        # AUC降順でソート
        candidates.sort(key=lambda x: float(x["auc"]), reverse=True)

        if progress_callback:
            progress_callback(2, 3, "交互作用分析中...")

        # === 2. 交互作用分析 ===
        interactions = self._analyze_interactions(
            features_list, labels, base_rate,
        )

        return {
            "n_samples": n_samples,
            "n_positive": n_pos,
            "base_rate": round(base_rate, 4),
            "candidates": candidates,
            "interactions": interactions,
        }

    def _quintile_analysis(
        self, scores: list[float], labels: list[int],
    ) -> list[dict[str, Any]]:
        """五分位別の的中率を計算する。"""
        if not scores:
            return []

        paired = sorted(zip(scores, labels, strict=False), key=lambda x: x[0])
        n = len(paired)
        quintiles = []
        for q in range(5):
            start = n * q // 5
            end = n * (q + 1) // 5
            subset = paired[start:end]
            if not subset:
                continue
            vals = [s for s, _ in subset]
            labs = [lb for _, lb in subset]
            rate = sum(labs) / len(labs) if labs else 0.0
            quintiles.append({
                "quintile": q + 1,
                "label": f"Q{q + 1}",
                "min": round(min(vals), 2),
                "max": round(max(vals), 2),
                "count": len(subset),
                "win_rate": round(rate, 4),
            })
        return quintiles

    def _describe_feature(
        self, name: str, direction: str, auc: float,
    ) -> str:
        """特徴量の説明文を生成する。"""
        jp_name = self._COL_NAMES.get(name, name)
        strength = "強い" if auc >= 0.60 else "中程度の" if auc >= 0.55 else "弱い"
        dir_text = "高いほど有利" if direction == "higher_is_better" else "低いほど有利"
        return f"{jp_name}: {strength}予測力 ({dir_text}, AUC={auc:.3f})"

    def _categorize_feature(self, name: str) -> str:
        """特徴量をカテゴリに分類する。"""
        if name in ("Ninki", "Odds", "is_favorite", "is_longshot", "odds_dm_gap", "odds_x_dm"):
            return "odds"
        if name in ("DMJyuni",):
            return "prediction"
        if name in ("BaTaijyu", "ZogenSa", "weight_diff", "weight_decrease", "weight_increase_large"):
            return "weight"
        if name in ("HaronTimeL3", "HaronTimeL4", "l3f_fast", "l3f_slow"):
            return "speed"
        if name in ("KyakusituKubun", "Jyuni4c", "is_front_runner", "is_closer", "corner4_relative"):
            return "pace"
        if name in ("Wakuban", "Umaban", "gate_position", "is_inner", "is_outer"):
            return "gate"
        if name in ("Barei", "is_young", "is_old", "SexCD", "is_female", "is_gelding"):
            return "profile"
        if name in ("Futan", "futan_per_weight"):
            return "weight"
        if name in ("Kyori", "is_turf", "is_dirt", "is_sprint", "is_mile", "is_middle", "is_long"):
            return "course"
        if name in ("num_entries",):
            return "race"
        if name in ("is_good_baba", "is_heavy_baba"):
            return "baba"
        if name in ("is_graded",):
            return "grade"
        if name in ("position_change", "is_makuri"):
            return "pace"
        return "derived"

    def _suggest_expression(
        self,
        name: str,
        direction: str,
        scores: list[float],
        labels: list[int],
        quintiles: list[dict[str, Any]],
    ) -> str:
        """ファクター式の候補を自動生成する。

        五分位分析の結果から、最も的中率が高い区間を特定し、
        条件式を生成する。
        """
        if not quintiles:
            return ""

        # 最も的中率が高い五分位を特定
        best_q = max(quintiles, key=lambda q: q["win_rate"])
        min(quintiles, key=lambda q: q["win_rate"])

        # 二値変数（0/1のみ）の場合
        unique_vals = set(scores)
        if unique_vals <= {0.0, 1.0}:
            cnt_1 = max(sum(1 for s in scores if s == 1), 1)
            cnt_0 = max(sum(1 for s in scores if s == 0), 1)
            rate_1 = sum(
                lb for s, lb in zip(scores, labels, strict=False) if s == 1
            ) / cnt_1
            rate_0 = sum(
                lb for s, lb in zip(scores, labels, strict=False) if s == 0
            ) / cnt_0
            if rate_1 > rate_0:
                return f"1 if {name} else 0"
            else:
                return f"1 if not {name} else 0"

        # 連続変数の場合
        # 上位/下位の五分位で有意な差がある場合に式を生成
        if direction == "lower_is_better":
            # Q1（最小値群）が最も良い → 閾値以下で+1
            threshold = best_q["max"]
            ctx_name = self._get_context_var_name(name)
            if ctx_name:
                return f"1 if {ctx_name} <= {threshold} else 0"
        else:
            # Q5（最大値群）が最も良い → 閾値以上で+1
            threshold = best_q["min"]
            ctx_name = self._get_context_var_name(name)
            if ctx_name:
                return f"1 if {ctx_name} >= {threshold} else 0"

        return ""

    def _get_context_var_name(self, col_name: str) -> str:
        """カラム名をevaluator.pyのコンテキスト変数名に変換する。"""
        # evaluator.py の build_eval_context で使用される変数名
        mapping = {
            "BaTaijyu": "weight",
            "ZogenSa": "weight_diff",
            "Futan": "Futan",
            "Ninki": "Ninki",
            "Odds": "odds",
            "DMJyuni": "dm_rank",
            "HaronTimeL3": "last_3f",
            "HaronTimeL4": "",  # 直接は未対応
            "Barei": "Barei",
            "KyakusituKubun": "running_style",
            "Jyuni4c": "corner4_pos",
            "Wakuban": "Wakuban",
            "Umaban": "Umaban",
            "gate_position": "gate_position",
            "is_inner": "is_inner_gate",
            "is_outer": "is_outer_gate",
            "is_favorite": "is_favorite",
            "is_longshot": "is_longshot",
            "is_front_runner": "is_front_runner",
            "is_closer": "is_closer",
            "is_turf": "is_turf",
            "is_dirt": "is_dirt",
            "is_sprint": "is_sprint",
            "is_mile": "is_mile",
            "is_middle": "is_middle",
            "is_long": "is_long",
            "is_female": "is_female",
            "is_gelding": "is_gelding",
            "weight_diff": "weight_diff",
            "num_entries": "num_entries",
            "corner4_relative": "corner4_pos / max(num_entries, 1)",
            "futan_per_weight": "Futan / max(weight, 1)",
            "odds_dm_gap": "abs(Ninki - dm_rank)",
            "odds_x_dm": "odds * dm_rank",
            # 馬場・グレード・位置変化
            "is_good_baba": "is_good_baba",
            "is_heavy_baba": "is_heavy_baba",
            "is_graded": "is_graded",
            "position_change": "position_change",
            "is_makuri": "position_change >= 5",
        }
        return mapping.get(col_name, col_name)

    def _analyze_interactions(
        self,
        features_list: list[dict[str, Any]],
        labels: list[int],
        base_rate: float,
    ) -> list[dict[str, Any]]:
        """交互作用（条件の組み合わせ）を分析する。

        二値変数同士の組み合わせで的中率が大きく変わるペアを発見する。
        """
        binary_features = [
            "is_favorite", "is_longshot", "is_inner", "is_outer",
            "is_front_runner", "is_closer", "is_turf", "is_dirt",
            "is_sprint", "is_mile", "is_middle", "is_long",
            "is_female", "is_gelding", "is_young", "is_old",
            "weight_decrease", "weight_increase_large",
            "l3f_fast", "l3f_slow",
            # 馬場・グレード・位置変化フラグ
            "is_good_baba", "is_heavy_baba", "is_graded", "is_makuri",
        ]
        # 存在する変数のみ
        available = [
            f for f in binary_features
            if any(feat.get(f, 0) != 0 for feat in features_list[:100])
        ]

        interactions: list[dict[str, Any]] = []
        for i, f1 in enumerate(available):
            for f2 in available[i + 1:]:
                # 両方1のサンプルを抽出
                both_on = [
                    labels[j] for j in range(len(features_list))
                    if features_list[j].get(f1, 0) == 1
                    and features_list[j].get(f2, 0) == 1
                ]
                if len(both_on) < 20:
                    continue

                rate = sum(both_on) / len(both_on)
                lift = rate / max(base_rate, 0.001)
                if lift < 1.3:
                    continue

                ctx1 = self._get_context_var_name(f1)
                ctx2 = self._get_context_var_name(f2)
                if not ctx1 or not ctx2:
                    continue

                interactions.append({
                    "feature_1": f1,
                    "feature_2": f2,
                    "n_samples": len(both_on),
                    "win_rate": round(rate, 4),
                    "lift": round(lift, 2),
                    "suggested_expression": f"1 if {ctx1} and {ctx2} else 0",
                    "description": (
                        f"{self._COL_NAMES.get(f1, f1)} + "
                        f"{self._COL_NAMES.get(f2, f2)}: "
                        f"的中率 {rate:.1%} (Lift {lift:.1f}x)"
                    ),
                })

        interactions.sort(key=lambda x: float(x["lift"]), reverse=True)
        return interactions[:20]  # 上位20件
