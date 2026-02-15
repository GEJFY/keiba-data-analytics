"""ファクター分析ページ。

特徴量重要度分析、Weight最適化、キャリブレーター学習を
推奨ワークフロー順にダッシュボードから実行する。
バックグラウンド実行対応: ページ遷移しても処理が継続する。
タブ化レイアウト: 6ステップをタブで整理し、縦スクロールを大幅削減。
"""

from typing import Any

import streamlit as st

from src.dashboard.components.task_status import show_task_progress
from src.dashboard.components.workflow_bar import mark_step_completed, render_workflow_bar
from src.dashboard.config_loader import PROJECT_ROOT
from src.dashboard.task_manager import TaskManager

# ==============================================================
# バックグラウンドタスク用ラッパー関数
# （別スレッドで実行されるため、DB接続を新規作成する）
# ==============================================================

def _resolve_db_paths() -> tuple[str, str]:
    """メインスレッドでDBパスを解決する（submit前に呼ぶこと）。"""
    config = st.session_state.config
    db_cfg = config.get("database", {})
    jvlink_path = str((PROJECT_ROOT / db_cfg.get("jvlink_db_path", "data/jvlink.db")).resolve())
    ext_path = str((PROJECT_ROOT / db_cfg.get("extension_db_path", "data/extension.db")).resolve())
    return jvlink_path, ext_path


def _create_db_managers(jvlink_db_path: str, ext_db_path: str) -> tuple:
    """スレッドセーフな新規DBマネージャを作成する（バックグラウンドスレッド用）。"""
    from src.data.db import DatabaseManager
    return DatabaseManager(jvlink_db_path), DatabaseManager(ext_db_path)


def _run_importance(
    jvlink_db_path: str, ext_db_path: str,
    date_from: str, date_to: str, max_races: int, target_jyuni: int,
    progress_callback: Any = None,
) -> dict:
    from src.scoring.feature_importance import FeatureImportanceAnalyzer
    jvlink_db, ext_db = _create_db_managers(jvlink_db_path, ext_db_path)
    analyzer = FeatureImportanceAnalyzer(jvlink_db, ext_db)
    return analyzer.analyze(
        date_from=date_from, date_to=date_to,
        max_races=max_races, target_jyuni=target_jyuni,
        progress_callback=progress_callback,
    )


def _run_optimize(
    jvlink_db_path: str, ext_db_path: str,
    date_from: str, date_to: str, max_races: int, target_jyuni: int,
    progress_callback: Any = None,
) -> dict:
    from src.scoring.weight_optimizer import WeightOptimizer
    jvlink_db, ext_db = _create_db_managers(jvlink_db_path, ext_db_path)
    optimizer = WeightOptimizer(jvlink_db, ext_db)
    return optimizer.optimize(
        date_from=date_from, date_to=date_to,
        max_races=max_races, target_jyuni=target_jyuni,
        progress_callback=progress_callback,
    )


def _run_calibrator(
    jvlink_db_path: str, ext_db_path: str,
    date_from: str, date_to: str, max_races: int, target_jyuni: int,
    cal_method: str,
    progress_callback: Any = None,
) -> dict:
    import numpy as np

    from src.scoring.calibration_trainer import CalibrationTrainer
    jvlink_db, ext_db = _create_db_managers(jvlink_db_path, ext_db_path)
    trainer = CalibrationTrainer(jvlink_db, ext_db)
    calibrator = trainer.train(
        method=cal_method, target_jyuni=target_jyuni, min_samples=50,
        use_batch=True, date_from=date_from, date_to=date_to,
        max_races=max_races, progress_callback=progress_callback,
    )
    scores, labels = trainer.build_training_data_from_batch(
        date_from, date_to, max_races, target_jyuni, min_samples=10,
    )
    probs = np.array([calibrator.predict_proba(s) for s in scores])
    brier = float(np.mean((probs - labels) ** 2))
    return {
        "calibrator": calibrator, "brier": brier,
        "samples": len(labels), "method": cal_method,
    }


def _run_correlation(
    jvlink_db_path: str, ext_db_path: str,
    date_from: str, date_to: str, max_races: int,
    progress_callback: Any = None,
) -> dict:
    from src.scoring.correlation_analyzer import CorrelationAnalyzer
    jvlink_db, ext_db = _create_db_managers(jvlink_db_path, ext_db_path)
    analyzer = CorrelationAnalyzer(jvlink_db, ext_db)
    return analyzer.analyze_correlations(
        date_from=date_from, date_to=date_to, max_races=max_races,
        progress_callback=progress_callback,
    )


def _run_sensitivity(
    jvlink_db_path: str, ext_db_path: str,
    date_from: str, date_to: str, max_races: int,
    progress_callback: Any = None,
) -> dict:
    from src.scoring.correlation_analyzer import CorrelationAnalyzer
    jvlink_db, ext_db = _create_db_managers(jvlink_db_path, ext_db_path)
    analyzer = CorrelationAnalyzer(jvlink_db, ext_db)
    return analyzer.sensitivity_analysis(
        date_from=date_from, date_to=date_to, max_races=max_races,
        progress_callback=progress_callback,
    )


def _run_discovery(
    jvlink_db_path: str, ext_db_path: str,
    date_from: str, date_to: str, max_races: int, target_jyuni: int,
    min_auc: float,
    progress_callback: Any = None,
) -> dict:
    from src.scoring.factor_discovery import FactorDiscovery
    jvlink_db, ext_db = _create_db_managers(jvlink_db_path, ext_db_path)
    fd = FactorDiscovery(jvlink_db, ext_db)
    return fd.discover(
        date_from=date_from, date_to=date_to,
        max_races=max_races, target_jyuni=target_jyuni,
        min_auc=min_auc, progress_callback=progress_callback,
    )


# ==============================================================
# 手法説明テキスト
# ==============================================================

HELP_STEP1 = """
#### 目的
各ファクター（予測変数）が実際にレース結果の予測にどの程度寄与しているかを定量的に評価します。
「なんとなく重要そう」ではなく、データに基づいて有効なファクターを選別するためのステップです。

#### アルゴリズム
1. **Permutation Importance (PI)**: ランダムフォレストモデルを学習後、
各ファクターの値をランダムにシャッフルして精度低下を測定します。
低下が大きいほど、そのファクターが重要であることを意味します。
2. **発火分析**: 各ファクターが「発火」（条件に該当）した場合と非発火の場合の的中率を比較します。

#### 指標の読み方
| 指標 | 意味 | 良い値の目安 | 判断基準 |
|------|------|-------------|---------|
| **PI (Permutation Importance)** | シャッフルによる精度低下幅。大きいほど重要 | > 0.01 | 0.01未満は寄与が小さい |
| **Lift** | 発火時の的中率 / 非発火時の的中率。1.0超で有効 | > 1.5 | 1.0未満は逆効果 |
| **発火率** | 全レースのうちファクターが発火した割合 | 10%〜50% | 5%未満は適用機会が少なすぎ、80%超は差別化できない |
| **相関** | 目的変数（着順）との相関係数 | |r| > 0.05 | 0に近いほど無関係 |

#### 結果を見た後のアクション
- **有効** (Lift > 1.5, PI > 0.01): そのまま利用
- **やや有効** (Lift > 1.0, PI > 0): Weightを下げて様子見
- **逆効果** (Lift < 1.0): 無効化を検討（ファクター管理ページで `DEPRECATED` に変更）
- **要検討**: 期間やサンプル数を変えて再検証
"""

HELP_STEP2 = """
#### 目的
各ファクターの「重み（Weight）」を、過去データから統計的に最適化します。
人手で調整するのではなく、機械学習で客観的な重みを算出します。

#### アルゴリズム
1. **LogisticRegression (L2正則化)** を使用し、各ファクターのスコアを特徴量、着順を目的変数として学習
2. 回帰係数（各ファクターの寄与度）を算出
3. 係数を 0.1〜3.0 のWeight範囲にスケーリング変換
4. L2正則化により過学習を防ぎ、極端な重み偏りを抑制

#### 指標の読み方
| 指標 | 意味 | 良い値の目安 |
|------|------|-------------|
| **Accuracy** | 分類精度。高いほどモデルの予測が正確 | > 0.55（単勝は難しいため0.6超で優秀） |
| **Log Loss** | 確率予測の誤差。低いほど確率分布の精度が高い | < 0.65 |
| **変化率** | 現在Weight → 最適Weightの変動幅 | ±30%以内が安定的 |

#### 結果を見た後のアクション
- **「DBに反映」ボタン**: 最適化結果をデータベースに保存（反映前に自動バックアップ作成）
- 反映後は必ず **Step 3（キャリブレーター）** を再学習してください（Weightが変わると勝率推定が変わるため）
- 大幅に変化したファクター（変化率 > ±50%）は個別に妥当性を確認してください

#### パラメータの影響
- **最大レース数**: 多いほど安定するが処理時間も増加。2000以上推奨
- **対象着順**: 1=単勝向き、2=連対向き、3=複勝向き。投票戦略に合わせて設定
"""

HELP_STEP3 = """
#### 目的
GY指数（ファクタースコアの合計値）を「勝率（確率）」に変換するモデルを学習します。
スコアが高い馬が実際にどの程度の確率で勝つのかを正確に推定するために必要です。

#### なぜキャリブレーションが必要か
GY指数はスコアであり、そのままでは確率ではありません。
例えば「GY指数80の馬」が実際に何%で勝つかを知るにはキャリブレーションが必要です。
正確な確率推定は、期待値（EV）計算 → バリュー投票の根幹を成します。

#### 校正方式の比較
| 方式 | アルゴリズム | 特徴 | 推奨条件 |
|------|------------|------|---------|
| **Platt** | シグモイド回帰でスコア→確率変換 | 少データでも安定 | 5000未満 |
| **Isotonic** | 非パラメトリック単調回帰 | 大データで高精度 | 5000以上 |

#### 指標の読み方
| 指標 | 意味 | 良い値の目安 |
|------|------|-------------|
| **Brier Score** | 確率予測の平均二乗誤差。0に近いほど良い | < 0.10（優秀）、0.15（良好） |
| **学習サンプル数** | キャリブレーションに使用したデータ量 | 1000以上推奨 |

#### 結果を見た後のアクション
- Brier Score が 0.10 未満 → 優秀な校正。そのまま保存
- 0.10〜0.15 → 良好。Platt ↔ Isotonic を試して比較
- 0.15超 → データ量不足 or Weightに問題。Step 1, 2 を見直し
"""

HELP_STEP4 = """
#### 目的
ファクター同士の相関を分析し、冗長（=ほぼ同じ情報を持つ）なファクターペアを検出します。
冗長なファクターを除去することで、過学習の防止とモデルの安定化に繋がります。

#### なぜ相関分析が必要か
例えば「前走タイム」と「前走上がり3F」は強い相関を持つことが多く、
両方をモデルに含めると同じ情報を二重カウントしてしまいます。
これは過学習の原因となり、未知のレースでの予測精度が低下します。

#### アルゴリズム
1. 全有効ファクターのスコアベクトルを算出
2. ファクター間のピアソン相関係数行列を計算
3. |r| > 0.7 のペアを「冗長」として検出

#### 指標の読み方
| 相関係数 |r| | 意味 | 対応 |
|-----------|------|------|
| > 0.7 | **強い相関**（冗長） | 片方を無効化 or Weight引き下げ |
| 0.3〜0.7 | 中程度の相関 | 注意して観察 |
| < 0.3 | 相関なし（独立） | 理想的 — 異なる情報を捉えている |

#### 結果を見た後のアクション
- 冗長ペアが見つかった場合: Step 1 の PI が低い方を `DEPRECATED` にすることを検討
- ヒートマップで全体像を俯瞰し、ファクター群のバランスを確認
"""

HELP_STEP5 = """
#### 目的
各ファクターのWeightを ±10%〜±50% 変動させた時に、
全体のスコアがどの程度変化するかを視覚化します。
Weightの安定性と影響度を確認するためのステップです。

#### なぜ感度分析が必要か
最適化で得たWeightが「ピンポイントで最適」な場合、
わずかなデータの変化でパフォーマンスが大きく変わる（=脆弱な）リスクがあります。
感度分析により、Weightの変動に対して安定した（ロバストな）モデルかどうかを確認します。

#### アルゴリズム
1. 各ファクターについて、Weightを -50%, -30%, -10%, +10%, +30%, +50% に変動
2. 変動後のスコアで全サンプルを再評価し、精度変化を測定
3. 結果をヒートマップとして表示

#### ヒートマップの読み方
| 色 | 意味 |
|-----|------|
| **濃い緑** | スコアが大きく改善 → このWeight方向に調整する価値あり |
| **薄い緑** | スコアの変化が小さい → Weight変動に鈍感（ロバスト）|
| **暗い色** | スコアが低下 → この方向へのWeight変更は避けるべき |

#### 結果を見た後のアクション
- 全方向で変化が小さいファクター → 安定して機能している（良好）
- 特定方向で大きく改善するファクター → Step 2 の最適化が不完全な可能性。再実行を検討
- Weight変動で極端に悪化するファクター → そのファクターへの依存度が高い。冗長性チェック（Step 4）と併せて確認
"""

HELP_STEP6 = """
#### 目的
既存ファクターにない新しい予測変数（特徴量）をデータから自動探索します。
人間の直感では見つけにくいパターンを発見し、ファクターの多様性を強化します。

#### アルゴリズム
1. **単変量スクリーニング**: レースデータの各カラムについて、目的変数（着順）との
   AUC（分離能力）を算出
2. **閾値フィルタリング**: AUC が指定値以上の候補を抽出
3. **五分位分析**: 候補変数を5等分し、各区間での的中率を算出。
   単調な増減傾向があるほど有望なファクター
4. **交互作用検出**: 2変数の組み合わせで、単独より高い予測力を持つペアを探索

#### 指標の読み方
| 指標 | 意味 | 良い値の目安 |
|------|------|-------------|
| **AUC** | 勝ち馬と負け馬の分離能力。0.5=ランダム | > 0.55 で有望、> 0.60 で優秀 |
| **Lift** | 上位区間 / 全体の的中率比 | > 1.5（1.5倍以上の向上）|
| **相関** | 目的変数との相関係数 | |r| > 0.05 |
| **五分位パターン** | Q1→Q5 の的中率推移 | 単調増加 or 単調減少が理想 |

#### 結果を見た後のアクション
- 有望な候補が見つかったら:
  1. 「提案式」をコピー
  2. ファクター管理ページ → 新規作成タブで登録
  3. ステータスを `DRAFT → TESTING` に遷移
  4. バックテストで効果を確認後 `APPROVED` に昇格

#### パラメータの影響
- **最低AUC**: 低くすると候補が増える（ノイズも増加）。初回は0.52、精査時は0.55以上推奨
"""


# ==============================================================
# ページ本体
# ==============================================================

def _render() -> None:
    st.header("ファクター分析")
    render_workflow_bar("factor")

    tm: TaskManager = st.session_state.task_manager

    jvlink_db = st.session_state.jvlink_db
    ext_db = st.session_state.ext_db
    jvlink_db_path, ext_db_path = _resolve_db_paths()

    # --- 共通パラメータ入力（タブ外に配置して全ステップで共有） ---
    from src.dashboard.components.date_defaults import factor_analysis_defaults
    default_from, default_to, default_max_races = factor_analysis_defaults()

    with st.expander("分析パラメータ（共通設定）", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            date_from = st.text_input(
                "開始日 (YYYYMMDD)", value=default_from, placeholder="20240101",
                help="分析対象の開始日。デフォルトは過去1年。",
            )
        with col2:
            date_to = st.text_input(
                "終了日 (YYYYMMDD)", value=default_to, placeholder="20241231",
                help="分析対象の終了日。デフォルトは今日。",
            )
        with col3:
            max_races = st.number_input(
                "最大レース数", value=default_max_races, min_value=100, step=500,
                help="多いほど精度が上がりますが処理時間も増えます。初回は1000〜2000がお勧め。",
            )
        with col4:
            target_jyuni = st.selectbox(
                "対象着順", [1, 2, 3], index=0,
                help="1=単勝、2=連対、3=複勝。バリュー投資では1が基本。",
            )

    # --- タブステップナビゲーション ---
    step_labels = [
        "Step 1: 重要度分析",
        "Step 2: Weight最適化",
        "Step 3: キャリブレーター",
        "Step 4: 相関分析",
        "Step 5: 感度分析",
        "Step 6: ファクター発見",
    ]
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(step_labels)

    # ================================================================
    # Step 1: 特徴量重要度分析
    # ================================================================
    with tab1:
        st.subheader("特徴量重要度分析")
        st.caption("各ファクターの有効性を診断 — どのファクターが本当に役立っているかを定量評価します")

        with st.expander("この分析について詳しく見る", expanded=False):
            st.markdown(HELP_STEP1)

        show_task_progress("task_importance", "importance_result", tm)

        has_running = "task_importance" in st.session_state or tm.has_running("重要度分析")
        if st.button(
            "実行中..." if has_running else "重要度分析を実行",
            key="btn_importance", type="primary", disabled=has_running,
        ):
            task_id = tm.submit(
                name="重要度分析",
                target=_run_importance,
                kwargs={
                    "jvlink_db_path": jvlink_db_path, "ext_db_path": ext_db_path,
                    "date_from": date_from, "date_to": date_to,
                    "max_races": max_races, "target_jyuni": target_jyuni,
                },
            )
            st.session_state.task_importance = task_id
            st.rerun()

        if "importance_result" in st.session_state:
            imp_result = st.session_state.importance_result
            c1, c2, c3 = st.columns(3)
            c1.metric("サンプル数", f"{imp_result['n_samples']:,}")
            c2.metric("ベースライン精度", f"{imp_result['baseline_accuracy']:.4f}")
            effective = sum(
                1 for f in imp_result["factors"]
                if f["lift"] > 1.0 and f["permutation_importance"] > 0
            )
            c3.metric("有効ファクター数", f"{effective} / {len(imp_result['factors'])}")

            from src.dashboard.components.charts import importance_chart
            fig = importance_chart(
                [f["rule_name"] for f in imp_result["factors"]],
                [f["permutation_importance"] for f in imp_result["factors"]],
            )
            st.plotly_chart(fig, use_container_width=True)

            import pandas as pd
            rows = []
            for f in imp_result["factors"]:
                if f["lift"] > 1.5 and f["permutation_importance"] > 0.01:
                    status = "\u2705 有効"
                elif f["lift"] > 1.0 and f["permutation_importance"] > 0:
                    status = "\U0001f7e1 やや有効"
                elif f["lift"] < 1.0:
                    status = "\u274c 逆効果"
                else:
                    status = "\u2753 要検討"
                rows.append({
                    "判定": status,
                    "ファクター": f["rule_name"],
                    "カテゴリ": f["category"],
                    "PI": round(f["permutation_importance"], 4),
                    "Lift": round(f["lift"], 2),
                    "発火時的中率": f"{f['hit_rate_with']:.1%}",
                    "非発火時": f"{f['hit_rate_without']:.1%}",
                    "発火率": f"{f['activation_rate']:.1%}",
                    "相関": round(f["correlation"], 3),
                    "Weight": f["current_weight"],
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            if st.button("結果をクリア", key="btn_clear_importance"):
                del st.session_state["importance_result"]
                st.rerun()

            mark_step_completed("factor")
            st.success("Step 1 完了 — 「Step 2: Weight最適化」タブに進んでください")

    # ================================================================
    # Step 2: Weight最適化
    # ================================================================
    with tab2:
        st.subheader("Weight最適化")
        st.caption("LogisticRegressionで過去データから最適Weightを算出します")

        with st.expander("この分析について詳しく見る", expanded=False):
            st.markdown(HELP_STEP2)

        show_task_progress("task_optimize", "optimize_result", tm)

        has_running = "task_optimize" in st.session_state or tm.has_running("Weight最適化")
        if st.button(
            "実行中..." if has_running else "Weight最適化を実行",
            key="btn_optimize", type="primary", disabled=has_running,
        ):
            task_id = tm.submit(
                name="Weight最適化",
                target=_run_optimize,
                kwargs={
                    "jvlink_db_path": jvlink_db_path, "ext_db_path": ext_db_path,
                    "date_from": date_from, "date_to": date_to,
                    "max_races": max_races, "target_jyuni": target_jyuni,
                },
            )
            st.session_state.task_optimize = task_id
            st.rerun()

        if "optimize_result" in st.session_state:
            result = st.session_state.optimize_result
            c1, c2, c3 = st.columns(3)
            c1.metric("サンプル数", f"{result['n_samples']:,}")
            c2.metric("Accuracy", f"{result['accuracy']:.4f}")
            c3.metric("Log Loss", f"{result['log_loss']:.4f}")

            import pandas as pd

            from src.dashboard.components.charts import weight_comparison_chart
            rows = []
            for name in result["weights"]:
                current = result["current_weights"].get(name, 1.0)
                optimized = result["weights"][name]
                diff = optimized - current
                rows.append({
                    "ファクター": name,
                    "現在Weight": current,
                    "最適Weight": optimized,
                    "変化": round(diff, 2),
                    "変化率": f"{diff / max(current, 0.01):+.0%}",
                })
            df = pd.DataFrame(rows)

            fig = weight_comparison_chart(
                df["ファクター"].tolist(),
                df["現在Weight"].tolist(),
                df["最適Weight"].tolist(),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)

            st.warning(
                "「DBに反映」を押すと現在のWeightが上書きされます。"
                "反映後はStep 3のキャリブレーター再学習が必要です。",
                icon="\u26a0\ufe0f",
            )
            if st.button("最適化結果をDBに反映", key="btn_apply_weights"):
                try:
                    from src.factors.registry import FactorRegistry
                    _registry = FactorRegistry(ext_db)
                    _registry.create_snapshot(
                        "pre-weight-apply",
                        description="Weight最適化適用前の自動バックアップ",
                        trigger="optimization",
                    )

                    from src.scoring.weight_optimizer import WeightOptimizer
                    optimizer = WeightOptimizer(jvlink_db, ext_db)
                    updated = optimizer.apply_weights(
                        result["weights"],
                        training_from=result.get("training_from", ""),
                        training_to=result.get("training_to", ""),
                    )
                    mark_step_completed("optimize")
                    st.success(f"{updated}ルールのWeightを更新しました — Step 3 で再学習してください")
                except Exception as e:
                    st.error(f"Weight適用エラー: {e}")

            if st.button("結果をクリア", key="btn_clear_optimize"):
                del st.session_state["optimize_result"]
                st.rerun()

    # ================================================================
    # Step 3: キャリブレーター学習
    # ================================================================
    with tab3:
        st.subheader("キャリブレーター学習")
        st.caption("GY指数を正確な勝率に変換するモデルを学習します")

        with st.expander("この分析について詳しく見る", expanded=False):
            st.markdown(HELP_STEP3)

        cal_method = st.selectbox("校正方式", ["platt", "isotonic"], index=0)

        show_task_progress("task_calibrator", "calibrator_result", tm)

        has_running = "task_calibrator" in st.session_state or tm.has_running("キャリブレーター学習")
        if st.button(
            "実行中..." if has_running else "キャリブレーターを学習",
            key="btn_calibrator", type="primary", disabled=has_running,
        ):
            task_id = tm.submit(
                name="キャリブレーター学習",
                target=_run_calibrator,
                kwargs={
                    "jvlink_db_path": jvlink_db_path, "ext_db_path": ext_db_path,
                    "date_from": date_from, "date_to": date_to,
                    "max_races": max_races, "target_jyuni": target_jyuni,
                    "cal_method": cal_method,
                },
            )
            st.session_state.task_calibrator = task_id
            st.rerun()

        if "calibrator_result" in st.session_state:
            cal_result = st.session_state.calibrator_result
            st.session_state.calibrator = cal_result["calibrator"]
            st.session_state.calibrator_brier = cal_result["brier"]
            st.session_state.calibrator_samples = cal_result["samples"]
            st.session_state.calibrator_method = cal_result["method"]

            c1, c2, c3 = st.columns(3)
            c1.metric("Brier Score", f"{cal_result['brier']:.4f}")
            c2.metric("学習サンプル数", f"{cal_result['samples']:,}")
            c3.metric("校正方式", cal_result["method"])

            save_path = PROJECT_ROOT / "data" / "calibrator.joblib"
            if st.button("キャリブレーターを保存", key="btn_save_cal"):
                try:
                    cal_result["calibrator"].save(save_path)
                    st.success(f"保存完了: {save_path.name}")
                except Exception as e:
                    st.error(f"保存エラー: {e}")

            if save_path.exists():
                st.caption(f"保存済みファイル: `{save_path.name}`")

            if st.button("結果をクリア", key="btn_clear_calibrator"):
                del st.session_state["calibrator_result"]
                st.rerun()

            st.success("Step 3 完了 — バックテストタブでROIを確認してください")

    # ================================================================
    # Step 4: ファクター相関分析
    # ================================================================
    with tab4:
        st.subheader("ファクター相関分析")
        st.caption("冗長なファクターを検出して精度向上・過学習防止に活用")

        with st.expander("この分析について詳しく見る", expanded=False):
            st.markdown(HELP_STEP4)

        show_task_progress("task_correlation", "corr_result", tm)

        has_running = "task_correlation" in st.session_state or tm.has_running("相関分析")
        if st.button(
            "実行中..." if has_running else "相関分析を実行",
            key="btn_correlation", type="primary", disabled=has_running,
        ):
            task_id = tm.submit(
                name="相関分析",
                target=_run_correlation,
                kwargs={
                    "jvlink_db_path": jvlink_db_path, "ext_db_path": ext_db_path,
                    "date_from": date_from, "date_to": date_to, "max_races": max_races,
                },
            )
            st.session_state.task_correlation = task_id
            st.rerun()

        if "corr_result" in st.session_state:
            corr_result = st.session_state.corr_result
            st.metric("分析サンプル数", f"{corr_result['n_samples']:,}")

            if corr_result["redundant_pairs"]:
                st.warning(f"{len(corr_result['redundant_pairs'])}組の冗長ペアを検出しました。")
                import pandas as pd
                df_pairs = pd.DataFrame(corr_result["redundant_pairs"])
                df_pairs.columns = ["ファクターA", "ファクターB", "相関係数"]
                st.dataframe(df_pairs, use_container_width=True, hide_index=True)
            else:
                st.success("冗長なファクターペアなし（|r| > 0.7 なし）")

            import plotly.graph_objects as go

            from src.dashboard.components.theme import (
                ACCENT_BLUE,
                ACCENT_RED,
                BG_PRIMARY,
                BG_SECONDARY,
                BORDER,
                TEXT_PRIMARY,
            )
            names = corr_result["factor_names"]
            fig_corr = go.Figure(data=go.Heatmap(
                z=corr_result["correlation_matrix"], x=names, y=names,
                colorscale=[[0, ACCENT_RED], [0.5, BG_SECONDARY], [1, ACCENT_BLUE]],
                zmid=0, zmin=-1, zmax=1,
                texttemplate="%{z:.2f}",
                textfont=dict(size=9, color=TEXT_PRIMARY),
            ))
            fig_corr.update_layout(
                paper_bgcolor=BG_PRIMARY, plot_bgcolor=BG_SECONDARY,
                font=dict(color=TEXT_PRIMARY, family="JetBrains Mono, Consolas, monospace"),
                title="ファクター相関行列",
                xaxis=dict(side="bottom", tickangle=-45, gridcolor=BORDER),
                yaxis=dict(autorange="reversed", gridcolor=BORDER),
                height=max(500, len(names) * 30 + 200),
                margin=dict(l=200, r=20, t=40, b=200),
            )
            st.plotly_chart(fig_corr, use_container_width=True)

            if st.button("結果をクリア", key="btn_clear_corr"):
                del st.session_state["corr_result"]
                st.rerun()

    # ================================================================
    # Step 5: Weight感度分析
    # ================================================================
    with tab5:
        st.subheader("Weight感度分析")
        st.caption("Weightを変動させた時のスコアへの影響を視覚化")

        with st.expander("この分析について詳しく見る", expanded=False):
            st.markdown(HELP_STEP5)

        show_task_progress("task_sensitivity", "sens_result", tm)

        has_running = "task_sensitivity" in st.session_state or tm.has_running("感度分析")
        if st.button(
            "実行中..." if has_running else "感度分析を実行",
            key="btn_sensitivity", type="primary", disabled=has_running,
        ):
            task_id = tm.submit(
                name="感度分析",
                target=_run_sensitivity,
                kwargs={
                    "jvlink_db_path": jvlink_db_path, "ext_db_path": ext_db_path,
                    "date_from": date_from, "date_to": date_to, "max_races": max_races,
                },
            )
            st.session_state.task_sensitivity = task_id
            st.rerun()

        if "sens_result" in st.session_state:
            sens_result = st.session_state.sens_result
            st.metric("分析サンプル数", f"{sens_result['n_samples']:,}")

            import plotly.graph_objects as go

            from src.dashboard.components.theme import (
                ACCENT_GREEN,
                BG_PRIMARY,
                BG_SECONDARY,
                BORDER,
                TEXT_PRIMARY,
            )
            names = sens_result["factor_names"]
            deltas = sens_result["deltas"]
            delta_labels = [f"{d:+.0%}" for d in deltas]
            fig_sens = go.Figure(data=go.Heatmap(
                z=sens_result["sensitivity_matrix"], x=delta_labels, y=names,
                colorscale=[[0, BG_SECONDARY], [1, ACCENT_GREEN]],
                texttemplate="%{z:.2f}",
                textfont=dict(size=10, color=TEXT_PRIMARY),
            ))
            fig_sens.update_layout(
                paper_bgcolor=BG_PRIMARY, plot_bgcolor=BG_SECONDARY,
                font=dict(color=TEXT_PRIMARY, family="JetBrains Mono, Consolas, monospace"),
                title="Weight変動に対するスコア感度",
                xaxis=dict(title="Weight変動幅", gridcolor=BORDER),
                yaxis=dict(autorange="reversed", gridcolor=BORDER),
                height=max(400, len(names) * 25 + 100),
                margin=dict(l=200, r=20, t=40, b=60),
            )
            st.plotly_chart(fig_sens, use_container_width=True)

            if st.button("結果をクリア", key="btn_clear_sens"):
                del st.session_state["sens_result"]
                st.rerun()

    # ================================================================
    # Step 6: データドリブンファクター発見
    # ================================================================
    with tab6:
        st.subheader("データドリブンファクター発見")
        st.caption("データから予測に有効な変数・条件を自動発見")

        with st.expander("この分析について詳しく見る", expanded=False):
            st.markdown(HELP_STEP6)

        disc_min_auc = st.number_input(
            "最低AUC", value=0.52, step=0.01, format="%.2f", key="disc_min_auc",
        )

        show_task_progress("task_discovery", "disc_result", tm)

        has_running = "task_discovery" in st.session_state or tm.has_running("ファクター発見")
        if st.button(
            "実行中..." if has_running else "ファクター発見を実行",
            key="btn_discovery", type="primary", disabled=has_running,
        ):
            task_id = tm.submit(
                name="ファクター発見",
                target=_run_discovery,
                kwargs={
                    "jvlink_db_path": jvlink_db_path, "ext_db_path": ext_db_path,
                    "date_from": date_from, "date_to": date_to,
                    "max_races": max_races, "target_jyuni": target_jyuni,
                    "min_auc": disc_min_auc,
                },
            )
            st.session_state.task_discovery = task_id
            st.rerun()

        if "disc_result" in st.session_state:
            disc_result = st.session_state.disc_result
            dc1, dc2, dc3 = st.columns(3)
            dc1.metric("分析サンプル数", f"{disc_result['n_samples']:,}")
            dc2.metric("正例数", f"{disc_result['n_positive']:,}")
            dc3.metric("基準的中率", f"{disc_result['base_rate']:.1%}")

            candidates = disc_result["candidates"]
            if candidates:
                st.success(f"{len(candidates)}件の候補ファクターを発見しました。")
                import pandas as pd
                rows = []
                for c in candidates:
                    rows.append({
                        "名前": c["display_name"],
                        "カテゴリ": c["category"],
                        "AUC": f"{c['auc']:.4f}",
                        "相関": f"{c['correlation']:+.4f}",
                        "方向": "高い方が有利" if c["direction"] == "higher_is_better" else "低い方が有利",
                        "派生変数": "Yes" if c["is_derived"] else "",
                        "提案式": c["suggested_expression"],
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                st.subheader("上位候補の五分位分析")
                from src.dashboard.components.charts import bar_chart
                for c in candidates[:5]:
                    if not c["quintile_rates"]:
                        continue
                    with st.expander(f"{c['display_name']} (AUC={c['auc']:.3f})"):
                        st.caption(c["description"])
                        q_labels = [q["label"] for q in c["quintile_rates"]]
                        q_rates = [q["win_rate"] * 100 for q in c["quintile_rates"]]
                        fig_q = bar_chart(q_labels, q_rates, f"{c['display_name']} 五分位別的中率(%)")
                        fig_q.update_layout(
                            yaxis_title="的中率 (%)",
                            height=300,
                        )
                        st.plotly_chart(fig_q, use_container_width=True)
                        q_rows = []
                        for q in c["quintile_rates"]:
                            q_rows.append({
                                "区間": q["label"],
                                "範囲": f"{q['min']:.1f} ~ {q['max']:.1f}",
                                "件数": q["count"],
                                "的中率": f"{q['win_rate']:.1%}",
                            })
                        st.dataframe(
                            pd.DataFrame(q_rows), use_container_width=True, hide_index=True,
                        )
                        if c["suggested_expression"]:
                            st.code(c["suggested_expression"], language="python")
            else:
                st.info("有意な候補は見つかりませんでした。AUC閾値を下げてみてください。")

            interactions = disc_result.get("interactions", [])
            if interactions:
                st.subheader("交互作用（条件の組み合わせ）")
                import pandas as pd
                int_rows = []
                for ia in interactions:
                    int_rows.append({
                        "条件1": ia["feature_1"],
                        "条件2": ia["feature_2"],
                        "件数": ia["n_samples"],
                        "的中率": f"{ia['win_rate']:.1%}",
                        "Lift": f"{ia['lift']:.1f}x",
                        "提案式": ia["suggested_expression"],
                    })
                st.dataframe(
                    pd.DataFrame(int_rows), use_container_width=True, hide_index=True,
                )

            if st.button("結果をクリア", key="btn_clear_disc"):
                del st.session_state["disc_result"]
                st.rerun()


_render()
