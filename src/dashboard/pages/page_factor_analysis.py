"""ファクター分析ページ。

特徴量重要度分析、Weight最適化、キャリブレーター学習を
推奨ワークフロー順にダッシュボードから実行する。
バックグラウンド実行対応: ページ遷移しても処理が継続する。
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
# 進捗表示ヘルパー
# ==============================================================

# ==============================================================
# ページ本体
# ==============================================================

def _render() -> None:
    st.header("ファクター分析")
    render_workflow_bar("factor")

    tm: TaskManager = st.session_state.task_manager

    # --- ワークフローガイド ---
    st.info(
        "**推奨ワークフロー:** "
        "Step 1 重要度分析 \u2192 "
        "Step 2 Weight最適化 \u2192 "
        "Step 3 キャリブレーター学習  \n"
        "\u23f3 各ステップはバックグラウンドで実行され、他のタブに移動しても処理が継続します",
        icon="\U0001f4a1",
    )

    jvlink_db = st.session_state.jvlink_db
    ext_db = st.session_state.ext_db
    jvlink_db_path, ext_db_path = _resolve_db_paths()

    # --- 共通パラメータ入力（デフォルト値: 過去1年/2000レース） ---
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
                help="多いほど精度が上がりますが処理時間も増えます。初回は1000\u301c2000がお勧め。",
            )
        with col4:
            target_jyuni = st.selectbox(
                "対象着順", [1, 2, 3], index=0,
                help="1=単勝、2=連対、3=複勝。バリュー投資では1が基本。",
            )

    # ================================================================
    # Step 1: 特徴量重要度分析
    # ================================================================
    st.divider()
    st.subheader("Step 1: 特徴量重要度分析")
    st.caption("各ファクターの有効性を診断 \u2014 どのファクターが本当に役立っているかを定量評価します")

    with st.expander("特徴量重要度分析とは？", expanded=False):
        st.markdown("""
**指標の読み方**:
| 指標 | 意味 | 良い値 |
|------|------|--------|
| **PI** | Permutation Importance。大きいほど重要 | > 0.01 |
| **Lift** | 発火時/非発火時の的中率比。1.0以上で有効 | > 1.5 |
| **発火率** | ファクター発火の割合 | 10%\u301c50% |
""")

    # バックグラウンドタスク進捗チェック
    show_task_progress("task_importance", "importance_result", tm)

    # 実行ボタン
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
        mark_step_completed("factor")
        st.success("Step 1 完了 \u2014 Step 2 に進んでください")

    # ================================================================
    # Step 2: Weight最適化
    # ================================================================
    st.divider()
    st.subheader("Step 2: Weight最適化")
    st.caption("LogisticRegressionで過去データから最適Weightを算出します")

    with st.expander("Weight最適化とは？", expanded=False):
        st.markdown("""
- 各ファクターの「重み」を過去データから自動最適化
- LogisticRegression (L2正則化) で回帰係数を算出し、0.1\u301c3.0 のWeightに変換
- Accuracy と Log Loss で予測精度を評価
""")

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
                # 適用前に自動スナップショット作成
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
                st.success(f"{updated}ルールのWeightを更新しました \u2014 Step 3 で再学習してください")
            except Exception as e:
                st.error(f"Weight適用エラー: {e}")

    # ================================================================
    # Step 3: キャリブレーター学習
    # ================================================================
    st.divider()
    st.subheader("Step 3: キャリブレーター学習")
    st.caption("GY指数を正確な勝率に変換するモデルを学習します")

    with st.expander("キャリブレーターとは？", expanded=False):
        st.markdown("""
- **Platt (シグモイド)**: データが少なくても安定。推奨。
- **Isotonic (単調回帰)**: データが多い場合（5000+）に高精度。
- **Brier Score**: 0に近いほど良い校正。0.1以下なら優秀。
""")

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
        # 旧session stateキーとの互換
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
        st.success("Step 3 完了 \u2014 バックテストタブでROIを確認してください")

    # ================================================================
    # Step 4: ファクター相関分析
    # ================================================================
    st.divider()
    st.subheader("Step 4: ファクター相関分析")
    st.caption("冗長なファクターを検出して精度向上・過学習防止に活用")

    with st.expander("相関分析とは？", expanded=False):
        st.markdown("""
| 値 | 意味 |
|---|---|
| |r| > 0.7 | 強い相関（冗長 \u2014 片方を検討） |
| |r| < 0.3 | 相関なし（独立 = 理想的） |
""")

    show_task_progress("task_correlation", "corr_result", tm)

    has_running = "task_correlation" in st.session_state or tm.has_running("相関分析")
    if st.button(
        "実行中..." if has_running else "相関分析を実行",
        key="btn_correlation", type="secondary", disabled=has_running,
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

    # ================================================================
    # Step 5: Weight感度分析
    # ================================================================
    st.divider()
    st.subheader("Step 5: Weight感度分析")
    st.caption("Weightを変動させた時のスコアへの影響を視覚化")

    show_task_progress("task_sensitivity", "sens_result", tm)

    has_running = "task_sensitivity" in st.session_state or tm.has_running("感度分析")
    if st.button(
        "実行中..." if has_running else "感度分析を実行",
        key="btn_sensitivity", type="secondary", disabled=has_running,
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

    # ================================================================
    # Step 6: データドリブンファクター発見
    # ================================================================
    st.divider()
    st.subheader("Step 6: データドリブンファクター発見")
    st.caption("データから予測に有効な変数・条件を自動発見")

    with st.expander("データドリブン発見とは？", expanded=False):
        st.markdown("""
| 指標 | 意味 | 良い値 |
|------|------|--------|
| **AUC** | 分離能力。0.5=ランダム | > 0.55 |
| **Lift** | 的中率向上倍率 | > 1.5 |
""")

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

            # 五分位分析 上位5件
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
                    # 数値テーブルも表示
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


_render()
