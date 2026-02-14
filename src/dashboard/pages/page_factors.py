"""Tab 2: ファクター管理ページ。

ファクタールールのCRUD、ステータス遷移、重み変更、初期値リセット、変更履歴を提供する。
"""

from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from src.dashboard.components.factor_badges import source_emoji
from src.dashboard.components.reset_defaults import render_reset_controls
from src.dashboard.components.workflow_bar import render_workflow_bar
from src.data.db import DatabaseManager
from src.factors.lifecycle import FactorLifecycleManager
from src.factors.registry import FactorRegistry

CATEGORIES = [
    "form", "course", "jockey", "speed", "track",
    "weight", "blood", "gate", "gender", "matchup", "other",
]

VALID_TRANSITIONS = {
    "DRAFT": ["TESTING"],
    "TESTING": ["APPROVED", "DRAFT"],
    "APPROVED": ["DEPRECATED"],
    "DEPRECATED": [],
}

STATUS_COLORS = {
    "DRAFT": "\U0001f7e1",
    "TESTING": "\U0001f535",
    "APPROVED": "\U0001f7e2",
    "DEPRECATED": "\u26ab",
}

SOURCE_OPTIONS = ["gy_initial", "discovery", "manual", "ai_generated", "research"]


def _load_all_rules(ext_db: DatabaseManager) -> pd.DataFrame:
    """全ファクタールールを取得する。"""
    if not ext_db.table_exists("factor_rules"):
        return pd.DataFrame()
    rows = ext_db.execute_query(
        "SELECT rule_id, rule_name, category, weight, validation_score, "
        "is_active, review_status, decay_rate, description, source, updated_at "
        "FROM factor_rules ORDER BY rule_id"
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _load_change_log(ext_db: DatabaseManager, rule_id: int | None = None) -> pd.DataFrame:
    """変更履歴を取得する。"""
    if not ext_db.table_exists("factor_review_log"):
        return pd.DataFrame()
    if rule_id:
        rows = ext_db.execute_query(
            "SELECT log_id, rule_id, action, old_weight, new_weight, reason, "
            "backtest_roi, changed_at, changed_by "
            "FROM factor_review_log WHERE rule_id = ? ORDER BY changed_at DESC",
            (rule_id,),
        )
    else:
        rows = ext_db.execute_query(
            "SELECT log_id, rule_id, action, old_weight, new_weight, reason, "
            "backtest_roi, changed_at, changed_by "
            "FROM factor_review_log ORDER BY changed_at DESC LIMIT 50"
        )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ==============================
# ページ本体
# ==============================
st.header("ファクター管理")
render_workflow_bar("optimize")

ext_db: DatabaseManager = st.session_state.ext_db
registry = FactorRegistry(ext_db)
lifecycle = FactorLifecycleManager(ext_db)

# --- フィルター ---
col_filter, col_source, col_action = st.columns([2, 2, 1])
with col_filter:
    status_filter = st.multiselect(
        "ステータス",
        ["DRAFT", "TESTING", "APPROVED", "DEPRECATED"],
        default=["DRAFT", "TESTING", "APPROVED"],
    )
with col_source:
    source_filter = st.multiselect(
        "ソース",
        SOURCE_OPTIONS,
        default=SOURCE_OPTIONS,
        format_func=lambda s: f"{source_emoji(s)} {s}",
    )
with col_action:
    st.write("")
    st.write("")
    if st.button("劣化ルール検出"):
        decayed = lifecycle.detect_decayed_rules()
        if decayed:
            st.warning(f"{len(decayed)} 件の劣化ルールを検出")
            for r in decayed:
                st.text(f"  - [{r['rule_id']}] {r['rule_name']} (decay: {r.get('decay_rate', 0):.1%})")
        else:
            st.success("劣化ルールなし")

# --- ルール一覧 ---
st.subheader("ルール一覧")
df_rules = _load_all_rules(ext_db)
if df_rules.empty:
    st.info("ファクタールールがありません。下の「新規ルール作成」から追加してください。")
else:
    df_display = df_rules.copy()

    # ステータスフィルター
    if status_filter:
        df_display = df_display[df_display["review_status"].isin(status_filter)]

    # ソースフィルター
    if source_filter and "source" in df_display.columns:
        df_display = df_display[df_display["source"].isin(source_filter)]

    # ステータスアイコン付き
    df_display = df_display.copy()
    df_display["status"] = df_display["review_status"].map(
        lambda s: f"{STATUS_COLORS.get(s, '')} {s}"
    )

    # ソースバッジ
    if "source" in df_display.columns:
        df_display["source_label"] = df_display["source"].map(
            lambda s: f"{source_emoji(s)} {s}" if pd.notna(s) else ""
        )
    else:
        df_display["source_label"] = ""

    st.dataframe(
        df_display[["rule_id", "rule_name", "category", "weight",
                     "source_label", "validation_score", "status",
                     "decay_rate", "updated_at"]],
        column_config={
            "source_label": st.column_config.TextColumn("ソース"),
        },
        use_container_width=True,
        hide_index=True,
        height=300,
    )

    # ステータス・Weight分布グラフ
    if len(df_display) > 0:
        from src.dashboard.components.charts import histogram_chart, pie_chart
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            status_counts = df_display["review_status"].value_counts()
            fig_status = pie_chart(
                status_counts.index.tolist(),
                status_counts.values.tolist(),
                "ステータス分布",
            )
            st.plotly_chart(fig_status, use_container_width=True)
        with col_chart2:
            weights = df_display["weight"].dropna().tolist()
            if weights:
                fig_weight = histogram_chart(weights, "Weight分布", nbins=15)
                st.plotly_chart(fig_weight, use_container_width=True)

# --- ルール操作 ---
st.divider()
tab_create, tab_transition, tab_weight, tab_reset, tab_history, tab_version = st.tabs(
    ["新規作成", "ステータス遷移", "重み変更", "初期値リセット", "変更履歴", "バージョン管理"]
)

# --- 新規ルール作成 ---
with tab_create:
    with st.form("create_rule_form"):
        st.subheader("新規ファクタールール作成")
        rule_name = st.text_input("ルール名", placeholder="例: 前走着順加点")
        category = st.selectbox("カテゴリー", CATEGORIES)
        description = st.text_area("説明", placeholder="ルールの概要を記述")
        sql_expression = st.text_area(
            "SQL式 / Python式",
            placeholder="例: CASE WHEN KakuteiJyuni <= 3 THEN -1 ELSE 1 END",
            height=100,
        )
        weight = st.slider("重み", min_value=0.1, max_value=5.0, value=1.0, step=0.1)
        source = st.selectbox("ソース", ["manual", "ai_generated", "research"])

        submitted = st.form_submit_button("作成 (DRAFT)")
        if submitted and rule_name:
            now = datetime.now(timezone.utc).isoformat()
            rule_data = {
                "rule_name": rule_name,
                "category": category,
                "description": description,
                "sql_expression": sql_expression,
                "weight": weight,
                "source": source,
                "created_at": now,
                "updated_at": now,
            }
            new_id = registry.create_rule(rule_data)
            st.success(f"ルール作成完了: rule_id = {new_id}")
            st.rerun()
        elif submitted:
            st.error("ルール名を入力してください")

# --- ステータス遷移 ---
with tab_transition:
    st.subheader("ステータス遷移")
    if df_rules.empty:
        st.info("ルールがありません")
    else:
        rule_options = {
            f"[{r['rule_id']}] {r['rule_name']} ({r['review_status']})": r["rule_id"]
            for _, r in df_rules.iterrows()
        }
        selected_label = st.selectbox("対象ルール", list(rule_options.keys()))
        selected_id = rule_options[selected_label]
        current_status = df_rules[df_rules["rule_id"] == selected_id]["review_status"].iloc[0]
        possible = VALID_TRANSITIONS.get(current_status, [])

        if possible:
            new_status = st.selectbox("遷移先", possible)
            reason = st.text_input("遷移理由", placeholder="例: バックテスト合格")
            if st.button("遷移実行"):
                registry.transition_status(selected_id, new_status, reason)
                st.success(f"ステータス変更: {current_status} \u2192 {new_status}")
                st.rerun()
        else:
            st.info(f"現在のステータス「{current_status}」から遷移可能な先はありません。")

# --- 重み変更 ---
with tab_weight:
    st.subheader("重み変更")
    if df_rules.empty:
        st.info("ルールがありません")
    else:
        rule_options_w = {
            f"[{r['rule_id']}] {r['rule_name']} (現在: {r['weight']})": r["rule_id"]
            for _, r in df_rules.iterrows()
        }
        selected_label_w = st.selectbox("対象ルール", list(rule_options_w.keys()), key="weight_select")
        selected_id_w = rule_options_w[selected_label_w]
        current_weight = float(df_rules[df_rules["rule_id"] == selected_id_w]["weight"].iloc[0])

        new_weight = st.slider(
            "新しい重み", min_value=0.1, max_value=5.0,
            value=current_weight, step=0.1, key="new_weight_slider",
        )
        reason_w = st.text_input("変更理由", placeholder="例: 回収率改善に寄与", key="weight_reason")
        if st.button("重み更新"):
            registry.update_weight(selected_id_w, new_weight, reason_w)
            st.success(f"重み変更: {current_weight} \u2192 {new_weight}")
            st.rerun()

# --- 初期値リセット ---
with tab_reset:
    st.subheader("Weight 初期値リセット")
    st.caption("GY_INITIAL_FACTORS で定義されたデフォルトの重みに戻すことができます。")
    render_reset_controls(ext_db)

# --- 変更履歴 ---
with tab_history:
    st.subheader("変更履歴 (直近50件)")
    df_log = _load_change_log(ext_db)
    if df_log.empty:
        st.info("変更履歴はまだありません。")
    else:
        st.dataframe(df_log, use_container_width=True, hide_index=True, height=400)

# --- バージョン管理 ---
with tab_version:
    st.subheader("ルールセット バージョン管理")
    st.caption("全ルールのスナップショットを作成・復元できます。Weight変更やステータス遷移時に自動バックアップされます。")

    # スナップショット一覧
    snapshots = registry.list_snapshots()
    if snapshots:
        snap_rows = []
        for s in snapshots:
            snap_rows.append({
                "ID": s["snapshot_id"],
                "バージョン": s["version_label"],
                "トリガー": s.get("trigger", "manual"),
                "ルール数": s.get("rule_count", 0),
                "説明": s.get("description", ""),
                "作成日時": s.get("created_at", "")[:19],
            })
        st.dataframe(pd.DataFrame(snap_rows), use_container_width=True, hide_index=True)
    else:
        st.info("スナップショットはまだありません。")

    # スナップショット作成
    with st.form("snapshot_form"):
        st.markdown("**新規スナップショット作成**")
        snap_label = st.text_input("バージョンラベル", placeholder="例: v1.2.0")
        snap_desc = st.text_area("説明", placeholder="変更内容を記述", height=80)
        snap_trigger = st.selectbox("トリガー", ["manual", "optimization", "calibration"])
        if st.form_submit_button("スナップショット作成"):
            if snap_label:
                snap_id = registry.create_snapshot(snap_label, snap_desc, snap_trigger)
                st.success(f"スナップショット作成完了: ID={snap_id}")
                st.rerun()
            else:
                st.error("バージョンラベルを入力してください")

    # スナップショット復元
    if snapshots:
        st.markdown("---")
        st.markdown("**スナップショット復元**")
        st.warning("復元すると現在のルール状態が上書きされます。復元前に自動バックアップが作成されます。")
        restore_options = {
            f"#{s['snapshot_id']} {s['version_label']} ({s.get('created_at', '')[:10]})": s["snapshot_id"]
            for s in snapshots
        }
        selected_snap = st.selectbox("復元元", list(restore_options.keys()), key="restore_select")
        if st.button("復元実行", key="btn_restore"):
            snap_id = restore_options[selected_snap]
            restored = registry.restore_snapshot(snap_id)
            st.success(f"{restored}件のルールを復元しました")
            st.rerun()

    # ルール変更履歴ビューア
    st.markdown("---")
    st.markdown("**ルール別 変更履歴**")
    if not df_rules.empty:
        hist_options = {
            f"[{r['rule_id']}] {r['rule_name']}": r["rule_id"]
            for _, r in df_rules.iterrows()
        }
        hist_label = st.selectbox("ルール選択", list(hist_options.keys()), key="hist_rule_select")
        hist_rule_id = hist_options[hist_label]
        history = registry.get_rule_history(hist_rule_id)
        if history:
            hist_rows = []
            for h in history:
                hist_rows.append({
                    "バージョン": h.get("version_label", "—"),
                    "Weight": h.get("weight", 0),
                    "ステータス": h.get("review_status", ""),
                    "有効": "Yes" if h.get("is_active") else "No",
                    "アーカイブ日": h.get("archived_at", "")[:19],
                })
            st.dataframe(pd.DataFrame(hist_rows), use_container_width=True, hide_index=True)
        else:
            st.info("このルールのアーカイブ履歴はありません。")
