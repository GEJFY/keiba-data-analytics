"""ファクターWeight初期値リセットコンポーネント。"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.data.db import DatabaseManager
from src.factors.registry import FactorRegistry
from src.factors.rules.gy_factors import GY_INITIAL_FACTORS


def get_weight_diff(ext_db: DatabaseManager) -> list[dict]:
    """現在のWeightとデフォルトWeightの差分を返す。"""
    if not ext_db.table_exists("factor_rules"):
        return []
    registry = FactorRegistry(ext_db)
    rules = registry.get_active_rules()

    default_map = {f["rule_name"]: f["weight"] for f in GY_INITIAL_FACTORS}

    diffs: list[dict] = []
    for rule in rules:
        name = rule["rule_name"]
        current = rule.get("weight", 1.0)
        default = default_map.get(name)
        if default is not None:
            diffs.append({
                "rule_id": rule["rule_id"],
                "rule_name": name,
                "current_weight": round(current, 2),
                "default_weight": round(default, 2),
                "diff": round(current - default, 2),
                "is_changed": abs(current - default) > 0.01,
            })
    return diffs


def render_reset_controls(ext_db: DatabaseManager) -> None:
    """リセットUI: 全体一括 or 個別リセット。"""
    diffs = get_weight_diff(ext_db)

    if not diffs:
        st.info("初期ファクターが登録されていません。")
        return

    changed = [d for d in diffs if d["is_changed"]]

    if not changed:
        st.success("全てのWeight が初期値と一致しています。変更は不要です。")
        return

    st.warning(
        f"**{len(changed)} / {len(diffs)}** 件のファクターが初期値から変更されています。"
    )

    # 差分テーブル表示
    df = pd.DataFrame(changed)
    df_display = df[["rule_name", "current_weight", "default_weight", "diff"]].copy()
    df_display.columns = ["ルール名", "現在値", "初期値", "差分"]
    st.dataframe(df_display, use_container_width=True, hide_index=True)

    # 全体リセットボタン
    col1, col2 = st.columns([2, 1])
    with col1:
        st.caption("全ファクターのWeightを初期値に戻します。変更は監査ログに記録されます。")
    with col2:
        if st.button("全ファクターを初期値にリセット", type="primary", key="btn_reset_all"):
            registry = FactorRegistry(ext_db)
            for d in changed:
                registry.update_weight(
                    d["rule_id"],
                    d["default_weight"],
                    reason="初期値にリセット",
                    changed_by="reset",
                )
            st.success(f"{len(changed)} 件のWeightを初期値にリセットしました。")
            st.rerun()

    # 個別リセット
    st.divider()
    st.markdown("##### 個別リセット")
    for d in changed:
        col_name, col_val, col_btn = st.columns([3, 2, 1])
        with col_name:
            st.text(d["rule_name"])
        with col_val:
            st.text(f"{d['current_weight']} \u2192 {d['default_weight']}")
        with col_btn:
            if st.button("戻す", key=f"reset_{d['rule_id']}"):
                registry = FactorRegistry(ext_db)
                registry.update_weight(
                    d["rule_id"],
                    d["default_weight"],
                    reason="初期値に個別リセット",
                    changed_by="reset",
                )
                st.rerun()
