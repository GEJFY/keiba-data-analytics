"""Tab 1: データ管理ページ。

JVLink DBのテーブル状況、レコード数、データ品質チェック結果を表示する。
JVLink同期、拡張データ削除の機能を提供する。
バックグラウンド実行対応。
"""

from typing import Any

import pandas as pd
import streamlit as st

from src.dashboard import config_loader
from src.dashboard.components.charts import horizontal_bar_chart
from src.dashboard.components.task_status import show_task_progress
from src.dashboard.components.workflow_bar import mark_step_completed, render_workflow_bar
from src.dashboard.config_loader import PROJECT_ROOT
from src.dashboard.task_manager import TaskManager
from src.data.db import DatabaseManager
from src.data.validator import CheckItem, DataValidator

# 競馬場コードマッピング
JYO_MAP = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
}

# 削除可能な拡張テーブル（factor_rulesは保護）
DELETABLE_TABLES = {
    "horse_scores": "スコアリング結果",
    "bets": "投票履歴",
    "bankroll_log": "資金ログ",
    "backtest_results": "バックテスト結果",
    "data_sync_log": "同期履歴",
    "factor_review_log": "ファクター変更履歴",
}


def _detect_data_source(db: DatabaseManager) -> dict:
    """ダミーデータか本番データかを判定する。"""
    if not db.table_exists("NL_RA_RACE"):
        return {"source": "empty"}

    rows = db.execute_query("SELECT DISTINCT idJyoCD FROM NL_RA_RACE")
    jyo_codes = {r["idJyoCD"] for r in rows}

    date_info = db.execute_query(
        "SELECT MIN(idYear || idMonthDay) AS min_d, "
        "MAX(idYear || idMonthDay) AS max_d, "
        "COUNT(DISTINCT idYear || idMonthDay) AS days "
        "FROM NL_RA_RACE"
    )
    if not date_info or date_info[0]["days"] is None:
        return {"source": "empty"}

    info = date_info[0]
    # ダミーデータ判定: 中山のみ + 3日以下
    is_dummy = jyo_codes == {"06"} and info["days"] <= 3

    return {
        "source": "dummy" if is_dummy else "jvlink",
        "jyo_codes": jyo_codes,
        "min_date": str(info["min_d"]),
        "max_date": str(info["max_d"]),
        "day_count": info["days"],
    }


def _get_table_counts(db: DatabaseManager) -> list[dict]:
    """主要テーブルのレコード数を取得する。"""
    tables = [
        "NL_RA_RACE", "NL_SE_RACE_UMA",
        "NL_O1_ODDS_TANFUKUWAKU", "NL_O2_ODDS_UMAREN",
        "NL_O3_ODDS_WIDE", "NL_O4_ODDS_UMATAN",
        "NL_O5_ODDS_SANREN", "NL_O6_ODDS_SANRENTAN",
        "NL_HR_PAY", "NL_UM_UMA", "NL_KS_KISYU",
    ]
    # 存在テーブルを1クエリで確認
    placeholders = ",".join("?" * len(tables))
    existing = db.execute_query(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({placeholders})",
        tuple(tables),
    )
    existing_set = {r["name"] for r in existing}

    # 存在するテーブルのCOUNTをUNION ALLで一括取得
    present = [t for t in tables if t in existing_set]
    count_map: dict[str, int] = {}
    if present:
        union_sql = " UNION ALL ".join(
            f"SELECT '{t}' AS tbl, COUNT(*) AS cnt FROM [{t}]" for t in present
        )
        for r in db.execute_query(union_sql):
            count_map[r["tbl"]] = r["cnt"]

    rows = []
    for tbl in tables:
        if tbl in existing_set:
            rows.append({"テーブル": tbl, "レコード数": count_map.get(tbl, 0), "状態": "OK"})
        else:
            rows.append({"テーブル": tbl, "レコード数": 0, "状態": "未作成"})
    return rows


def _get_race_list(db: DatabaseManager) -> pd.DataFrame:
    """レース一覧を取得する。"""
    if not db.table_exists("NL_RA_RACE"):
        return pd.DataFrame()

    sql = """
        SELECT
            idYear || '/' || SUBSTR(idMonthDay, 1, 2) || '/' || SUBSTR(idMonthDay, 3, 2) AS 日付,
            idJyoCD AS 場CD,
            idRaceNum AS R,
            RaceInfoHondai AS レース名,
            Kyori AS 距離,
            CASE SUBSTR(TrackCD, 1, 1)
                WHEN '1' THEN '芝'
                WHEN '2' THEN 'ダート'
                ELSE TrackCD
            END AS コース,
            SyussoTosu AS 頭数
        FROM NL_RA_RACE r
        ORDER BY idYear DESC, idMonthDay DESC, idJyoCD, CAST(idRaceNum AS INTEGER)
    """
    rows = db.execute_query(sql)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["競馬場"] = df["場CD"].map(JYO_MAP).fillna(df["場CD"])
    return df[["日付", "競馬場", "R", "レース名", "距離", "コース", "頭数"]]


def _get_sync_log(ext_db: DatabaseManager) -> pd.DataFrame:
    """データ同期履歴を取得する。"""
    if not ext_db.table_exists("data_sync_log"):
        return pd.DataFrame()
    rows = ext_db.execute_query(
        "SELECT started_at, finished_at, status, records_added, error_message "
        "FROM data_sync_log ORDER BY started_at DESC LIMIT 20"
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ==============================================================
# バックグラウンドタスク用ラッパー
# ==============================================================

def _resolve_db_paths() -> tuple[str, str]:
    """メインスレッドでDBパスを解決する（submit前に呼ぶこと）。"""
    config = st.session_state.config
    db_cfg = config.get("database", {})
    jvlink_path = str((PROJECT_ROOT / db_cfg.get("jvlink_db_path", "data/jvlink.db")).resolve())
    ext_path = str((PROJECT_ROOT / db_cfg.get("extension_db_path", "data/extension.db")).resolve())
    return jvlink_path, ext_path


def _run_sync_bg(
    jvlink_db_path: str,
    ext_db_path: str,
    exe_path: str,
    timeout_sec: int = 600,
    enable_setup_data: bool = False,
    progress_callback: Any = None,
) -> dict:
    """JVLink同期をバックグラウンドで実行する。"""
    import time

    from src.data.jvlink_sync import JVLinkSyncManager

    jvlink_db = DatabaseManager(jvlink_db_path)
    ext_db = DatabaseManager(ext_db_path)

    if progress_callback:
        progress_callback(0, 4, "同期マネージャ初期化中...")

    sync_mgr = JVLinkSyncManager(
        jvlink_db=jvlink_db,
        ext_db=ext_db,
        exe_path=exe_path,
        enable_setup_data=enable_setup_data,
    )

    if progress_callback:
        progress_callback(1, 4, "同期前レコード数を取得中...")

    before = sync_mgr._get_record_counts()

    if progress_callback:
        progress_callback(2, 4, "JVLinkToSQLite.exe を実行中...")

    t0 = time.time()
    result = sync_mgr.run_sync(timeout_sec=timeout_sec)
    elapsed = time.time() - t0

    if progress_callback:
        progress_callback(3, 4, "同期後レコード数を確認中...")

    after = sync_mgr._get_record_counts()

    # 差分計算
    diff_info = {}
    for tbl in after:
        diff = after[tbl] - before.get(tbl, 0)
        diff_info[tbl] = {"before": before.get(tbl, 0), "after": after[tbl], "diff": diff}

    if progress_callback:
        progress_callback(4, 4, "同期完了")

    return {
        "status": result["status"],
        "stdout": result.get("stdout", ""),
        "records_added": result.get("records_added", 0),
        "error_message": result.get("error_message", ""),
        "elapsed": elapsed,
        "diff_info": diff_info,
        "validation": result.get("validation", {}),
    }


# ==============================
# ページ本体
# ==============================
st.header("データ管理")
render_workflow_bar("data")

tm: TaskManager = st.session_state.task_manager
jvlink_db: DatabaseManager = st.session_state.jvlink_db
ext_db: DatabaseManager = st.session_state.ext_db
_jvlink_db_path, _ext_db_path = _resolve_db_paths()

# --- データソース判定バナー ---
data_info = _detect_data_source(jvlink_db)
if data_info["source"] == "dummy":
    st.warning(
        "**現在のデータはダミーデータです**\n\n"
        "`scripts/seed_dummy_data.py` で生成された模擬データが表示されています。\n"
        "2025年1月 中山開催 3日分（36レース）の架空データです。\n\n"
        "本番データを使用するには、下の「JVLink データ同期」セクションからデータを取り込んでください。"
    )
elif data_info["source"] == "jvlink":
    min_d = data_info["min_date"]
    max_d = data_info["max_date"]
    jyo_names = ", ".join(JYO_MAP.get(c, c) for c in sorted(data_info["jyo_codes"]))
    st.success(
        f"**JVLinkデータ**: {min_d[:4]}年{min_d[4:6]}月 〜 "
        f"{max_d[:4]}年{max_d[4:6]}月 "
        f"({data_info['day_count']}開催日 / {jyo_names})"
    )
    mark_step_completed("data")
else:
    st.info(
        "データがまだ読み込まれていません。\n\n"
        "`run.bat` のメニュー4でダミーデータを生成するか、"
        "下の「JVLink データ同期」から本番データを取り込んでください。"
    )

# --- テーブル状況 ---
st.subheader("テーブル状況")
table_data = _get_table_counts(jvlink_db)
ext_tables = ["factor_rules", "horse_scores", "bets", "bankroll_log", "backtest_results", "data_sync_log"]
# 拡張テーブルも一括でCOUNT取得
_ext_placeholders = ",".join("?" * len(ext_tables))
_ext_existing = ext_db.execute_query(
    f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({_ext_placeholders})",
    tuple(ext_tables),
)
_ext_existing_set = {r["name"] for r in _ext_existing}
_ext_present = [t for t in ext_tables if t in _ext_existing_set]
_ext_count_map: dict[str, int] = {}
if _ext_present:
    _ext_union = " UNION ALL ".join(
        f"SELECT '{t}' AS tbl, COUNT(*) AS cnt FROM [{t}]" for t in _ext_present
    )
    for r in ext_db.execute_query(_ext_union):
        _ext_count_map[r["tbl"]] = r["cnt"]
for tbl in ext_tables:
    if tbl in _ext_existing_set:
        table_data.append({"テーブル": f"[拡張] {tbl}", "レコード数": _ext_count_map.get(tbl, 0), "状態": "OK"})
    else:
        table_data.append({"テーブル": f"[拡張] {tbl}", "レコード数": 0, "状態": "未作成"})

col1, col2 = st.columns(2)
with col1:
    jvlink_tables = [r for r in table_data if not r["テーブル"].startswith("[拡張]")]
    st.markdown("**JVLink DB** (レースデータ元)")
    st.dataframe(pd.DataFrame(jvlink_tables), use_container_width=True, hide_index=True)
with col2:
    ext_table_rows = [r for r in table_data if r["テーブル"].startswith("[拡張]")]
    st.markdown("**拡張DB** (分析結果保管先)")
    st.dataframe(pd.DataFrame(ext_table_rows), use_container_width=True, hide_index=True)

# --- JVLink データ同期 ---
st.divider()
st.subheader("JVLink データ同期")

jvlink_config = st.session_state.config.get("jvlink", {})
exe_path_raw = jvlink_config.get("exe_path", "")
timeout_sec = jvlink_config.get("sync_timeout_sec", 600)

if not exe_path_raw:
    st.info(
        "JVLinkToSQLite.exe のパスが未設定です。\n\n"
        "`config/config.yaml` の `jvlink.exe_path` を設定してください。\n\n"
        "```yaml\n"
        "jvlink:\n"
        '  exe_path: "./JVLinkToSQLiteArtifact_0.1.2.0.exe"\n'
        "```"
    )
else:
    exe_resolved = (config_loader.PROJECT_ROOT / exe_path_raw).resolve()
    exe_exists = exe_resolved.exists()

    if not exe_exists:
        st.error(f"JVLinkToSQLite.exe が見つかりません: `{exe_resolved}`")
    else:
        # 初回セットアップガイド
        sync_bat = config_loader.PROJECT_ROOT / "sync_jvlink.bat"
        with st.expander("初めて同期する場合（JRA-VAN利用規約の同意が必要）"):
            st.markdown(
                "JV-Linkを初めて使用する場合、またはJRA-VAN利用規約が更新された場合は、\n"
                "**手動で1回だけ** `sync_jvlink.bat` を実行して利用規約に同意する必要があります。\n\n"
                "1. 下のボタンで `sync_jvlink.bat` を起動\n"
                "2. コマンドプロンプトが開き、JRA-VAN利用規約ダイアログが表示される\n"
                "3. 利用規約のリンクを確認 → チェック → 「同意する」をクリック\n"
                "4. データ同期が完了するまで待つ（初回は数分〜数十分）\n"
                "5. 完了後、このページに戻って画面を更新\n\n"
                "同意後は、このダッシュボードの「JVLink同期を実行」ボタンで自動同期できます。"
            )
            if sync_bat.exists():
                if st.button("sync_jvlink.bat を実行", key="btn_run_sync_bat"):
                    import subprocess as sp
                    sp.Popen(
                        ["cmd", "/c", "start", "", str(sync_bat)],
                        cwd=str(config_loader.PROJECT_ROOT),
                    )
                    st.info("別ウィンドウでコマンドプロンプトが開きます。利用規約に同意してください。")
            else:
                st.warning(f"`sync_jvlink.bat` が見つかりません: `{sync_bat}`")

        col_before, col_action = st.columns([2, 1])
        with col_before:
            st.markdown("**現在のレコード数:**")
            for tbl in ["NL_RA_RACE", "NL_SE_RACE_UMA", "NL_HR_PAY"]:
                if jvlink_db.table_exists(tbl):
                    cnt = jvlink_db.execute_query(
                        f"SELECT COUNT(*) as cnt FROM [{tbl}]"
                    )[0]["cnt"]
                    st.text(f"  {tbl}: {cnt:,} 件")
        with col_action:
            st.markdown("**同期設定:**")
            st.caption(f"exe: {exe_path_raw}")
            st.caption(f"タイムアウト: {timeout_sec}秒")

        st.caption(
            "JVLinkToSQLiteは主キー（開催日+競馬場+回次+日次+レース番号）で"
            "自動的にUPSERTを行うため、同じデータを複数回同期しても重複は発生しません。"
        )

        # バックグラウンド同期の進捗表示
        is_syncing = tm.has_running("JVLink同期")
        show_task_progress("sync_task_id", "sync_result", tm)

        # 前回の同期結果表示
        sync_result = st.session_state.get("sync_result")
        if sync_result is not None:
            if sync_result["status"] == "SUCCESS":
                st.success(
                    f"同期完了: {sync_result.get('records_added', 0):,} 件追加 "
                    f"({sync_result['elapsed']:.1f}秒)"
                )
            elif sync_result["status"] == "SKIPPED":
                st.info(f"同期スキップ: {sync_result.get('error_message', '')}")
            else:
                st.error(f"同期エラー: {sync_result.get('error_message', '')}")

            # 差分詳細
            diff_info = sync_result.get("diff_info", {})
            if diff_info:
                with st.expander("同期結果詳細"):
                    for tbl, info in diff_info.items():
                        mark = f" (+{info['diff']:,})" if info["diff"] > 0 else ""
                        st.text(f"  {tbl}: {info['after']:,} 件{mark}")

                    stdout = sync_result.get("stdout", "")
                    if stdout:
                        st.code(stdout, language="text")

                    validation = sync_result.get("validation", {})
                    if validation and not validation.get("error"):
                        missing = validation.get("missing_tables", [])
                        if missing:
                            st.write(f"  未検出テーブル: {', '.join(missing)}")
                        else:
                            st.write("  全必須テーブル確認済み")

            if st.button("結果をクリア", key="btn_sync_clear"):
                del st.session_state["sync_result"]
                st.toast("同期結果をクリアしました", icon="🗑️")
                st.rerun()

        btn_label = "同期実行中..." if is_syncing else "JVLink同期を実行"
        if st.button(btn_label, type="primary", key="btn_sync", disabled=is_syncing):
            task_id = tm.submit(
                name="JVLink同期",
                target=_run_sync_bg,
                kwargs={
                    "jvlink_db_path": _jvlink_db_path,
                    "ext_db_path": _ext_db_path,
                    "exe_path": str(exe_resolved),
                    "timeout_sec": timeout_sec,
                },
            )
            st.session_state["sync_task_id"] = task_id
            st.toast("JVLink同期を開始しました — サイドバーで進捗を確認できます", icon="⏳")
            st.rerun()

# --- データ品質チェック ---
st.divider()
st.subheader("データ品質チェック")
st.caption("テーブル存在・レコード数・重要カラム欠損・テーブル間整合性を一括チェックします。")

if st.button("品質チェック実行", key="btn_quality"):
    with st.spinner("品質チェックを実行中..."):
        validator = DataValidator(jvlink_db)
        result = validator.run_full_check()
        st.session_state["quality_result"] = result
    st.toast("品質チェックが完了しました", icon="✅")

quality_result = st.session_state.get("quality_result")
if quality_result is not None:
    check_items: list[CheckItem] = quality_result.get("check_items", [])

    # --- サマリー ---
    ok_count = sum(1 for c in check_items if c.status == "OK")
    warn_count = sum(1 for c in check_items if c.status == "WARNING")
    err_count = sum(1 for c in check_items if c.status == "ERROR")
    total_count = len(check_items)

    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("チェック項目数", total_count)
    sc2.metric("OK", ok_count)
    sc3.metric("警告", warn_count)
    sc4.metric("エラー", err_count)

    if err_count == 0 and warn_count == 0:
        st.success(f"全 {total_count} 項目のチェックに合格しました。")
    elif err_count > 0:
        st.error(f"{err_count} 件のエラーがあります。データの取り込み状況を確認してください。")
    else:
        st.warning(f"{warn_count} 件の警告があります。")

    # --- データカバレッジ ---
    coverage = quality_result.get("data_coverage", {})
    if coverage and coverage.get("race_count"):
        jyo_map = {
            "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
            "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
        }
        with st.expander("データカバレッジ", expanded=True):
            cc1, cc2, cc3 = st.columns(3)
            min_d = coverage.get("min_date", "")
            max_d = coverage.get("max_date", "")
            if len(min_d) >= 8:
                date_str = f"{min_d[:4]}/{min_d[4:6]}/{min_d[6:8]} 〜 {max_d[:4]}/{max_d[4:6]}/{max_d[6:8]}"
            else:
                date_str = f"{min_d} 〜 {max_d}"
            cc1.metric("データ期間", date_str)
            cc2.metric("開催日数", coverage.get("day_count", 0))
            cc3.metric("レース数", f"{coverage.get('race_count', 0):,}")

            venues = coverage.get("venues", {})
            if venues:
                st.markdown("**競馬場別レース数:**")
                venue_parts = []
                for code, cnt in sorted(venues.items(), key=lambda x: -x[1]):
                    name = jyo_map.get(code, code)
                    venue_parts.append(f"{name}: {cnt:,}R")
                st.text("  " + " / ".join(venue_parts))

                # 競馬場別レース数グラフ
                venue_labels = [JYO_MAP.get(c, c) for c, _ in sorted(venues.items(), key=lambda x: -x[1])]
                venue_values = [cnt for _, cnt in sorted(venues.items(), key=lambda x: -x[1])]
                fig_venue = horizontal_bar_chart(venue_labels, venue_values, "競馬場別レース数")
                st.plotly_chart(fig_venue, use_container_width=True)

            entries = coverage.get("horse_entries")
            if entries:
                st.text(f"  出走馬レコード数: {entries:,} 件")

    # --- カテゴリ別チェック結果 ---
    status_icon = {"OK": ":white_check_mark:", "WARNING": ":warning:", "ERROR": ":x:"}

    # テーブル存在チェック
    table_checks = [c for c in check_items if c.category == "table"]
    if table_checks:
        with st.expander(
            f"テーブル存在チェック ({sum(1 for c in table_checks if c.status == 'OK')}/{len(table_checks)} OK)",
            expanded=err_count > 0,
        ):
            for c in table_checks:
                icon = status_icon.get(c.status, "")
                st.markdown(f"{icon} **{c.name}**: {c.detail}")

    # レコード数・欠損値チェック
    record_checks = [c for c in check_items if c.category in ("record", "column")]
    if record_checks:
        with st.expander(
            f"レコード数・欠損値チェック ({sum(1 for c in record_checks if c.status == 'OK')}/{len(record_checks)} OK)",
            expanded=any(c.status != "OK" for c in record_checks),
        ):
            for c in record_checks:
                icon = status_icon.get(c.status, "")
                st.markdown(f"{icon} **{c.name}**: {c.detail}")

    # テーブル間整合性
    consistency_checks = [c for c in check_items if c.category == "consistency"]
    if consistency_checks:
        with st.expander(
            f"テーブル間整合性 ({sum(1 for c in consistency_checks if c.status == 'OK')}/{len(consistency_checks)} OK)",
            expanded=any(c.status != "OK" for c in consistency_checks),
        ):
            for c in consistency_checks:
                icon = status_icon.get(c.status, "")
                st.markdown(f"{icon} **{c.name}**: {c.detail}")

    # オッズテーブル詳細
    odds_checks = [c for c in check_items if c.category == "odds"]
    if odds_checks:
        with st.expander(
            f"オッズテーブル ({sum(1 for c in odds_checks if c.status == 'OK')}/{len(odds_checks)} OK)",
            expanded=any(c.status != "OK" for c in odds_checks),
        ):
            for c in odds_checks:
                icon = status_icon.get(c.status, "")
                st.markdown(f"{icon} **{c.name}**: {c.detail}")

    # クリアボタン
    if st.button("チェック結果をクリア", key="btn_quality_clear"):
        del st.session_state["quality_result"]
        st.toast("チェック結果をクリアしました", icon="🗑️")
        st.rerun()

# --- レース一覧 ---
st.divider()
st.subheader("レース一覧")
df_races = _get_race_list(jvlink_db)
if df_races.empty:
    st.info(
        "レースデータがありません。\n\n"
        "`run.bat` のメニュー4でダミーデータを生成するか、"
        "上の「JVLink データ同期」から本番データを取り込んでください。"
    )
else:
    st.dataframe(df_races, use_container_width=True, hide_index=True, height=400)
    st.caption(f"全 {len(df_races)} レース")

# --- 拡張データ管理（削除） ---
st.divider()
st.subheader("拡張データ管理")
st.caption(
    "拡張DBのテーブルデータを個別にクリアできます。"
    "JVLink DBのレースデータは保護されており、ここからは削除できません。"
)

delete_data = []
for tbl, desc in DELETABLE_TABLES.items():
    cnt = ext_db.execute_query(f"SELECT COUNT(*) as cnt FROM [{tbl}]")[0]["cnt"] if ext_db.table_exists(tbl) else 0
    delete_data.append({"テーブル": tbl, "説明": desc, "件数": cnt})

st.dataframe(pd.DataFrame(delete_data), use_container_width=True, hide_index=True)

tables_to_delete = st.multiselect(
    "削除するテーブルを選択",
    list(DELETABLE_TABLES.keys()),
    format_func=lambda t: f"{t} ({DELETABLE_TABLES[t]})",
    key="ms_delete",
)

if tables_to_delete:
    total_records = sum(
        d["件数"] for d in delete_data if d["テーブル"] in tables_to_delete
    )
    st.warning(
        f"**{len(tables_to_delete)} テーブル / {total_records:,} 件のデータが完全に削除されます。**\n\n"
        "この操作は取り消せません。"
    )
    confirm_text = st.text_input(
        '削除を実行するには "delete" と入力してください',
        key="delete_confirm",
    )

    if st.button("データ削除を実行", type="primary", key="btn_delete"):
        if confirm_text == "delete":
            deleted_total = 0
            for tbl in tables_to_delete:
                if ext_db.table_exists(tbl):
                    cnt = ext_db.execute_write(f"DELETE FROM [{tbl}]")
                    deleted_total += cnt
                    st.text(f"  {tbl}: {cnt} 件削除")
            st.success(f"合計 {deleted_total:,} 件のデータを削除しました。")
            st.toast(f"{deleted_total:,} 件のデータを削除しました", icon="🗑️")
            st.rerun()
        else:
            st.error('確認テキストが一致しません。"delete" と入力してください。')

# --- 同期履歴 ---
st.divider()
st.subheader("データ同期履歴")
df_sync = _get_sync_log(ext_db)
if df_sync.empty:
    st.info("同期履歴はまだありません。")
else:
    st.dataframe(df_sync, use_container_width=True, hide_index=True)
