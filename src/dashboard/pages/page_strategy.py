"""Tab 3: 戦略実行ページ。

レース選択、馬のスコアリング、バリューベット抽出、投票シミュレーション、投票実行を行う。
"""

import pandas as pd
import streamlit as st

from src.dashboard.components.workflow_bar import render_workflow_bar
from src.betting.bankroll import BankrollManager, BettingMethod
from src.betting.executor import BetExecutor
from src.betting.safety import SafetyGuard
from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider
from src.scoring.engine import ScoringEngine
from src.strategy.base import Bet

# 競馬場コード
JYO_MAP = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
}


def _get_race_keys(db: DatabaseManager) -> list[dict]:
    """全レースキーを取得する。"""
    if not db.table_exists("NL_RA_RACE"):
        return []
    return db.execute_query(
        "SELECT idYear AS Year, idMonthDay AS MonthDay, idJyoCD AS JyoCD, "
        "idKaiji AS Kaiji, idNichiji AS Nichiji, idRaceNum AS RaceNum, "
        "RaceInfoHondai AS RaceName, Kyori "
        "FROM NL_RA_RACE ORDER BY idYear DESC, idMonthDay DESC, CAST(idRaceNum AS INTEGER)"
    )


def _build_odds_map(db: DatabaseManager, race_key: str) -> dict[str, float]:
    """オッズマップを構築する。provider.get_odds()がdict[str,float]を返す。"""
    provider = JVLinkDataProvider(db)
    return provider.get_odds(race_key)


# ==============================
# ページ本体
# ==============================
st.header("戦略実行")
render_workflow_bar("betting")

jvlink_db: DatabaseManager = st.session_state.jvlink_db
ext_db: DatabaseManager = st.session_state.ext_db
config = st.session_state.config

# --- レース選択 ---
st.subheader("レース選択")
races = _get_race_keys(jvlink_db)
if not races:
    st.warning("レースデータがありません。データ管理タブからデータを取り込んでください。")
    st.stop()

# --- フィルタ ---
with st.expander("レース絞り込み", expanded=False):
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        jyo_options = ["全て"] + sorted(set(JYO_MAP.get(r["JyoCD"], r["JyoCD"]) for r in races))
        filter_jyo = st.selectbox("競馬場", jyo_options, key="filter_jyo")
    with fc2:
        dist_options = ["全て", "短距離(~1400m)", "マイル(1401-1800m)", "中距離(1801-2200m)", "長距離(2201m~)"]
        filter_dist = st.selectbox("距離", dist_options, key="filter_dist")
    with fc3:
        filter_date = st.text_input("日付(YYYYMMDD)", value="", placeholder="20250105", key="filter_date")

filtered_races = races
if filter_jyo != "全て":
    filtered_races = [r for r in filtered_races if JYO_MAP.get(r["JyoCD"], r["JyoCD"]) == filter_jyo]
if filter_dist != "全て":
    def _match_dist(kyori_str, category):
        try:
            k = int(kyori_str)
        except (ValueError, TypeError):
            return True
        if category == "短距離(~1400m)":
            return k <= 1400
        elif category == "マイル(1401-1800m)":
            return 1401 <= k <= 1800
        elif category == "中距離(1801-2200m)":
            return 1801 <= k <= 2200
        elif category == "長距離(2201m~)":
            return k >= 2201
        return True
    filtered_races = [r for r in filtered_races if _match_dist(r.get("Kyori", ""), filter_dist)]
if filter_date:
    filtered_races = [r for r in filtered_races if f"{r['Year']}{r['MonthDay']}" == filter_date]

if not filtered_races:
    st.warning("条件に合うレースがありません。フィルタを調整してください。")
    st.stop()

race_labels = {}
for r in filtered_races:
    jyo_name = JYO_MAP.get(r["JyoCD"], r["JyoCD"])
    date_str = f"{r['Year']}/{r['MonthDay'][:2]}/{r['MonthDay'][2:]}"
    label = f"{date_str} {jyo_name} {r['RaceNum']}R {r.get('RaceName', '')} ({r.get('Kyori', '')}m)"
    key = f"{r['Year']}{r['MonthDay']}{r['JyoCD']}{r['Kaiji']}{r['Nichiji']}{r['RaceNum']}"
    race_labels[label] = (key, r)

selected_label = st.selectbox("レースを選択", list(race_labels.keys()))
race_key, race_info = race_labels[selected_label]

# --- 出走馬・オッズ取得 ---
provider = JVLinkDataProvider(jvlink_db)
entries = provider.get_race_entries(race_key)
odds_map = _build_odds_map(jvlink_db, race_key)

if not entries:
    st.warning("出走馬データがありません。")
    st.stop()

# --- スコアリング ---
st.subheader("スコアリング結果")
engine = ScoringEngine(ext_db, jvlink_provider=provider)
results = engine.score_race(race_info, entries, odds_map, race_key=race_key)

if results:
    rows = []
    for r in results:
        rows.append({
            "馬番": r["umaban"],
            "GY指数": f"{r['total_score']:.1f}",
            "推定勝率": f"{r.get('estimated_prob', 0):.1%}",
            "適正オッズ": f"{r.get('fair_odds', 0):.1f}",
            "実オッズ": f"{r.get('actual_odds', 0):.1f}",
            "EV": f"{r.get('expected_value', 0):.3f}",
            "バリュー": "★" if r.get("is_value_bet", False) else "",
        })
    df_scores = pd.DataFrame(rows)
    st.dataframe(df_scores, width="stretch", hide_index=True)

    # バリューベット抽出
    value_bets = [r for r in results if r.get("is_value_bet", False)]
    if value_bets:
        st.success(f"バリューベット: {len(value_bets)} 件検出")
    else:
        st.info("バリューベットは検出されませんでした。")

    # GY指数分布とEV散布図
    from src.dashboard.components.charts import histogram_chart, scatter_chart
    col_gs1, col_gs2 = st.columns(2)
    with col_gs1:
        scores = [float(r["total_score"]) for r in results]
        fig_score = histogram_chart(scores, "GY指数分布", nbins=20)
        st.plotly_chart(fig_score, use_container_width=True)
    with col_gs2:
        odds_vals = [r.get("actual_odds", 0) for r in results if r.get("actual_odds", 0) > 0]
        ev_vals = [r.get("expected_value", 0) for r in results if r.get("actual_odds", 0) > 0]
        labels = [f"馬番{r['umaban']}" for r in results if r.get("actual_odds", 0) > 0]
        if odds_vals:
            fig_ev = scatter_chart(odds_vals, ev_vals, labels, "オッズ vs EV", "実オッズ", "期待値(EV)")
            # Add EV=1.0 reference line
            fig_ev.add_hline(y=1.0, line_dash="dash", line_color="gray", annotation_text="EV=1.0")
            st.plotly_chart(fig_ev, use_container_width=True)
else:
    st.info("スコアリング結果がありません（オッズ=0の馬は除外）。")

# --- 投票シミュレーション ---
st.divider()
st.subheader("投票シミュレーション")

bankroll_config = config.get("bankroll", {})
initial_balance = bankroll_config.get("initial_balance", 1_000_000)

col1, col2, col3, col4 = st.columns(4)
with col1:
    sim_balance = st.number_input("シミュレーション残高 (円)", value=initial_balance, step=100_000)
with col2:
    method_name = st.selectbox("投票方式", ["QUARTER_KELLY", "EQUAL", "EV_PROPORTIONAL"])
with col3:
    ev_threshold = st.number_input(
        "EV閾値", value=1.15, step=0.05, format="%.2f",
        help="高いほど厳選（件数減・ROI向上）。1.05=緩め、1.15=推奨、1.30=厳選。",
    )
with col4:
    bet_type = st.selectbox(
        "券種", ["WIN", "PLACE"],
        help="WIN=単勝(控除率20%)、PLACE=複勝(控除率20%)。三連系は控除率25-27.5%のため非推奨。",
    )

with st.expander("回収率向上のヒント", expanded=False):
    st.markdown("""
| 項目 | 推奨値 | 理由 |
|------|--------|------|
| **EV閾値** | 1.15以上 | 閾値を上げると件数は減るがROI改善。1.05だと薄利多売になりがち |
| **券種** | WIN / PLACE | 控除率20%で最もハードルが低い。三連単は控除率27.5% |
| **投票方式** | QUARTER_KELLY | Kelly基準の1/4でリスク管理。破産確率を大幅に低減 |

**券種別控除率（JRA）:**
- 単勝/複勝: **20%** (回収率ベースライン80%)
- 馬連/ワイド: **22.5%** (ベースライン77.5%)
- 三連複: **25%** (ベースライン75%)
- 三連単: **27.5%** (ベースライン72.5%)
""")

if st.button("投票額を計算", key="btn_simulate"):
    method = BettingMethod[method_name]
    bankroll = BankrollManager(
        initial_balance=sim_balance,
        method=method,
        max_daily_rate=bankroll_config.get("max_daily_rate", 0.20),
        max_per_race_rate=bankroll_config.get("max_per_race_rate", 0.05),
        drawdown_cutoff=bankroll_config.get("drawdown_cutoff", 0.30),
    )
    safety = SafetyGuard()

    can_bet, reason = safety.check_can_bet()
    if not can_bet:
        st.error(f"安全チェック不合格: {reason}")
    else:
        bet_rows = []
        for r in results:
            if r.get("expected_value", 0) > ev_threshold:
                stake = bankroll.calculate_stake(
                    estimated_prob=r.get("estimated_prob", 0),
                    odds=r.get("actual_odds", 0),
                )
                if stake > 0:
                    bet_rows.append({
                        "馬番": r["umaban"],
                        "EV": f"{r['expected_value']:.3f}",
                        "推定勝率": f"{r['estimated_prob']:.1%}",
                        "投票額": f"{stake:,}円",
                    })
        if bet_rows:
            st.dataframe(pd.DataFrame(bet_rows), width="stretch", hide_index=True)
            total_stake = sum(int(b["投票額"].replace(",", "").replace("円", "")) for b in bet_rows)
            st.metric("合計投票額", f"{total_stake:,}円")
        else:
            st.info("投票対象なし（EV閾値を超えるベットがありません）。")

# --- 投票実行 ---
st.divider()
st.subheader("投票実行")

if not results:
    st.info("先にスコアリング結果を確認してください。")
    st.stop()

betting_config = config.get("betting", {})

col_m, col_e, col_bt = st.columns(3)
with col_m:
    exec_method = st.selectbox(
        "投票方式",
        ["dryrun", "ipatgo"],
        help="dryrun: ログのみ（テスト用） / ipatgo: CSV出力（外部ツール連携）",
        key="sel_exec_method",
    )
with col_e:
    exec_ev = st.number_input(
        "EV閾値", value=1.15, step=0.05, format="%.2f",
        key="exec_ev",
        help="推奨: 1.15以上。高いほどROI改善（件数は減少）。",
    )
with col_bt:
    exec_bet_type = st.selectbox(
        "券種", ["WIN", "PLACE"],
        key="exec_bet_type",
        help="WIN=単勝、PLACE=複勝（控除率同一: 20%）。",
    )

# Build bet candidates
method = BettingMethod[method_name]
bankroll_mgr = BankrollManager(
    initial_balance=sim_balance,
    method=method,
    max_daily_rate=bankroll_config.get("max_daily_rate", 0.20),
    max_per_race_rate=bankroll_config.get("max_per_race_rate", 0.05),
    drawdown_cutoff=bankroll_config.get("drawdown_cutoff", 0.30),
)

bet_candidates: list[Bet] = []
for r in results:
    if r.get("expected_value", 0) > exec_ev:
        stake = bankroll_mgr.calculate_stake(
            estimated_prob=r.get("estimated_prob", 0),
            odds=r.get("actual_odds", 0),
        )
        if stake > 0:
            selected_bet_type = exec_bet_type
            bet_candidates.append(Bet(
                race_key=race_key,
                bet_type=selected_bet_type,
                selection=str(r["umaban"]),
                stake_yen=stake,
                est_prob=r.get("estimated_prob", 0),
                odds_at_bet=r.get("actual_odds", 0),
                est_ev=r.get("expected_value", 0),
                factor_details=r.get("factor_details", {}),
            ))

if not bet_candidates:
    st.info("EV閾値を超えるバリューベットがありません。")
else:
    # Show bet summary table
    bet_summary = []
    for b in bet_candidates:
        bet_summary.append({
            "馬番": b.selection,
            "オッズ": f"{b.odds_at_bet:.1f}",
            "EV": f"{b.est_ev:.3f}",
            "投票額": f"{b.stake_yen:,}円",
        })
    st.dataframe(pd.DataFrame(bet_summary), width="stretch", hide_index=True)

    total_bet = sum(b.stake_yen for b in bet_candidates)
    st.metric("合計投票額", f"{total_bet:,}円")

    # Safety check
    safety_guard = SafetyGuard()
    can_bet, reason = safety_guard.check_can_bet()
    if not can_bet:
        st.error(f"安全チェック不合格: {reason}")
    else:
        st.success(f"安全チェック: OK")

        if exec_method != "dryrun":
            st.warning(
                f"**注意**: `{exec_method}` モードでは実際の投票処理が行われます。\n"
                "投票内容を十分に確認してから実行してください。"
            )

        # Build race_date string for ipatgo CSV filename
        race_date_str = f"{race_info['Year']}{race_info['MonthDay']}"

        if st.button(
            f"投票を実行 ({exec_method})",
            type="primary",
            key="btn_execute_bets",
        ):
            executor = BetExecutor(
                ext_db=ext_db,
                method=exec_method,
                approval_required=False,
                csv_output_dir=betting_config.get("csv_output_dir", "./data/ipatgo"),
            )

            with st.spinner(f"投票実行中... ({exec_method})"):
                exec_results = executor.execute_bets(
                    bets=bet_candidates,
                    race_date=race_date_str,
                )

            success_count = sum(
                1 for r in exec_results if r.status in ("EXECUTED", "DRYRUN")
            )
            fail_count = sum(1 for r in exec_results if r.status == "FAILED")

            if fail_count == 0:
                st.success(
                    f"投票完了: {success_count}件 ({exec_method}) / "
                    f"合計投票額: {total_bet:,}円"
                )
            else:
                st.warning(f"投票完了: 成功{success_count}件 / 失敗{fail_count}件")

            for er in exec_results:
                status_icon = "✓" if er.status in ("EXECUTED", "DRYRUN") else "✗"
                st.text(
                    f"  {status_icon} [{er.status}] 馬番{er.selection} "
                    f"{er.stake_yen:,}円 (odds={er.odds_at_bet:.1f})"
                )

            if exec_method == "ipatgo":
                csv_dir = betting_config.get("csv_output_dir", "./data/ipatgo")
                st.info(f"ipatgo CSV出力先: `{csv_dir}`")
