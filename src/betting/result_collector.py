"""レース結果収集・ベット照合モジュール。

NL_HR_PAY（払戻テーブル）とbetsテーブルを照合し、
投票結果（WIN/LOSE）と払戻金を確定する。
"""

from datetime import UTC, datetime
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider


class ResultCollector:
    """レース結果収集・照合クラス。

    JVLink DBの払戻データとbetsテーブルを照合し、
    各投票の結果を確定・更新する。
    """

    def __init__(
        self,
        jvlink_db: DatabaseManager,
        ext_db: DatabaseManager,
    ) -> None:
        self._jvlink_db = jvlink_db
        self._ext_db = ext_db
        self._provider = JVLinkDataProvider(jvlink_db)

    def collect_results(self, race_key: str) -> dict[str, Any]:
        """指定レースの結果を収集する。

        Args:
            race_key: 16桁のレースキー

        Returns:
            {"race_key", "payouts", "kakutei_jyuni"} のdict
        """
        payouts = self._provider.get_payouts(race_key)

        # 確定着順も取得
        entries = self._provider.get_race_entries(race_key)
        kakutei = {}
        for e in entries:
            umaban = e.get("Umaban", "")
            if not umaban or not umaban.strip():
                continue
            jyuni = e.get("KakuteiJyuni", "0")
            kakutei[umaban] = int(jyuni) if jyuni and jyuni != "0" else 0

        return {
            "race_key": race_key,
            "payouts": payouts,
            "kakutei_jyuni": kakutei,
        }

    def reconcile_bets(self, race_key: str) -> list[dict[str, Any]]:
        """指定レースのベットを払戻データと照合する。

        betsテーブルのPENDING/EXECUTED投票を検索し、
        払戻データと照合して結果を更新する。

        Args:
            race_key: レースキー

        Returns:
            更新されたベット情報のリスト
        """
        if not self._ext_db.table_exists("bets"):
            logger.warning("betsテーブルが存在しません")
            return []

        # 対象ベット取得
        pending_bets = self._ext_db.execute_query(
            """SELECT bet_id, race_key, bet_type, selection, stake_yen, odds_at_bet
               FROM bets
               WHERE race_key = ? AND status IN ('EXECUTED', 'DRYRUN')
               AND result IS NULL""",
            (race_key,),
        )

        if not pending_bets:
            logger.info(f"照合対象ベットなし: {race_key}")
            return []

        # 結果収集
        race_result = self.collect_results(race_key)
        payouts = race_result["payouts"]
        kakutei = race_result["kakutei_jyuni"]

        now = datetime.now(UTC).isoformat()
        updated = []

        for bet in pending_bets:
            selection = bet["selection"]
            bet_type = bet["bet_type"]
            stake = bet["stake_yen"]

            # 的中判定
            payout_yen = self._calculate_payout(
                bet_type, selection, stake, payouts, kakutei
            )

            if payout_yen > 0:
                result = "WIN"
            elif kakutei:
                result = "LOSE"
            else:
                # まだ確定着順がない場合はスキップ
                continue

            # DB更新
            try:
                self._ext_db.execute_write(
                    """UPDATE bets
                       SET result = ?, payout_yen = ?, settled_at = ?,
                           status = 'SETTLED'
                       WHERE bet_id = ?""",
                    (result, payout_yen, now, bet["bet_id"]),
                )
                updated.append({
                    "bet_id": bet["bet_id"],
                    "selection": selection,
                    "result": result,
                    "payout_yen": payout_yen,
                })
                logger.info(
                    f"ベット照合: ID={bet['bet_id']} 馬番{selection} "
                    f"→ {result} 払戻={payout_yen:,}円"
                )
            except Exception as e:
                logger.error(f"ベット更新エラー: {e}")

        return updated

    def reconcile_all_pending(self) -> int:
        """全未照合ベットを一括照合する。

        Returns:
            照合されたベット数
        """
        if not self._ext_db.table_exists("bets"):
            return 0

        # 未照合のレースキーを取得
        rows = self._ext_db.execute_query(
            """SELECT DISTINCT race_key FROM bets
               WHERE status IN ('EXECUTED', 'DRYRUN')
               AND result IS NULL"""
        )
        if not rows:
            logger.info("未照合ベットなし")
            return 0

        total_updated = 0
        for row in rows:
            race_key = row["race_key"]
            updated = self.reconcile_bets(race_key)
            total_updated += len(updated)

        logger.info(f"一括照合完了: {total_updated}件更新")
        return total_updated

    def write_daily_snapshot(
        self, date: str, initial_bankroll: int = 1_000_000
    ) -> bool:
        """当日の収支スナップショットを bankroll_log に書き込む。

        bets テーブルの settled_at が date に一致する行を集計し、
        bankroll_log に UPSERT する。

        Args:
            date: 対象日 YYYY-MM-DD 形式
            initial_bankroll: 前日残高が存在しない場合の初期残高

        Returns:
            書き込み成功なら True
        """
        if not self._ext_db.table_exists("bets"):
            logger.warning("betsテーブルが存在しないためスナップショットをスキップ")
            return False
        if not self._ext_db.table_exists("bankroll_log"):
            logger.warning("bankroll_logテーブルが存在しないためスナップショットをスキップ")
            return False

        # 当日の決済済みベットを集計
        rows = self._ext_db.execute_query(
            """SELECT COALESCE(SUM(stake_yen), 0) AS total_stake,
                      COALESCE(SUM(payout_yen), 0) AS total_payout
               FROM bets
               WHERE settled_at LIKE ? AND status = 'SETTLED'""",
            (f"{date}%",),
        )
        if not rows:
            return False

        total_stake = rows[0]["total_stake"]
        total_payout = rows[0]["total_payout"]

        if total_stake == 0 and total_payout == 0:
            logger.info(f"bankroll_log: {date} の決済済みベットなし — スキップ")
            return False

        pnl = total_payout - total_stake

        # 前日の closing_balance を opening_balance として使用
        prev = self._ext_db.execute_query(
            "SELECT closing_balance FROM bankroll_log ORDER BY date DESC LIMIT 1"
        )
        opening_balance = prev[0]["closing_balance"] if prev else initial_bankroll
        closing_balance = opening_balance + pnl
        roi = pnl / total_stake if total_stake > 0 else 0.0

        # UPSERT: 同一日が既存なら更新、なければ挿入
        existing = self._ext_db.execute_query(
            "SELECT log_id FROM bankroll_log WHERE date = ?", (date,)
        )
        if existing:
            self._ext_db.execute_write(
                """UPDATE bankroll_log
                   SET opening_balance = ?, total_stake = ?, total_payout = ?,
                       closing_balance = ?, pnl = ?, roi = ?
                   WHERE date = ?""",
                (opening_balance, total_stake, total_payout,
                 closing_balance, pnl, roi, date),
            )
        else:
            self._ext_db.execute_write(
                """INSERT INTO bankroll_log
                   (date, opening_balance, total_stake, total_payout,
                    closing_balance, pnl, roi)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (date, opening_balance, total_stake, total_payout,
                 closing_balance, pnl, roi),
            )

        logger.info(
            f"bankroll_log: {date} PnL={pnl:+,}円 "
            f"残高={closing_balance:,}円 ROI={roi:+.1%}"
        )
        return True

    @staticmethod
    def _calculate_payout(
        bet_type: str,
        selection: str,
        stake: int,
        payouts: dict[str, Any],
        kakutei: dict[str, int],
    ) -> int:
        """払戻金額を計算する。

        Args:
            bet_type: 券種（WIN/PLACE等）
            selection: 馬番
            stake: 投票額
            payouts: 払戻データ（provider.get_payouts()の戻り値）
            kakutei: 確定着順マップ

        Returns:
            払戻金額（円）。不的中の場合は0。
        """
        jyuni = kakutei.get(selection, 0)

        if bet_type == "WIN":
            # 単勝: 1着のみ的中
            if jyuni == 1:
                # provider.get_payouts()は {"tansyo": [{"selection", "pay", "ninki"}]} を返す
                tansyo = payouts.get("tansyo", [])
                for pay in tansyo:
                    if isinstance(pay, dict) and pay.get("selection") == selection:
                        # 100円あたりの払戻 × (投票額 / 100)
                        return int(pay.get("pay", 0)) * (stake // 100)
                return 0
            return 0

        elif bet_type == "PLACE":
            # 複勝: 3着以内で的中
            if 1 <= jyuni <= 3:
                fukusyo = payouts.get("fukusyo", [])
                for pay in fukusyo:
                    if isinstance(pay, dict) and pay.get("selection") == selection:
                        return int(pay.get("pay", 0)) * (stake // 100)
                return 0
            return 0

        # その他の券種は将来拡張
        return 0
