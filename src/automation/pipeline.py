"""レースデー自動化パイプライン。

JVLinkデータ同期 → 全レーススコアリング → バリューベット投票 → 結果照合 → 通知
の一連のフローを自動化するオーケストレータ。

Usage:
    pipeline = RaceDayPipeline(jvlink_db, ext_db, config)
    result = pipeline.run_full(target_date="20250215")
"""

import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger

from src.betting.executor import BetExecutor
from src.betting.result_collector import ResultCollector
from src.betting.safety import SafetyGuard
from src.data.db import DatabaseManager
from src.data.provider import JVLinkDataProvider
from src.notifications.notifier import NotificationConfig, Notifier
from src.strategy.plugins.gy_value import GYValueStrategy


@dataclass
class PipelineResult:
    """パイプライン実行結果。"""

    run_id: int = 0
    run_date: str = ""
    status: str = "RUNNING"
    sync_result: dict[str, Any] | None = None
    races_found: int = 0
    races_scored: int = 0
    total_bets: int = 0
    total_stake: int = 0
    reconciled: int = 0
    errors: list[str] = field(default_factory=list)
    started_at: str = ""
    completed_at: str = ""


# pipeline_runsテーブルDDL（_ensure_pipeline_tableで使用）
_PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    status TEXT DEFAULT 'RUNNING',
    sync_status TEXT,
    sync_records_added INTEGER DEFAULT 0,
    races_found INTEGER DEFAULT 0,
    races_scored INTEGER DEFAULT 0,
    total_bets INTEGER DEFAULT 0,
    total_stake INTEGER DEFAULT 0,
    reconciled INTEGER DEFAULT 0,
    errors TEXT DEFAULT '[]',
    started_at TEXT NOT NULL,
    completed_at TEXT
)
"""


class RaceDayPipeline:
    """レースデー自動化パイプライン。

    既存コンポーネント（GYValueStrategy, BetExecutor, SafetyGuard,
    ResultCollector, Notifier）をオーケストレーションし、
    レース日の一連の処理を自動化する。

    Args:
        jvlink_db: JVLink DB
        ext_db: 拡張DB
        config: config.yaml全体のdict
    """

    def __init__(
        self,
        jvlink_db: DatabaseManager,
        ext_db: DatabaseManager,
        config: dict[str, Any],
    ) -> None:
        self._jvlink_db = jvlink_db
        self._ext_db = ext_db
        self._config = config

        # 通知設定構築
        notif_cfg = config.get("notification", {})
        self._notifier = Notifier(NotificationConfig(
            slack_webhook_url=notif_cfg.get("slack_webhook_url", ""),
            slack_channel=notif_cfg.get("slack_channel", ""),
            smtp_host=notif_cfg.get("smtp_host", ""),
            smtp_port=notif_cfg.get("smtp_port", 587),
            smtp_user=notif_cfg.get("smtp_user", ""),
            smtp_password=notif_cfg.get("smtp_password", ""),
            email_from=notif_cfg.get("email_from", ""),
            email_to=notif_cfg.get("email_to", []),
            min_level=notif_cfg.get("min_level", "INFO"),
        ))

        # 安全機構
        betting_cfg = config.get("betting", {})
        self._safety = SafetyGuard(
            max_consecutive_losses=betting_cfg.get("max_consecutive_losses", 20),
        )

    def run_full(self, target_date: str = "") -> PipelineResult:
        """全ステップを順番に実行する。

        Args:
            target_date: 対象日 YYYYMMDD（空の場合は当日）

        Returns:
            PipelineResult
        """
        if not target_date:
            target_date = datetime.now().strftime("%Y%m%d")

        auto_cfg = self._config.get("automation", {})
        if not auto_cfg.get("enabled", False):
            logger.warning(
                "自動化が無効です (automation.enabled=false)。"
                "config.yamlで有効化してください。dryrunモードで続行します。"
            )
            self._config.setdefault("betting", {})["method"] = "dryrun"

        self._ensure_pipeline_table()
        now = datetime.now(timezone.utc).isoformat()

        result = PipelineResult(
            run_date=target_date,
            started_at=now,
        )
        result.run_id = self._create_run_record(target_date, now)

        logger.info(f"パイプライン開始: run_id={result.run_id}, date={target_date}")

        # Step 1: データ同期
        try:
            sync_result = self.step_sync()
            result.sync_result = sync_result
            logger.info(f"データ同期完了: {sync_result.get('status', 'UNKNOWN')}")
        except Exception as e:
            err_msg = f"データ同期エラー: {e}"
            logger.error(err_msg)
            result.errors.append(err_msg)

        # Step 2: スコアリング + 投票
        try:
            score_result = self.step_score_and_bet(target_date)
            result.races_found = score_result.get("races_found", 0)
            result.races_scored = score_result.get("races_scored", 0)
            result.total_bets = score_result.get("total_bets", 0)
            result.total_stake = score_result.get("total_stake", 0)
            logger.info(
                f"スコアリング完了: {result.races_found}レース, "
                f"{result.total_bets}ベット, {result.total_stake:,}円"
            )
        except Exception as e:
            err_msg = f"スコアリングエラー: {e}"
            logger.error(err_msg)
            result.errors.append(err_msg)

        # Step 3: 結果照合
        auto_reconcile = auto_cfg.get("auto_reconcile", True)
        if auto_reconcile:
            try:
                reconcile_result = self.step_reconcile()
                result.reconciled = reconcile_result.get("reconciled", 0)
                logger.info(f"結果照合完了: {result.reconciled}件")
            except Exception as e:
                err_msg = f"結果照合エラー: {e}"
                logger.error(err_msg)
                result.errors.append(err_msg)

        # ステータス決定
        result.completed_at = datetime.now(timezone.utc).isoformat()
        if not result.errors:
            result.status = "SUCCESS"
        elif result.total_bets > 0:
            result.status = "PARTIAL"
        else:
            result.status = "FAILED"

        self._update_run_record(result.run_id, result)

        # Step 4: 通知
        try:
            self.step_notify(result)
        except Exception as e:
            logger.error(f"通知エラー: {e}")

        logger.info(
            f"パイプライン完了: status={result.status}, "
            f"bets={result.total_bets}, stake={result.total_stake:,}円"
        )
        return result

    def step_sync(self) -> dict[str, Any]:
        """JVLinkデータ同期を実行する。

        JVLinkToSQLite.exeをsubprocess.runで実行し、結果を返す。
        exe_pathが未設定または存在しない場合はスキップする。

        Returns:
            {"status": str, "exit_code": int, "records_added": int}
        """
        jvlink_cfg = self._config.get("jvlink", {})
        exe_path = jvlink_cfg.get("exe_path", "")
        timeout = jvlink_cfg.get("sync_timeout_sec", 3600)
        retry_count = jvlink_cfg.get("retry_count", 3)

        if not exe_path:
            logger.info("jvlink.exe_path未設定のためデータ同期をスキップ")
            return {"status": "SKIPPED", "exit_code": -1, "records_added": 0}

        # プロジェクトルートからの相対パス解決
        project_root = Path(__file__).resolve().parent.parent.parent
        exe_resolved = (project_root / exe_path).resolve()

        if not exe_resolved.exists():
            logger.warning(f"JVLinkToSQLite.exe が見つかりません: {exe_resolved}")
            return {"status": "SKIPPED", "exit_code": -1, "records_added": 0}

        db_path = str(Path(
            self._config.get("database", {}).get("jvlink_db_path", "./data/jvlink.db")
        ))
        db_resolved = str((project_root / db_path).resolve())

        # リトライ付き実行
        last_exit_code = -1
        for attempt in range(1, retry_count + 1):
            logger.info(f"JVLink同期 試行 {attempt}/{retry_count}")
            try:
                proc = subprocess.run(
                    [str(exe_resolved), "-m", "Exec", "-d", db_resolved, "-l", "Info"],
                    cwd=str(exe_resolved.parent),
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                )
                last_exit_code = proc.returncode
                if proc.returncode == 0:
                    logger.info("JVLink同期成功")
                    return {"status": "SUCCESS", "exit_code": 0, "records_added": 0}
                logger.warning(
                    f"JVLink同期失敗 (exit={proc.returncode}): {proc.stderr[:500]}"
                )
            except subprocess.TimeoutExpired:
                logger.error(f"JVLink同期タイムアウト ({timeout}秒)")
                last_exit_code = -2
            except Exception as e:
                logger.error(f"JVLink同期例外: {e}")
                last_exit_code = -3

        return {"status": "FAILED", "exit_code": last_exit_code, "records_added": 0}

    def step_score_and_bet(self, target_date: str) -> dict[str, Any]:
        """当日全レースをスコアリングし投票する。

        Args:
            target_date: 対象日 YYYYMMDD

        Returns:
            {"races_found", "races_scored", "total_bets", "total_stake"}
        """
        provider = JVLinkDataProvider(self._jvlink_db)

        # 当日レースを一括取得（バッチクエリで高速化）
        auto_cfg = self._config.get("automation", {})
        max_races = auto_cfg.get("max_races_per_day", 36)

        with self._jvlink_db.session():
            batch_races = provider.fetch_races_batch(
                date_from=target_date,
                date_to=target_date,
                max_races=max_races,
                include_payouts=False,
            )

        if not batch_races:
            logger.info(f"当日レースなし: {target_date}")
            return {
                "races_found": 0, "races_scored": 0,
                "total_bets": 0, "total_stake": 0,
            }

        # 戦略・投票準備
        betting_cfg = self._config.get("betting", {})
        bankroll_cfg = self._config.get("bankroll", {})
        scoring_cfg = self._config.get("scoring", {})

        strategy = GYValueStrategy(
            self._ext_db,
            jvlink_db=self._jvlink_db,
            ev_threshold=scoring_cfg.get("ev_threshold", 1.05),
        )
        executor = BetExecutor(
            ext_db=self._ext_db,
            method=betting_cfg.get("method", "dryrun"),
            approval_required=False,
            csv_output_dir=betting_cfg.get("csv_output_dir", "./data/ipatgo"),
        )
        bankroll = bankroll_cfg.get("initial_balance", 1_000_000)

        all_bets = []
        races_scored = 0

        for race_data in batch_races:
            race_info = race_data["race_info"]
            entries = race_data["entries"]
            odds_map = race_data["odds"]

            if not entries:
                continue

            races_scored += 1

            # 安全チェック
            can_bet, reason = self._safety.check_can_bet()
            if not can_bet:
                logger.warning(f"安全チェック不合格: {reason} — 投票を中止")
                break

            # 戦略実行
            bets = strategy.run(race_info, entries, odds_map, bankroll, {})
            if not bets:
                continue

            # 重複チェック
            unique_bets = []
            for bet in bets:
                if not self._safety.check_duplicate_bet(bet.race_key, bet.selection):
                    unique_bets.append(bet)
                    self._safety.register_bet(bet.race_key, bet.selection)

            if unique_bets:
                race_date = f"{race_info.get('Year', '')}{race_info.get('MonthDay', '')}"
                executor.execute_bets(unique_bets, race_date)
                all_bets.extend(unique_bets)

        return {
            "races_found": len(batch_races),
            "races_scored": races_scored,
            "total_bets": len(all_bets),
            "total_stake": sum(b.stake_yen for b in all_bets),
        }

    def step_reconcile(self) -> dict[str, Any]:
        """未照合ベットを一括照合する。

        Returns:
            {"reconciled": int}
        """
        collector = ResultCollector(self._jvlink_db, self._ext_db)
        count = collector.reconcile_all_pending()
        return {"reconciled": count}

    def step_notify(self, result: PipelineResult) -> None:
        """パイプライン実行結果を通知する。"""
        status_emoji = {
            "SUCCESS": "OK",
            "PARTIAL": "一部エラー",
            "FAILED": "失敗",
        }
        title = f"パイプライン実行: {result.run_date} — {status_emoji.get(result.status, result.status)}"
        lines = [
            f"実行日: {result.run_date}",
            f"ステータス: {result.status}",
            f"レース数: {result.races_found}",
            f"投票数: {result.total_bets}",
            f"合計投票額: {result.total_stake:,}円",
            f"照合数: {result.reconciled}",
        ]
        if result.errors:
            lines.append(f"エラー: {len(result.errors)}件")
            for err in result.errors[:3]:
                lines.append(f"  - {err[:100]}")

        level = "INFO" if result.status == "SUCCESS" else "WARNING"
        self._notifier.send(title, "\n".join(lines), level)

    # --- DB操作ヘルパー ---

    def _ensure_pipeline_table(self) -> None:
        """pipeline_runsテーブルが存在しなければ作成する。"""
        if not self._ext_db.table_exists("pipeline_runs"):
            with self._ext_db.connect() as conn:
                conn.execute(_PIPELINE_RUNS_DDL)

    def _create_run_record(self, target_date: str, started_at: str) -> int:
        """実行記録を作成しrun_idを返す。"""
        self._ext_db.execute_write(
            """INSERT INTO pipeline_runs (run_date, status, started_at)
               VALUES (?, 'RUNNING', ?)""",
            (target_date, started_at),
        )
        rows = self._ext_db.execute_query(
            "SELECT MAX(run_id) AS run_id FROM pipeline_runs"
        )
        return rows[0]["run_id"] if rows else 0

    def _update_run_record(self, run_id: int, result: PipelineResult) -> None:
        """実行記録を更新する。"""
        self._ext_db.execute_write(
            """UPDATE pipeline_runs SET
                status = ?, sync_status = ?, sync_records_added = ?,
                races_found = ?, races_scored = ?,
                total_bets = ?, total_stake = ?, reconciled = ?,
                errors = ?, completed_at = ?
               WHERE run_id = ?""",
            (
                result.status,
                result.sync_result.get("status") if result.sync_result else None,
                result.sync_result.get("records_added", 0) if result.sync_result else 0,
                result.races_found,
                result.races_scored,
                result.total_bets,
                result.total_stake,
                result.reconciled,
                json.dumps(result.errors, ensure_ascii=False),
                result.completed_at,
                run_id,
            ),
        )
