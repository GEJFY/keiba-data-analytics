"""投票実行モジュール。

実行方式:
    - dryrun: ログ出力のみ（デフォルト、本番前テスト用）
    - ipatgo: CSV出力 → 外部ツール連携
    - selenium: ブラウザ自動操作（将来拡張）

全方式で共通のBetExecutionResult を返す。
"""

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from loguru import logger

from src.data.db import DatabaseManager
from src.strategy.base import Bet


@dataclass
class BetExecutionResult:
    """投票実行結果。"""

    race_key: str
    selection: str
    bet_type: str
    stake_yen: int
    odds_at_bet: float
    est_ev: float
    status: str  # EXECUTED / DRYRUN / FAILED
    executed_at: str = ""
    error_message: str = ""


class BetExecutor:
    """投票エグゼキュータ。

    投票指示(Bet)リストを受け取り、指定方式で実行する。
    全投票は bets テーブルに記録される。
    """

    VALID_METHODS = ("dryrun", "ipatgo", "selenium")

    def __init__(
        self,
        ext_db: DatabaseManager,
        method: str = "dryrun",
        approval_required: bool = True,
        csv_output_dir: str = "./data/ipatgo",
    ) -> None:
        """
        Args:
            ext_db: 拡張DB（betsテーブルへの書き込み用）
            method: 投票方式 (dryrun / ipatgo / selenium)
            approval_required: 実行前に確認を要求するか
            csv_output_dir: ipatgo方式のCSV出力先
        """
        if method not in self.VALID_METHODS:
            raise ValueError(f"無効な投票方式: {method} (有効: {self.VALID_METHODS})")
        self._db = ext_db
        self._method = method
        self._approval_required = approval_required
        self._csv_output_dir = Path(csv_output_dir)

    @property
    def method(self) -> str:
        return self._method

    def execute_bets(
        self,
        bets: list[Bet],
        race_date: str = "",
    ) -> list[BetExecutionResult]:
        """投票指示リストを実行する。

        Args:
            bets: 投票指示リスト
            race_date: レース日（YYYY-MM-DD）、ipatgo CSV名に使用

        Returns:
            各投票の実行結果リスト
        """
        if not bets:
            logger.info("投票対象なし")
            return []

        logger.info(
            f"投票実行開始: {len(bets)}件, 方式={self._method}, "
            f"合計={sum(b.stake_yen for b in bets):,}円"
        )

        if self._method == "dryrun":
            results = self._execute_dryrun(bets)
        elif self._method == "ipatgo":
            results = self._execute_ipatgo(bets, race_date)
        elif self._method == "selenium":
            results = self._execute_selenium(bets)
        else:
            results = []

        # DBに記録
        self._record_to_db(bets, results)

        executed = sum(1 for r in results if r.status in ("EXECUTED", "DRYRUN"))
        failed = sum(1 for r in results if r.status == "FAILED")
        logger.info(f"投票実行完了: 成功={executed}件, 失敗={failed}件")

        return results

    def _execute_dryrun(self, bets: list[Bet]) -> list[BetExecutionResult]:
        """ドライラン方式: ログ出力のみ。"""
        now = datetime.now(UTC).isoformat()
        results = []
        for bet in bets:
            logger.info(
                f"[DRYRUN] {bet.race_key} 馬番{bet.selection} "
                f"{bet.bet_type} {bet.stake_yen:,}円 "
                f"(odds={bet.odds_at_bet:.1f}, EV={bet.est_ev:.3f})"
            )
            results.append(BetExecutionResult(
                race_key=bet.race_key,
                selection=bet.selection,
                bet_type=bet.bet_type,
                stake_yen=bet.stake_yen,
                odds_at_bet=bet.odds_at_bet,
                est_ev=bet.est_ev,
                status="DRYRUN",
                executed_at=now,
            ))
        return results

    def _execute_ipatgo(
        self, bets: list[Bet], race_date: str
    ) -> list[BetExecutionResult]:
        """ipatgo方式: CSV出力。

        ipatgo互換のCSVフォーマットで出力し、
        外部ツールでIPATへ投票する。
        """
        now = datetime.now(UTC).isoformat()
        self._csv_output_dir.mkdir(parents=True, exist_ok=True)

        date_str = race_date.replace("-", "") or datetime.now().strftime("%Y%m%d")
        csv_path = self._csv_output_dir / f"bets_{date_str}.csv"

        results = []
        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "race_key", "bet_type", "selection", "stake_yen",
                    "odds", "est_ev",
                ])
                for bet in bets:
                    writer.writerow([
                        bet.race_key, bet.bet_type, bet.selection,
                        bet.stake_yen, bet.odds_at_bet, f"{bet.est_ev:.4f}",
                    ])
                    results.append(BetExecutionResult(
                        race_key=bet.race_key,
                        selection=bet.selection,
                        bet_type=bet.bet_type,
                        stake_yen=bet.stake_yen,
                        odds_at_bet=bet.odds_at_bet,
                        est_ev=bet.est_ev,
                        status="EXECUTED",
                        executed_at=now,
                    ))

            logger.info(f"ipatgo CSV出力完了: {csv_path} ({len(bets)}件)")
        except OSError as e:
            logger.error(f"ipatgo CSV出力エラー: {e}")
            for bet in bets:
                results.append(BetExecutionResult(
                    race_key=bet.race_key,
                    selection=bet.selection,
                    bet_type=bet.bet_type,
                    stake_yen=bet.stake_yen,
                    odds_at_bet=bet.odds_at_bet,
                    est_ev=bet.est_ev,
                    status="FAILED",
                    executed_at=now,
                    error_message=str(e),
                ))

        return results

    def _execute_selenium(self, bets: list[Bet]) -> list[BetExecutionResult]:
        """Selenium方式: 未実装（将来拡張）。

        現時点ではFAILED結果を返す。
        Selenium WebDriverによるIPAT自動操作は
        セキュリティ上の理由から手動拡張を前提とする。
        """
        now = datetime.now(UTC).isoformat()
        results = []
        for bet in bets:
            results.append(BetExecutionResult(
                race_key=bet.race_key,
                selection=bet.selection,
                bet_type=bet.bet_type,
                stake_yen=bet.stake_yen,
                odds_at_bet=bet.odds_at_bet,
                est_ev=bet.est_ev,
                status="FAILED",
                executed_at=now,
                error_message="Selenium方式は未実装です。dryrunまたはipatgoを使用してください。",
            ))
        logger.warning("Selenium方式は未実装です")
        return results

    def _record_to_db(
        self, bets: list[Bet], results: list[BetExecutionResult]
    ) -> None:
        """投票結果をbetsテーブルに記録する。"""
        if not self._db.table_exists("bets"):
            logger.warning("betsテーブルが存在しません — 記録をスキップ")
            return

        for bet, result in zip(bets, results, strict=False):
            try:
                self._db.execute_write(
                    """INSERT INTO bets
                    (race_key, bet_type, selection, stake_yen,
                     est_prob, odds_at_bet, est_ev, status,
                     factor_details, executed_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        bet.race_key, bet.bet_type, bet.selection,
                        bet.stake_yen, bet.est_prob, bet.odds_at_bet,
                        bet.est_ev, result.status,
                        json.dumps(bet.factor_details, ensure_ascii=False),
                        result.executed_at,
                        result.executed_at,
                    ),
                )
            except Exception as e:
                logger.error(f"投票記録エラー: {e}")
