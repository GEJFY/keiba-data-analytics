"""税務レポート生成モジュール。

競馬の払戻金に対する所得税計算と確定申告用サマリーを生成する。

日本の税法上の扱い:
    - 一般の競馬払戻金は「一時所得」
    - 一時所得 = 総収入金額 - その収入を得るために支出した金額 - 特別控除額(50万円)
    - 課税対象 = 一時所得 × 1/2
    - ※的中投票の購入費のみ経費算入可能（ハズレ馬券は経費不算入）
"""

from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager


@dataclass
class MonthlyBreakdown:
    """月次内訳。"""

    month: str  # YYYY-MM
    total_stake: int = 0
    total_payout: int = 0
    winning_stake: int = 0  # 的中分のみの購入費
    pnl: int = 0
    n_bets: int = 0
    n_wins: int = 0


@dataclass
class TaxReport:
    """年次税務レポート。"""

    year: int
    total_stake: int = 0
    total_payout: int = 0
    winning_stake: int = 0  # 的中分のみの購入費（経費算入可能額）
    gross_income: int = 0  # 総収入金額（払戻合計）
    deductible_expense: int = 0  # 控除可能経費（的中分の購入費）
    special_deduction: int = 500_000  # 特別控除（50万円）
    ichiji_shotoku: int = 0  # 一時所得
    taxable_amount: int = 0  # 課税対象額（一時所得 × 1/2）
    n_bets: int = 0
    n_wins: int = 0
    monthly_breakdown: list[MonthlyBreakdown] = field(default_factory=list)
    top_payouts: list[dict[str, Any]] = field(default_factory=list)


class TaxReportGenerator:
    """税務レポート生成器。

    betsテーブルの照合済みデータから年次の税務サマリーを生成する。
    """

    def __init__(self, ext_db: DatabaseManager) -> None:
        self._db = ext_db

    def generate(self, year: int) -> TaxReport:
        """指定年の税務レポートを生成する。

        Args:
            year: 対象年

        Returns:
            TaxReport
        """
        report = TaxReport(year=year)

        if not self._db.table_exists("bets"):
            logger.warning("betsテーブルが存在しません")
            return report

        # 対象年の照合済みベットを取得
        bets = self._db.execute_query(
            """SELECT race_key, bet_type, selection, stake_yen,
                      odds_at_bet, status, result, payout_yen, settled_at
               FROM bets
               WHERE settled_at IS NOT NULL
                 AND settled_at LIKE ?
               ORDER BY settled_at""",
            (f"{year}%",),
        )

        if not bets:
            logger.info(f"{year}年の照合済みベットがありません")
            return report

        monthly: dict[str, MonthlyBreakdown] = {}

        for bet in bets:
            stake = bet.get("stake_yen", 0)
            payout = bet.get("payout_yen", 0)
            is_win = bet.get("result") == "WIN"
            settled = bet.get("settled_at", "")

            # 月次集計
            month_key = settled[:7] if len(settled) >= 7 else f"{year}-01"
            if month_key not in monthly:
                monthly[month_key] = MonthlyBreakdown(month=month_key)
            m = monthly[month_key]
            m.total_stake += stake
            m.total_payout += payout
            m.pnl += payout - stake
            m.n_bets += 1
            if is_win:
                m.winning_stake += stake
                m.n_wins += 1

            # 年次集計
            report.total_stake += stake
            report.total_payout += payout
            report.n_bets += 1
            if is_win:
                report.winning_stake += stake
                report.n_wins += 1

            # 高額払戻追跡
            if payout > 0:
                report.top_payouts.append({
                    "race_key": bet.get("race_key", ""),
                    "bet_type": bet.get("bet_type", ""),
                    "selection": bet.get("selection", ""),
                    "stake": stake,
                    "payout": payout,
                    "profit": payout - stake,
                    "date": settled[:10] if settled else "",
                })

        # 高額払戻を降順ソート（上位10件）
        report.top_payouts.sort(key=lambda x: x["payout"], reverse=True)
        report.top_payouts = report.top_payouts[:10]

        # 月次集計をソートして格納
        report.monthly_breakdown = sorted(monthly.values(), key=lambda m: m.month)

        # 一時所得計算
        report.gross_income = report.total_payout
        report.deductible_expense = report.winning_stake
        report.ichiji_shotoku = max(
            0,
            report.gross_income
            - report.deductible_expense
            - report.special_deduction,
        )
        report.taxable_amount = report.ichiji_shotoku // 2

        logger.info(
            f"税務レポート生成完了: {year}年, "
            f"総払戻={report.total_payout:,}円, "
            f"一時所得={report.ichiji_shotoku:,}円, "
            f"課税対象={report.taxable_amount:,}円"
        )

        return report

    def format_summary(self, report: TaxReport) -> str:
        """税務レポートをテキストサマリーに整形する。

        Args:
            report: TaxReport

        Returns:
            整形済みテキスト
        """
        lines = [
            f"=== {report.year}年 競馬収支 税務サマリー ===",
            "",
            f"投票総額:     {report.total_stake:>12,}円",
            f"払戻総額:     {report.total_payout:>12,}円",
            f"収支:         {report.total_payout - report.total_stake:>+12,}円",
            "",
            f"投票回数:     {report.n_bets:>12,}回",
            f"的中回数:     {report.n_wins:>12,}回",
            f"的中率:       {report.n_wins / max(report.n_bets, 1):>11.1%}",
            "",
            "--- 一時所得計算 ---",
            f"総収入金額（払戻合計）:         {report.gross_income:>12,}円",
            f"控除可能経費（的中分購入費）:   {report.deductible_expense:>12,}円",
            f"特別控除額:                     {report.special_deduction:>12,}円",
            f"一時所得:                       {report.ichiji_shotoku:>12,}円",
            f"課税対象額（一時所得 × 1/2）:  {report.taxable_amount:>12,}円",
            "",
        ]

        if report.monthly_breakdown:
            lines.append("--- 月次内訳 ---")
            lines.append(f"{'月':>8}  {'投票額':>10}  {'払戻額':>10}  {'収支':>10}  {'的中':>4}")
            for m in report.monthly_breakdown:
                lines.append(
                    f"{m.month:>8}  {m.total_stake:>10,}  "
                    f"{m.total_payout:>10,}  {m.pnl:>+10,}  "
                    f"{m.n_wins:>4}"
                )
            lines.append("")

        if report.top_payouts:
            lines.append("--- 高額払戻 Top10 ---")
            for i, p in enumerate(report.top_payouts, 1):
                lines.append(
                    f"  {i:>2}. {p['date']} {p['race_key']} "
                    f"馬番{p['selection']} 払戻{p['payout']:,}円 "
                    f"(利益{p['profit']:+,}円)"
                )

        lines.append("")
        lines.append("※ハズレ馬券の購入費は一時所得の経費に算入できません。")
        lines.append("※確定申告が必要かどうかは税理士にご相談ください。")

        return "\n".join(lines)
