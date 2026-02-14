"""ダッシュボード各ページ用の日付デフォルト値。

競馬分析のベストプラクティスに基づく推奨値:
- ファクター分析: 過去1年（十分なサンプル数を確保）
- バックテスト: 過去6ヶ月（直近のパフォーマンスを評価）
- Walk-Forward: 過去2年（十分な訓練/テスト区間を確保）
"""

from datetime import date, timedelta


def factor_analysis_defaults() -> tuple[str, str, int]:
    """ファクター分析のデフォルト値を返す。

    Returns:
        (date_from, date_to, max_races)
        - date_from: 1年前 "YYYYMMDD"
        - date_to: 本日 "YYYYMMDD"
        - max_races: 2000
    """
    today = date.today()
    d_from = (today - timedelta(days=365)).strftime("%Y%m%d")
    d_to = today.strftime("%Y%m%d")
    return d_from, d_to, 2000


def backtest_defaults() -> tuple[date, date]:
    """バックテストのデフォルト日付範囲を返す。

    Returns:
        (date_from, date_to) — 過去6ヶ月
    """
    today = date.today()
    return today - timedelta(days=180), today


def walk_forward_defaults() -> tuple[str, str, int]:
    """Walk-Forwardのデフォルト値を返す。

    Returns:
        (date_from, date_to, n_windows)
        - date_from: 2年前 "YYYYMMDD"
        - date_to: 本日 "YYYYMMDD"
        - n_windows: 5
    """
    today = date.today()
    d_from = (today - timedelta(days=730)).strftime("%Y%m%d")
    d_to = today.strftime("%Y%m%d")
    return d_from, d_to, 5
