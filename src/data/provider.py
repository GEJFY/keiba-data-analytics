"""データプロバイダー — JVLinkToSQLite DBアダプタ。

JVLinkToSQLiteが生成する実テーブル（NL_RA_RACE, NL_SE_RACE_UMA等）への
アクセスを抽象化する。

アダプタ層として、実テーブルのidプレフィックス付きカラム名を
正規化された内部表現に変換して返す。

テーブル名マッピング:
    NL_RA_RACE           → レース情報
    NL_SE_RACE_UMA       → 馬毎レース情報
    NL_O1_ODDS_TANFUKUWAKU → 単複枠オッズ（1行=1レース、横持ち）
    NL_HR_PAY            → 払戻情報
    NL_UM_UMA            → 馬マスタ
    NL_KS_KISYU          → 騎手マスタ
"""

import contextlib
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from loguru import logger

from src.data.db import DatabaseManager

# 許可されるJVLinkオッズテーブル名のホワイトリスト
_ALLOWED_ODDS_TABLES = frozenset({
    "NL_O1_ODDS_TANFUKUWAKU",
    "NL_O2_ODDS_UMAREN",
    "NL_O3_ODDS_WIDE",
    "NL_O4_ODDS_UMATAN",
    "NL_O5_ODDS_SANREN",
    "NL_O6_ODDS_SANRENTAN",
})

# race_keyの正規表現パターン（16桁の数字文字列）
_RACE_KEY_PATTERN = re.compile(r"^\d{16}$")

# 実テーブルの主キーカラム名
_ID_COLUMNS = ["idYear", "idMonthDay", "idJyoCD", "idKaiji", "idNichiji", "idRaceNum"]


class JVLinkDataProvider:
    """JVLinkToSQLite DBからのデータ取得を提供するクラス。

    race_keyの構成:
        Year(4桁) + MonthDay(4桁) + JyoCD(2桁) + Kaiji(2桁) + Nichiji(2桁) + RaceNum(2桁)
        例: "2025010106010101" → 2025年1月1日 中山(06) 1回(01) 1日目(01) 1R(01)
    """

    # 下流コードとの互換性のため、出力キー名は旧名を維持
    RACE_KEY_COLUMNS = ["Year", "MonthDay", "JyoCD", "Kaiji", "Nichiji", "RaceNum"]

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    @staticmethod
    def build_race_key(row: dict[str, Any]) -> str:
        """行データからrace_keyを組み立てる。

        idプレフィックス付き・無し両方のキーに対応。

        Args:
            row: RACE_KEY_COLUMNSの各キーを含むdict

        Returns:
            16桁のrace_key文字列
        """
        parts = []
        for old_key, new_key in zip(
            JVLinkDataProvider.RACE_KEY_COLUMNS, _ID_COLUMNS, strict=True
        ):
            val = row.get(old_key) or row.get(new_key, "")
            parts.append(str(val))
        return "".join(parts)

    @staticmethod
    def _parse_race_key(race_key: str) -> tuple[str, str, str, str, str, str] | None:
        """race_keyを構成要素に分解する。

        Returns:
            (year, month_day, jyo_cd, kaiji, nichiji, race_num) または不正キーの場合None
        """
        if not _RACE_KEY_PATTERN.match(race_key):
            logger.debug(f"不正なrace_keyフォーマット: '{race_key}'")
            return None
        return (
            race_key[0:4],   # Year
            race_key[4:8],   # MonthDay
            race_key[8:10],  # JyoCD
            race_key[10:12], # Kaiji
            race_key[12:14], # Nichiji
            race_key[14:16], # RaceNum
        )

    def get_race_info(self, race_key: str) -> dict[str, Any] | None:
        """レース情報（NL_RA_RACE）を取得する。

        idプレフィックス付きカラムを正規化名に変換して返す。

        Args:
            race_key: 16桁のrace_key

        Returns:
            レース情報のdict（正規化済みキー）。該当なしまたは不正キーの場合None。
        """
        parts = self._parse_race_key(race_key)
        if parts is None:
            return None

        results = self._db.execute_query(
            """
            SELECT
                idYear AS Year, idMonthDay AS MonthDay,
                idJyoCD AS JyoCD, idKaiji AS Kaiji,
                idNichiji AS Nichiji, idRaceNum AS RaceNum,
                RaceInfoHondai AS RaceName,
                Kyori, TrackCD,
                TenkoBabaTenkoCD AS TenkoCD,
                TenkoBabaSibaBabaCD AS SibaBabaCD,
                TenkoBabaDirtBabaCD AS DirtBabaCD,
                JyokenInfoSyubetuCD AS SyubetuCD,
                GradeCD,
                HassoTime,
                TorokuTosu, SyussoTosu, NyusenTosu,
                HaronTimeL3, HaronTimeL4
            FROM NL_RA_RACE
            WHERE idYear = ? AND idMonthDay = ? AND idJyoCD = ?
              AND idKaiji = ? AND idNichiji = ? AND idRaceNum = ?
            """,
            parts,
        )
        return results[0] if results else None

    def get_race_entries(self, race_key: str) -> list[dict[str, Any]]:
        """出走馬情報（NL_SE_RACE_UMA）を取得する。

        idプレフィックス付きカラムを正規化名に変換して返す。

        Args:
            race_key: 16桁のrace_key

        Returns:
            出走馬のdictリスト（Umaban昇順）。該当なしの場合空リスト。
        """
        parts = self._parse_race_key(race_key)
        if parts is None:
            return []

        return self._db.execute_query(
            """
            SELECT
                idYear AS Year, idMonthDay AS MonthDay,
                idJyoCD AS JyoCD, idKaiji AS Kaiji,
                idNichiji AS Nichiji, idRaceNum AS RaceNum,
                Wakuban, Umaban, KettoNum, Bamei,
                SexCD, Barei, Futan,
                KisyuRyakusyo, ChokyosiRyakusyo,
                BaTaijyu, ZogenFugo, ZogenSa,
                KakuteiJyuni, Ninki, Odds, Time,
                HaronTimeL3, HaronTimeL4,
                DMJyuni, KyakusituKubun,
                Jyuni1c, Jyuni2c, Jyuni3c, Jyuni4c
            FROM NL_SE_RACE_UMA
            WHERE idYear = ? AND idMonthDay = ? AND idJyoCD = ?
              AND idKaiji = ? AND idNichiji = ? AND idRaceNum = ?
            ORDER BY CAST(Umaban AS INTEGER)
            """,
            parts,
        )

    def get_odds(self, race_key: str, odds_table: str = "NL_O1_ODDS_TANFUKUWAKU") -> dict[str, float]:
        """単勝オッズを馬番→オッズのdictとして取得する。

        NL_O1_ODDS_TANFUKUWAKUは1行=1レースの横持ち構造。
        OddsTansyoInfo0〜27のUmaban/Oddsペアを縦持ちdictに変換する。

        Args:
            race_key: 16桁のrace_key
            odds_table: オッズテーブル名（デフォルト: 単複枠）

        Returns:
            {馬番文字列: オッズfloat} のdict。該当なしの場合空dict。

        Raises:
            ValueError: 許可されていないテーブル名が指定された場合
        """
        if odds_table not in _ALLOWED_ODDS_TABLES:
            raise ValueError(
                f"許可されていないオッズテーブル名: '{odds_table}' "
                f"(許可: {sorted(_ALLOWED_ODDS_TABLES)})"
            )

        parts = self._parse_race_key(race_key)
        if parts is None:
            return {}

        results = self._db.execute_query(
            f"""
            SELECT * FROM {odds_table}
            WHERE idYear = ? AND idMonthDay = ? AND idJyoCD = ?
              AND idKaiji = ? AND idNichiji = ? AND idRaceNum = ?
            """,
            parts,
        )
        if not results:
            return {}

        return self._parse_odds_row(results[0])

    def get_payouts(self, race_key: str) -> dict[str, Any]:
        """払戻情報（NL_HR_PAY）を取得する。

        バックテストでの実際の的中判定に使用する。

        Args:
            race_key: 16桁のrace_key

        Returns:
            払戻情報dict。キー例:
                "tansyo": [{"umaban": "03", "pay": 1250, "ninki": 5}]
                "fukusyo": [...]
                "umaren": [...]
                "umatan": [...]
                "sanrenpuku": [...]
                "sanrentan": [...]
        """
        parts = self._parse_race_key(race_key)
        if parts is None:
            return {}

        results = self._db.execute_query(
            """
            SELECT * FROM NL_HR_PAY
            WHERE idYear = ? AND idMonthDay = ? AND idJyoCD = ?
              AND idKaiji = ? AND idNichiji = ? AND idRaceNum = ?
            """,
            parts,
        )
        if not results:
            return {}

        return self._parse_payouts_row(results[0])

    @staticmethod
    def _extract_pay_entries(
        row: dict[str, Any], prefix: str, count: int, selection_key: str
    ) -> list[dict[str, Any]]:
        """払戻テーブルの繰返し項目を抽出する。"""
        entries = []
        for i in range(count):
            sel = str(row.get(f"{prefix}{i}{selection_key}", "")).strip()
            pay_str = str(row.get(f"{prefix}{i}Pay", "0")).strip()
            ninki_str = str(row.get(f"{prefix}{i}Ninki", "0")).strip()

            if not sel or sel == "00" or sel == "0000" or sel == "000000":
                continue
            try:
                pay = int(pay_str)
                ninki = int(ninki_str) if ninki_str.isdigit() else 0
                if pay > 0:
                    entries.append({"selection": sel, "pay": pay, "ninki": ninki})
            except (ValueError, TypeError):
                continue
        return entries

    @staticmethod
    def _parse_odds_row(row: dict[str, Any]) -> dict[str, float]:
        """オッズ行(横持ち)を {馬番: オッズ} dictに変換する。"""
        odds_map: dict[str, float] = {}
        for i in range(28):
            umaban_key = f"OddsTansyoInfo{i}Umaban"
            odds_key = f"OddsTansyoInfo{i}Odds"
            umaban = str(row.get(umaban_key, "")).strip()
            odds_str = str(row.get(odds_key, "")).strip()

            if not umaban or umaban == "00" or not odds_str:
                continue
            if odds_str in ("----", "****", "0000", ""):
                continue

            try:
                odds_val = float(odds_str) / 10.0
                if odds_val > 0:
                    odds_map[umaban] = odds_val
            except (ValueError, TypeError):
                continue
        return odds_map

    @staticmethod
    def _parse_payouts_row(row: dict[str, Any]) -> dict[str, Any]:
        """払戻行(横持ち)を構造化dictに変換する。"""
        payouts: dict[str, list[dict]] = {}
        payouts["tansyo"] = JVLinkDataProvider._extract_pay_entries(row, "PayTansyo", 3, "Umaban")
        payouts["fukusyo"] = JVLinkDataProvider._extract_pay_entries(row, "PayFukusyo", 5, "Umaban")
        payouts["umaren"] = JVLinkDataProvider._extract_pay_entries(row, "PayUmaren", 3, "Kumi")
        payouts["umatan"] = JVLinkDataProvider._extract_pay_entries(row, "PayUmatan", 6, "Kumi")
        payouts["sanrenpuku"] = JVLinkDataProvider._extract_pay_entries(row, "PaySanrenpuku", 3, "Kumi")
        payouts["sanrentan"] = JVLinkDataProvider._extract_pay_entries(row, "PaySanrentan", 6, "Kumi")
        return payouts

    @staticmethod
    def _build_race_key_from_id_columns(row: dict[str, Any]) -> str:
        """idプレフィックス付き行からrace_keyを構築する。"""
        return (
            str(row.get("idYear", ""))
            + str(row.get("idMonthDay", ""))
            + str(row.get("idJyoCD", ""))
            + str(row.get("idKaiji", ""))
            + str(row.get("idNichiji", ""))
            + str(row.get("idRaceNum", ""))
        )

    @staticmethod
    def _build_date_conditions(
        date_from: str, date_to: str,
    ) -> tuple[str, list[Any]]:
        """日付範囲条件のWHERE句とパラメータを構築する。"""
        conditions: list[str] = []
        params: list[Any] = []
        if date_from:
            year_from, md_from = date_from[:4], date_from[4:8]
            conditions.append("(idYear > ? OR (idYear = ? AND idMonthDay >= ?))")
            params.extend([year_from, year_from, md_from])
        if date_to:
            year_to, md_to = date_to[:4], date_to[4:8]
            conditions.append("(idYear < ? OR (idYear = ? AND idMonthDay <= ?))")
            params.extend([year_to, year_to, md_to])
        if conditions:
            return " AND ".join(conditions), params
        return "", []

    @staticmethod
    def _build_bound_conditions(
        first_race: dict[str, Any], last_race: dict[str, Any],
    ) -> tuple[str, list[Any]]:
        """最初と最後のレースの日付範囲からbound WHERE句を構築する。"""
        cond = (
            "(idYear > ? OR (idYear = ? AND idMonthDay >= ?)) AND "
            "(idYear < ? OR (idYear = ? AND idMonthDay <= ?))"
        )
        params = [
            first_race["Year"], first_race["Year"], first_race["MonthDay"],
            last_race["Year"], last_race["Year"], last_race["MonthDay"],
        ]
        return cond, params

    def ensure_indexes(self) -> None:
        """バッチクエリ用インデックスを作成する（冪等）。"""
        _race_cols = "idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum"
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_ra_race_date"
            " ON NL_RA_RACE(idYear, idMonthDay)",
            "CREATE INDEX IF NOT EXISTS idx_se_race_uma_race"
            f" ON NL_SE_RACE_UMA({_race_cols})",
            "CREATE INDEX IF NOT EXISTS idx_se_race_uma_ketto"
            " ON NL_SE_RACE_UMA(KettoNum, idYear, idMonthDay)",
            "CREATE INDEX IF NOT EXISTS idx_o1_odds_race"
            f" ON NL_O1_ODDS_TANFUKUWAKU({_race_cols})",
            "CREATE INDEX IF NOT EXISTS idx_hr_pay_race"
            f" ON NL_HR_PAY({_race_cols})",
        ]
        for idx_sql in indexes:
            with contextlib.suppress(Exception):
                self._db.execute_write(idx_sql)

    def fetch_races_batch(
        self,
        date_from: str = "",
        date_to: str = "",
        max_races: int = 5000,
        include_payouts: bool = True,
    ) -> list[dict[str, Any]]:
        """対象期間のレースデータを一括取得する。

        4つのテーブルを各1回のクエリで取得し、race_keyでグルーピングする。
        N+1クエリ問題を解消するバッチ版メソッド。

        Args:
            date_from: 開始日 "YYYYMMDD"（空文字で制限なし）
            date_to: 終了日 "YYYYMMDD"（空文字で制限なし）
            max_races: 最大レース数
            include_payouts: 払戻テーブルも取得するか

        Returns:
            [{"race_key": str, "race_info": {...}, "entries": [...],
              "odds": {...}, "payouts": {...}}, ...]
            ASC時系列順でソート済み。
        """
        if not self._db.table_exists("NL_RA_RACE"):
            return []

        # インデックス作成（初回のみ実質動作、以降はIF NOT EXISTSでスキップ）
        self.ensure_indexes()

        # Step 1: レース一覧取得
        where_clause, params = self._build_date_conditions(date_from, date_to)
        where_sql = f"WHERE {where_clause}" if where_clause else ""

        race_rows = self._db.execute_query(
            f"""
            SELECT
                idYear AS Year, idMonthDay AS MonthDay,
                idJyoCD AS JyoCD, idKaiji AS Kaiji,
                idNichiji AS Nichiji, idRaceNum AS RaceNum,
                RaceInfoHondai AS RaceName,
                Kyori, TrackCD,
                TenkoBabaTenkoCD AS TenkoCD,
                TenkoBabaSibaBabaCD AS SibaBabaCD,
                TenkoBabaDirtBabaCD AS DirtBabaCD,
                JyokenInfoSyubetuCD AS SyubetuCD,
                GradeCD,
                HassoTime,
                TorokuTosu, SyussoTosu, NyusenTosu,
                HaronTimeL3, HaronTimeL4
            FROM NL_RA_RACE
            {where_sql}
            ORDER BY idYear ASC, idMonthDay ASC
            LIMIT ?
            """,
            tuple(params + [max_races]),
        )
        if not race_rows:
            return []

        # race_key → race_info マッピング（挿入順 = ASC時系列順）
        race_map: dict[str, dict[str, Any]] = {}
        for row in race_rows:
            rk = self.build_race_key(row)
            race_map[rk] = row
        valid_keys = set(race_map.keys())

        # bound条件: 最初〜最後レースの日付範囲
        first, last = race_rows[0], race_rows[-1]
        bound_where, bound_params = self._build_bound_conditions(first, last)

        # テーブル存在チェック（並列化前に確認）
        has_entries = self._db.table_exists("NL_SE_RACE_UMA")
        has_odds = self._db.table_exists("NL_O1_ODDS_TANFUKUWAKU")
        has_payouts = include_payouts and self._db.table_exists("NL_HR_PAY")

        db_path = str(self._db.db_path)
        bound_params_tuple = tuple(bound_params)

        # Steps 2-4: 出走馬・オッズ・払戻を並列取得
        # 各スレッドが独立したSQLite接続を使用（WALモード並行読み取り）
        def _query_entries() -> list[dict[str, Any]]:
            if not has_entries:
                return []
            tmp_db = DatabaseManager(db_path, wal_mode=False)
            return tmp_db.execute_query(
                f"""
                SELECT
                    idYear AS Year, idMonthDay AS MonthDay,
                    idJyoCD AS JyoCD, idKaiji AS Kaiji,
                    idNichiji AS Nichiji, idRaceNum AS RaceNum,
                    Wakuban, Umaban, KettoNum, Bamei,
                    SexCD, Barei, Futan,
                    KisyuRyakusyo, ChokyosiRyakusyo,
                    BaTaijyu, ZogenFugo, ZogenSa,
                    KakuteiJyuni, Ninki, Odds, Time,
                    HaronTimeL3, HaronTimeL4,
                    DMJyuni, KyakusituKubun,
                    Jyuni1c, Jyuni2c, Jyuni3c, Jyuni4c
                FROM NL_SE_RACE_UMA
                WHERE {bound_where}
                ORDER BY idYear ASC, idMonthDay ASC, CAST(Umaban AS INTEGER)
                """,
                bound_params_tuple,
            )

        def _query_odds() -> list[dict[str, Any]]:
            if not has_odds:
                return []
            tmp_db = DatabaseManager(db_path, wal_mode=False)
            return tmp_db.execute_query(
                f"""
                SELECT * FROM NL_O1_ODDS_TANFUKUWAKU
                WHERE {bound_where}
                """,
                bound_params_tuple,
            )

        def _query_payouts() -> list[dict[str, Any]]:
            if not has_payouts:
                return []
            tmp_db = DatabaseManager(db_path, wal_mode=False)
            return tmp_db.execute_query(
                f"""
                SELECT * FROM NL_HR_PAY
                WHERE {bound_where}
                """,
                bound_params_tuple,
            )

        with ThreadPoolExecutor(max_workers=3) as pool:
            fut_entries = pool.submit(_query_entries)
            fut_odds = pool.submit(_query_odds)
            fut_payouts = pool.submit(_query_payouts)
            try:
                entry_rows = fut_entries.result()
                odds_rows = fut_odds.result()
                pay_rows = fut_payouts.result()
            except Exception:
                # 全スレッド完了を待ってからre-raise（リソースリーク防止）
                for f in [fut_entries, fut_odds, fut_payouts]:
                    with contextlib.suppress(Exception):
                        f.result(timeout=5)
                raise

        # グルーピング
        entries_by_race: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in entry_rows:
            rk = self.build_race_key(row)
            if rk in valid_keys:
                entries_by_race[rk].append(row)

        odds_by_race: dict[str, dict[str, float]] = {}
        for row in odds_rows:
            rk = self._build_race_key_from_id_columns(row)
            if rk in valid_keys:
                odds_by_race[rk] = self._parse_odds_row(row)

        payouts_by_race: dict[str, dict[str, Any]] = {}
        for row in pay_rows:
            rk = self._build_race_key_from_id_columns(row)
            if rk in valid_keys:
                payouts_by_race[rk] = self._parse_payouts_row(row)

        # Step 5: 結果組み立て
        results: list[dict[str, Any]] = []
        for rk, race_info in race_map.items():
            entries = entries_by_race.get(rk, [])
            if not entries:
                continue
            results.append({
                "race_key": rk,
                "race_info": race_info,
                "entries": entries,
                "odds": odds_by_race.get(rk, {}),
                "payouts": payouts_by_race.get(rk, {}),
            })

        logger.info(
            f"バッチ取得完了: {len(results)}レース "
            f"(entries={sum(len(r['entries']) for r in results)}頭, "
            f"odds={len(odds_by_race)}件, payouts={len(payouts_by_race)}件)"
        )
        return results

    def get_previous_race_entry(
        self, ketto_num: str, current_race_key: str,
    ) -> dict[str, Any] | None:
        """指定馬の前走（直近1走前）の出走データを取得する。

        NL_SE_RACE_UMAから、同一KettoNumかつ日付が現在レースより前の
        直近レコードを返す。

        Args:
            ketto_num: 馬登録番号（10桁）
            current_race_key: 現在レースのrace_key（16桁）

        Returns:
            前走の出走馬データdict。前走なしの場合None。
        """
        if not ketto_num or not ketto_num.strip():
            return None

        parts = self._parse_race_key(current_race_key)
        if parts is None:
            return None

        cur_year, cur_monthday = parts[0], parts[1]

        if not self._db.table_exists("NL_SE_RACE_UMA"):
            return None

        results = self._db.execute_query(
            """
            SELECT
                Wakuban, Umaban, KettoNum, Bamei,
                SexCD, Barei, Futan,
                KisyuRyakusyo, ChokyosiRyakusyo,
                BaTaijyu, ZogenFugo, ZogenSa,
                KakuteiJyuni, Ninki, Odds, Time,
                HaronTimeL3, HaronTimeL4,
                DMJyuni, KyakusituKubun,
                Jyuni1c, Jyuni2c, Jyuni3c, Jyuni4c
            FROM NL_SE_RACE_UMA
            WHERE KettoNum = ?
              AND (idYear < ? OR (idYear = ? AND idMonthDay < ?))
            ORDER BY idYear DESC, idMonthDay DESC
            LIMIT 1
            """,
            (ketto_num, cur_year, cur_year, cur_monthday),
        )
        return results[0] if results else None

    def get_race_list(
        self,
        year: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """レース一覧を取得する（ダッシュボード用）。

        Args:
            year: 絞り込む開催年（Noneで全件）
            limit: 最大取得件数

        Returns:
            レース情報のdictリスト（日付降順）
        """
        if year:
            return self._db.execute_query(
                """
                SELECT
                    idYear AS Year, idMonthDay AS MonthDay,
                    idJyoCD AS JyoCD, idKaiji AS Kaiji,
                    idNichiji AS Nichiji, idRaceNum AS RaceNum,
                    RaceInfoHondai AS RaceName, Kyori, TrackCD,
                    SyussoTosu, GradeCD
                FROM NL_RA_RACE
                WHERE idYear = ?
                ORDER BY idYear DESC, idMonthDay DESC, CAST(idRaceNum AS INTEGER)
                LIMIT ?
                """,
                (year, limit),
            )
        return self._db.execute_query(
            """
            SELECT
                idYear AS Year, idMonthDay AS MonthDay,
                idJyoCD AS JyoCD, idKaiji AS Kaiji,
                idNichiji AS Nichiji, idRaceNum AS RaceNum,
                RaceInfoHondai AS RaceName, Kyori, TrackCD,
                SyussoTosu, GradeCD
            FROM NL_RA_RACE
            ORDER BY idYear DESC, idMonthDay DESC, CAST(idRaceNum AS INTEGER)
            LIMIT ?
            """,
            (limit,),
        )
