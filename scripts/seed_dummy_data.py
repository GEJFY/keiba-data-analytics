"""ダミーデータ生成スクリプト。

JVLinkToSQLiteの実テーブルスキーマ（NL_RA_RACE, NL_SE_RACE_UMA等）に準拠した
リアルな競馬データを投入する。2025年1月 中山開催を模擬した3日分・36レースのデータ。

Usage:
    python scripts/seed_dummy_data.py [--db-path ./data/demo.db]
"""

import random
import sqlite3
import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加（直接実行対応）
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ============================================================
# マスタデータ定義（実在する競馬場・馬名等を模擬）
# ============================================================

JYO_CODES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}

HORSE_NAMES = [
    "マンジフェニックス", "ゴールドクレスト", "サイレンスブレイク", "スカイフォーチュン",
    "ダイナミックエッジ", "レッドインパクト", "ブルーサンダー", "ホワイトアロー",
    "グリーンスプリンター", "シルバーストーム", "クリムゾンウィング", "ブラックダイヤ",
    "ゴールデンサンライズ", "ミッドナイトラン", "サクラブレイブ", "タイガーシャドウ",
    "フローラルキング", "アイアンハート", "ドリームキャッチャー", "スターダスト",
    "ライトニングボルト", "ウインドチェイサー", "ムーンライトソナタ", "サンデーサイレンス二世",
    "ノーザンフラッシュ", "サウスウインド", "イーストリバー", "ウエストバレー",
    "オーシャンブリーズ", "マウンテンピーク", "リバーサイドキング", "フォレストガーディアン",
    "デザートストーム", "アイスクリスタル", "フレイムランナー", "サンダーボルト",
    "レインボーブリッジ", "スプリングフィールド", "オータムリーフ", "ウインターソルジャー",
    "ダークホース", "ブライトスター", "クイーンズガード", "キングスナイト",
    "プリンスオブウェールズ", "ロイヤルアスコット", "セントレジャー", "エプソムダービー",
]

JOCKEY_NAMES = [
    "武田太郎", "鈴木一馬", "佐藤翔太", "田中勇人", "高橋直哉",
    "渡辺健二", "伊藤慎也", "山本和也", "中村俊介", "小林大輔",
    "加藤光", "吉田博", "山口亮", "松本裕", "井上翼",
]

COURSE_CONDITIONS = [
    ("1200", "芝", "10"),  # 10=芝・左
    ("1400", "芝", "11"),  # 11=芝・右
    ("1600", "芝", "10"),
    ("1800", "芝", "11"),
    ("2000", "芝", "10"),
    ("2200", "芝", "11"),
    ("2500", "芝", "10"),
    ("3600", "芝", "10"),  # 障害
    ("1200", "ダート", "22"),  # 22=ダート・右
    ("1400", "ダート", "23"),
    ("1800", "ダート", "22"),
    ("2100", "ダート", "23"),
]

RACE_NAMES = {
    1: "中山金杯", 2: "2歳新馬", 3: "3歳未勝利", 4: "1勝クラス",
    5: "2勝クラス", 6: "3勝クラス", 7: "サンライズS", 8: "迎春S",
    9: "初富士S", 10: "ニューイヤーS", 11: "ジュニアC", 12: "中山最終",
}

# 固定馬プール（複数レースに再出走して前走データを生成するため）
HORSE_POOL: list[dict] = []
_pool_rng = random.Random(99)
for _i in range(40):
    HORSE_POOL.append({
        "KettoNum": f"{2000000000 + _i:010d}",
        "Bamei": HORSE_NAMES[_i % len(HORSE_NAMES)],
        "SexCD": _pool_rng.choice(["1", "2", "3"]),
        "Barei": str(_pool_rng.randint(2, 8)),
    })


def _random_odds(num_horses: int, favorite_idx: int) -> list[float]:
    """リアルなオッズ分布を生成する。"""
    odds_list = []
    for i in range(num_horses):
        if i == favorite_idx:
            odds_list.append(round(random.uniform(1.5, 4.0), 1))
        elif i < 3:
            odds_list.append(round(random.uniform(3.0, 10.0), 1))
        elif i < 6:
            odds_list.append(round(random.uniform(8.0, 30.0), 1))
        else:
            odds_list.append(round(random.uniform(20.0, 200.0), 1))
    return odds_list


def _random_time(kyori: str) -> str:
    """走破タイムを生成する。"""
    dist = int(kyori)
    base_seconds = (dist / 200.0) * 12.0
    variation = random.uniform(-2.0, 4.0)
    total = base_seconds + variation
    minutes = int(total // 60)
    seconds = total % 60
    return f"{minutes}{seconds:04.1f}"


def _random_haron_l3() -> str:
    """上がり3Fタイムを生成する。"""
    return f"{random.uniform(33.0, 38.0):.1f}"


def create_jvlink_tables(conn: sqlite3.Connection) -> None:
    """JVLinkToSQLite実スキーマのテーブルを作成する。"""
    # NL_RA_RACE（レース情報）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_RA_RACE (
            headRecordSpec TEXT, headDataKubun TEXT, headMakeDate TEXT,
            idYear TEXT NOT NULL, idMonthDay TEXT NOT NULL,
            idJyoCD TEXT NOT NULL, idKaiji TEXT NOT NULL,
            idNichiji TEXT NOT NULL, idRaceNum TEXT NOT NULL,
            RaceInfoYoubiCD TEXT, RaceInfoTokuNum TEXT,
            RaceInfoHondai TEXT, RaceInfoFukudai TEXT, RaceInfoKakko TEXT,
            RaceInfoRyakusyo10 TEXT, RaceInfoRyakusyo6 TEXT, RaceInfoRyakusyo3 TEXT,
            GradeCD TEXT, GradeCDBefore TEXT,
            JyokenInfoSyubetuCD TEXT, JyokenInfoKigoCD TEXT, JyokenInfoJyuryoCD TEXT,
            JyokenInfoJyokenCD0 TEXT, JyokenInfoJyokenCD1 TEXT,
            JyokenName TEXT, Kyori TEXT, TrackCD TEXT,
            CourseKubunCD TEXT,
            HassoTime TEXT,
            TorokuTosu TEXT, SyussoTosu TEXT, NyusenTosu TEXT,
            TenkoBabaTenkoCD TEXT, TenkoBabaSibaBabaCD TEXT, TenkoBabaDirtBabaCD TEXT,
            HaronTimeS3 TEXT, HaronTimeS4 TEXT,
            HaronTimeL3 TEXT, HaronTimeL4 TEXT,
            RecordUpKubun TEXT,
            PRIMARY KEY (idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum)
        )
    """)

    # NL_SE_RACE_UMA（馬毎レース情報）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_SE_RACE_UMA (
            headRecordSpec TEXT, headDataKubun TEXT, headMakeDate TEXT,
            idYear TEXT NOT NULL, idMonthDay TEXT NOT NULL,
            idJyoCD TEXT NOT NULL, idKaiji TEXT NOT NULL,
            idNichiji TEXT NOT NULL, idRaceNum TEXT NOT NULL,
            Wakuban TEXT, Umaban TEXT, KettoNum TEXT NOT NULL,
            Bamei TEXT, UmaKigoCD TEXT, SexCD TEXT, HinsyuCD TEXT, KeiroCD TEXT,
            Barei TEXT, TozaiCD TEXT,
            ChokyosiCode TEXT, ChokyosiRyakusyo TEXT,
            BanusiCode TEXT, BanusiName TEXT,
            Futan TEXT, FutanBefore TEXT,
            Blinker TEXT,
            KisyuCode TEXT, KisyuRyakusyo TEXT,
            MinaraiCD TEXT,
            BaTaijyu TEXT, ZogenFugo TEXT, ZogenSa TEXT,
            IJyoCD TEXT, NyusenJyuni TEXT, KakuteiJyuni TEXT,
            DochakuKubun TEXT, DochakuTosu TEXT,
            Time TEXT, ChakusaCD TEXT,
            Jyuni1c TEXT, Jyuni2c TEXT, Jyuni3c TEXT, Jyuni4c TEXT,
            Odds TEXT, Ninki TEXT,
            Honsyokin TEXT, Fukasyokin TEXT,
            HaronTimeL4 TEXT, HaronTimeL3 TEXT,
            TimeDiff TEXT, RecordUpKubun TEXT,
            DMKubun TEXT, DMTime TEXT, DMGosaP TEXT, DMGosaM TEXT, DMJyuni TEXT,
            KyakusituKubun TEXT,
            PRIMARY KEY (idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum, KettoNum)
        )
    """)

    # NL_HR_PAY（払戻情報）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_HR_PAY (
            headRecordSpec TEXT, headDataKubun TEXT, headMakeDate TEXT,
            idYear TEXT NOT NULL, idMonthDay TEXT NOT NULL,
            idJyoCD TEXT NOT NULL, idKaiji TEXT NOT NULL,
            idNichiji TEXT NOT NULL, idRaceNum TEXT NOT NULL,
            TorokuTosu TEXT, SyussoTosu TEXT,
            PayTansyo0Umaban TEXT, PayTansyo0Pay TEXT, PayTansyo0Ninki TEXT,
            PayTansyo1Umaban TEXT, PayTansyo1Pay TEXT, PayTansyo1Ninki TEXT,
            PayTansyo2Umaban TEXT, PayTansyo2Pay TEXT, PayTansyo2Ninki TEXT,
            PayFukusyo0Umaban TEXT, PayFukusyo0Pay TEXT, PayFukusyo0Ninki TEXT,
            PayFukusyo1Umaban TEXT, PayFukusyo1Pay TEXT, PayFukusyo1Ninki TEXT,
            PayFukusyo2Umaban TEXT, PayFukusyo2Pay TEXT, PayFukusyo2Ninki TEXT,
            PRIMARY KEY (idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum)
        )
    """)

    # NL_O1_ODDS_TANFUKUWAKU（単複枠オッズ — 1行=1レース、横持ち）
    cols = ["headRecordSpec TEXT", "headDataKubun TEXT", "headMakeDate TEXT"]
    cols += [f"idYear TEXT NOT NULL", "idMonthDay TEXT NOT NULL"]
    cols += ["idJyoCD TEXT NOT NULL", "idKaiji TEXT NOT NULL"]
    cols += ["idNichiji TEXT NOT NULL", "idRaceNum TEXT NOT NULL"]
    cols += ["HappyoTime TEXT", "TorokuTosu TEXT", "SyussoTosu TEXT"]
    for i in range(28):
        cols += [
            f"OddsTansyoInfo{i}Umaban TEXT",
            f"OddsTansyoInfo{i}Odds TEXT",
            f"OddsTansyoInfo{i}Ninki TEXT",
        ]
    cols += ["PRIMARY KEY (idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum)"]
    conn.execute(f"CREATE TABLE IF NOT EXISTS NL_O1_ODDS_TANFUKUWAKU ({', '.join(cols)})")

    # NL_UM_UMA（馬マスタ）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_UM_UMA (
            KettoNum TEXT PRIMARY KEY, Bamei TEXT, SexCD TEXT,
            KeiroCD TEXT, HinsyuCD TEXT, SanchiName TEXT
        )
    """)

    # NL_KS_KISYU（騎手マスタ）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_KS_KISYU (
            KisyuCode TEXT PRIMARY KEY, KisyuName TEXT,
            MinaraiCD TEXT, SexCD TEXT, BirthDate TEXT
        )
    """)

    conn.commit()


def seed_races(conn: sqlite3.Connection) -> list[dict]:
    """レースデータを3日分投入する。"""
    random.seed(42)

    race_list = []
    days = [("0105", "01"), ("0106", "02"), ("0112", "03")]

    for month_day, nichiji in days:
        for race_num in range(1, 13):
            course = random.choice(COURSE_CONDITIONS)
            kyori, track_type, track_cd = course

            race_name = RACE_NAMES.get(race_num, f"{race_num}R")
            if race_num == 1 and month_day == "0105":
                race_name = "中山金杯"
                kyori = "2000"
                track_cd = "10"

            num_horses = random.randint(8, 18)
            tenko_cd = random.choice(["1", "2", "3"])
            baba_cd = random.choice(["1", "2", "3", "4"])
            grade_cd = " " if race_num != 1 or month_day != "0105" else "B"

            conn.execute(
                """INSERT INTO NL_RA_RACE (
                    headRecordSpec, headDataKubun, headMakeDate,
                    idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum,
                    RaceInfoHondai, RaceInfoRyakusyo6,
                    GradeCD, JyokenInfoSyubetuCD,
                    Kyori, TrackCD, CourseKubunCD,
                    HassoTime, TorokuTosu, SyussoTosu, NyusenTosu,
                    TenkoBabaTenkoCD, TenkoBabaSibaBabaCD, TenkoBabaDirtBabaCD,
                    HaronTimeL3
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    "RA", "7", "20250120",
                    "2025", month_day, "06", "01", nichiji, f"{race_num:02d}",
                    race_name, race_name[:6],
                    grade_cd, "11",
                    kyori, track_cd, "A ",
                    f"{9 + race_num:02d}{random.choice(['00', '10', '25', '35'])}",
                    str(num_horses), str(num_horses), str(num_horses),
                    tenko_cd, baba_cd, baba_cd,
                    f"{random.uniform(34.0, 37.0):.1f}",
                ),
            )
            race_list.append({
                "Year": "2025", "MonthDay": month_day, "JyoCD": "06",
                "Kaiji": "01", "Nichiji": nichiji, "RaceNum": f"{race_num:02d}",
                "Kyori": kyori, "TrackCD": track_cd,
                "num_horses": num_horses,
            })

    conn.commit()
    return race_list


def seed_entries(conn: sqlite3.Connection, race_list: list[dict]) -> None:
    """出走馬データ・オッズ・払戻を投入する。

    固定馬プール（HORSE_POOL）からランダムに選出し、
    同一馬が複数レースに出走することで前走データが生成される。
    """
    random.seed(42)
    used_ketto: set[str] = set()

    for race in race_list:
        num_horses = race["num_horses"]
        favorite_idx = random.randint(0, min(2, num_horses - 1))
        odds_list = _random_odds(num_horses, favorite_idx)
        odds_sorted_indices = sorted(range(num_horses), key=lambda i: odds_list[i])

        # 馬プールからランダムに選出（同一馬の再出走あり）
        selected_horses = random.sample(HORSE_POOL, min(num_horses, len(HORSE_POOL)))
        jockeys = random.choices(JOCKEY_NAMES, k=num_horses)

        # 着順をランダムに割り当て
        finish_order = list(range(1, num_horses + 1))
        random.shuffle(finish_order)

        # 4コーナー順位（着順と相関あるがシャッフル）
        corner4 = list(range(1, num_horses + 1))
        random.shuffle(corner4)

        for i in range(num_horses):
            pool_horse = selected_horses[i]
            ketto_num = pool_horse["KettoNum"]

            umaban = f"{i + 1:02d}"
            wakuban = f"{(i // 2) + 1}"
            ninki = str(odds_sorted_indices.index(i) + 1)

            # 馬体重（3カラム分離）
            bataijyu = str(random.randint(420, 540))
            zogen_diff = random.choice([0, 0, 2, 2, 4, 6, 8, 10])
            zogen_fugo = random.choice(["+", "-", " "])
            zogen_sa = str(zogen_diff)

            # 脚質
            kyakusitu = str(random.choice([1, 1, 2, 2, 2, 3, 3, 3, 4]))

            # DM予想順位（人気と相関ありだが完全一致ではない）
            dm_jyuni = str(max(1, min(num_horses, int(ninki) + random.randint(-3, 3))))

            conn.execute(
                """INSERT INTO NL_SE_RACE_UMA (
                    headRecordSpec, headDataKubun, headMakeDate,
                    idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum,
                    Wakuban, Umaban, KettoNum, Bamei,
                    SexCD, Barei, Futan,
                    KisyuRyakusyo, ChokyosiRyakusyo,
                    BaTaijyu, ZogenFugo, ZogenSa,
                    KakuteiJyuni, Ninki, Odds, Time,
                    HaronTimeL3,
                    DMJyuni, KyakusituKubun,
                    Jyuni1c, Jyuni2c, Jyuni3c, Jyuni4c
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    "SE", "7", "20250120",
                    race["Year"], race["MonthDay"], race["JyoCD"],
                    race["Kaiji"], race["Nichiji"], race["RaceNum"],
                    wakuban, umaban, ketto_num,
                    pool_horse["Bamei"],
                    pool_horse["SexCD"],
                    pool_horse["Barei"],
                    str(random.choice([520, 530, 540, 550, 560, 570, 580])),
                    jockeys[i][:4], f"調教{random.randint(1, 20):02d}",
                    bataijyu, zogen_fugo, zogen_sa,
                    str(finish_order[i]), ninki,
                    str(int(odds_list[i] * 10)),
                    _random_time(race["Kyori"]),
                    _random_haron_l3(),
                    dm_jyuni, kyakusitu,
                    str(corner4[i]),
                    str(max(1, corner4[i] + random.randint(-2, 2))),
                    str(max(1, corner4[i] + random.randint(-2, 2))),
                    str(corner4[i]),
                ),
            )

            # 馬マスタ（初回のみ）
            if ketto_num not in used_ketto:
                conn.execute(
                    "INSERT INTO NL_UM_UMA VALUES (?,?,?,?,?,?)",
                    (ketto_num, pool_horse["Bamei"], pool_horse["SexCD"],
                     random.choice(["01", "02", "03", "04"]), "01", "日本"),
                )
                used_ketto.add(ketto_num)

        # --- NL_O1_ODDS_TANFUKUWAKU（1行=1レース、横持ち）---
        o1_cols = ["headRecordSpec", "headDataKubun", "headMakeDate",
                   "idYear", "idMonthDay", "idJyoCD", "idKaiji", "idNichiji", "idRaceNum",
                   "TorokuTosu", "SyussoTosu"]
        o1_vals: list[str] = ["O1", "4", "20250120",
                              race["Year"], race["MonthDay"], race["JyoCD"],
                              race["Kaiji"], race["Nichiji"], race["RaceNum"],
                              str(num_horses), str(num_horses)]

        for idx in range(28):
            if idx < num_horses:
                o1_cols.extend([
                    f"OddsTansyoInfo{idx}Umaban",
                    f"OddsTansyoInfo{idx}Odds",
                    f"OddsTansyoInfo{idx}Ninki",
                ])
                o1_vals.extend([
                    f"{idx + 1:02d}",
                    str(int(odds_list[idx] * 10)),
                    str(odds_sorted_indices.index(idx) + 1),
                ])
            else:
                o1_cols.extend([
                    f"OddsTansyoInfo{idx}Umaban",
                    f"OddsTansyoInfo{idx}Odds",
                    f"OddsTansyoInfo{idx}Ninki",
                ])
                o1_vals.extend(["", "", ""])

        placeholders = ",".join(["?"] * len(o1_vals))
        conn.execute(
            f"INSERT INTO NL_O1_ODDS_TANFUKUWAKU ({','.join(o1_cols)}) VALUES ({placeholders})",
            o1_vals,
        )

        # --- NL_HR_PAY（払戻情報）---
        # 1着馬を特定（着順=1の馬）
        winner_idx = finish_order.index(1)
        second_idx = finish_order.index(2)
        third_idx = finish_order.index(3)

        tansyo_pay = int(odds_list[winner_idx] * 100)
        conn.execute(
            """INSERT INTO NL_HR_PAY (
                headRecordSpec, headDataKubun, headMakeDate,
                idYear, idMonthDay, idJyoCD, idKaiji, idNichiji, idRaceNum,
                TorokuTosu, SyussoTosu,
                PayTansyo0Umaban, PayTansyo0Pay, PayTansyo0Ninki,
                PayFukusyo0Umaban, PayFukusyo0Pay, PayFukusyo0Ninki,
                PayFukusyo1Umaban, PayFukusyo1Pay, PayFukusyo1Ninki,
                PayFukusyo2Umaban, PayFukusyo2Pay, PayFukusyo2Ninki
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "HR", "2", "20250120",
                race["Year"], race["MonthDay"], race["JyoCD"],
                race["Kaiji"], race["Nichiji"], race["RaceNum"],
                str(num_horses), str(num_horses),
                f"{winner_idx + 1:02d}", str(tansyo_pay),
                str(odds_sorted_indices.index(winner_idx) + 1),
                f"{winner_idx + 1:02d}", str(int(tansyo_pay * 0.3)),
                str(odds_sorted_indices.index(winner_idx) + 1),
                f"{second_idx + 1:02d}", str(int(odds_list[second_idx] * 30)),
                str(odds_sorted_indices.index(second_idx) + 1),
                f"{third_idx + 1:02d}", str(int(odds_list[third_idx] * 25)),
                str(odds_sorted_indices.index(third_idx) + 1),
            ),
        )

    conn.commit()


def seed_jockeys(conn: sqlite3.Connection) -> None:
    """騎手マスタを投入する。"""
    for i, name in enumerate(JOCKEY_NAMES):
        conn.execute(
            "INSERT INTO NL_KS_KISYU VALUES (?,?,?,?,?)",
            (
                f"{i + 1:05d}", name, "1", "1",
                f"19{random.randint(80, 99)}{random.randint(1, 12):02d}{random.randint(1, 28):02d}",
            ),
        )
    conn.commit()


def main(db_path: str = "./data/demo.db") -> None:
    """メイン処理：ダミーデータを生成してDBに投入する。"""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        path.unlink()
        print(f"既存DB削除: {path}")

    conn = sqlite3.connect(str(path))
    try:
        # 1. JVLinkテーブル作成
        create_jvlink_tables(conn)
        print("JVLink実スキーマテーブル作成完了")

        # 2. 拡張テーブル作成
        from scripts.init_db import EXTENSION_TABLES, INDEXES
        for ddl in EXTENSION_TABLES:
            conn.execute(ddl)
        for idx in INDEXES:
            conn.execute(idx)
        conn.commit()
        print("拡張テーブル作成完了")

        # 3. レースデータ投入
        race_list = seed_races(conn)
        print(f"レースデータ投入: {len(race_list)}レース")

        # 4. 出走馬・オッズ・払戻データ投入
        seed_entries(conn, race_list)
        total_entries = conn.execute("SELECT COUNT(*) FROM NL_SE_RACE_UMA").fetchone()[0]
        print(f"出走馬データ投入: {total_entries}頭")

        # 5. 騎手マスタ投入
        seed_jockeys(conn)
        print(f"騎手マスタ投入: {len(JOCKEY_NAMES)}名")

        # 6. サマリー出力
        print(f"\n=== ダミーデータ生成完了 ===")
        print(f"DBパス: {path.resolve()}")
        for table in ["NL_RA_RACE", "NL_SE_RACE_UMA", "NL_HR_PAY",
                       "NL_O1_ODDS_TANFUKUWAKU", "NL_UM_UMA", "NL_KS_KISYU"]:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,}件")

    finally:
        conn.close()


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "./data/demo.db"
    main(target)
