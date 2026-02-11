"""ダミーデータ生成スクリプト。

JVLinkToSQLiteと同等のNL_*テーブルにリアルな競馬データを投入する。
2025年1月 中山開催を模擬した3日分・36レースのデータを生成。

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

# 競馬場コード (JRA)
JYO_CODES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}

# 馬名プール（実在しない架空名）
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

# 騎手名プール
JOCKEY_NAMES = [
    "武田太郎", "鈴木一馬", "佐藤翔太", "田中勇人", "高橋直哉",
    "渡辺健二", "伊藤慎也", "山本和也", "中村俊介", "小林大輔",
    "加藤光", "吉田博", "山口亮", "松本裕", "井上翼",
]

# 距離と芝/ダートの組み合わせ
COURSE_CONDITIONS = [
    ("1200", "芝", "01"),  # 01=芝
    ("1400", "芝", "01"),
    ("1600", "芝", "01"),
    ("1800", "芝", "01"),
    ("2000", "芝", "01"),
    ("2200", "芝", "01"),
    ("2500", "芝", "01"),
    ("3600", "芝", "01"),  # 障害
    ("1200", "ダート", "02"),  # 02=ダート
    ("1400", "ダート", "02"),
    ("1800", "ダート", "02"),
    ("2100", "ダート", "02"),
]

# レース名（重賞含む）
RACE_NAMES = {
    1: "中山金杯", 2: "2歳新馬", 3: "3歳未勝利", 4: "1勝クラス",
    5: "2勝クラス", 6: "3勝クラス", 7: "サンライズS", 8: "迎春S",
    9: "初富士S", 10: "ニューイヤーS", 11: "ジュニアC", 12: "中山最終",
}


def _random_weight() -> str:
    """馬体重を生成する（420〜540kg）。"""
    base = random.randint(420, 540)
    diff = random.choice([-8, -6, -4, -2, 0, 0, 2, 2, 4, 6, 8])
    return f"{base}({diff:+d})"


def _random_time(kyori: str) -> str:
    """走破タイムを生成する（距離に応じたリアルな範囲）。"""
    dist = int(kyori)
    # 1Fあたり約12.0秒（芝）を基準に±ランダム
    base_seconds = (dist / 200.0) * 12.0
    variation = random.uniform(-2.0, 4.0)
    total = base_seconds + variation
    minutes = int(total // 60)
    seconds = total % 60
    return f"{minutes}:{seconds:04.1f}"


def _random_odds(num_horses: int, favorite_idx: int) -> list[float]:
    """リアルなオッズ分布を生成する。"""
    odds_list = []
    for i in range(num_horses):
        if i == favorite_idx:
            # 1番人気: 1.5〜4.0
            odds_list.append(round(random.uniform(1.5, 4.0), 1))
        elif i < 3:
            # 2-3番人気: 3.0〜10.0
            odds_list.append(round(random.uniform(3.0, 10.0), 1))
        elif i < 6:
            # 4-6番人気: 8.0〜30.0
            odds_list.append(round(random.uniform(8.0, 30.0), 1))
        else:
            # 人気薄: 20.0〜200.0
            odds_list.append(round(random.uniform(20.0, 200.0), 1))
    return odds_list


def create_jvlink_tables(conn: sqlite3.Connection) -> None:
    """JVLinkToSQLite相当のNL_*テーブルを作成する。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_RA (
            Year TEXT, MonthDay TEXT, JyoCD TEXT, Kaiji TEXT, Nichiji TEXT,
            RaceNum TEXT, RaceName TEXT, Kyori TEXT, TrackCD TEXT,
            TenkoBaba TEXT, Syubetucd TEXT, JyokenCD TEXT,
            HassoTime TEXT, LapTime TEXT, Ninkiord TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_SE (
            Year TEXT, MonthDay TEXT, JyoCD TEXT, Kaiji TEXT, Nichiji TEXT,
            RaceNum TEXT, Umaban TEXT, Wakuban TEXT, KettoNum TEXT,
            Bamei TEXT, SexCD TEXT, Barei TEXT, Futan TEXT,
            KisyuName TEXT, BanusiName TEXT, ChokyosiName TEXT,
            ZogenSa TEXT, Time TEXT, KakuteiJyuni TEXT,
            Ninki TEXT, IDM TEXT, SpeedIndex TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_HR (
            KettoNum TEXT, Bamei TEXT, SexCD TEXT, BirthDate TEXT,
            HansyokuF TEXT, HansyokuM TEXT, BanusiName TEXT,
            ChokyosiName TEXT, Syozoku TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_UM (
            KettoNum TEXT, Bamei TEXT, SexCD TEXT, KeiroCD TEXT,
            HinsyuCD TEXT, SanchiName TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_KS (
            KisyuCode TEXT, KisyuName TEXT, MinaraiCD TEXT,
            SexCD TEXT, BirthDate TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NL_O1 (
            Year TEXT, MonthDay TEXT, JyoCD TEXT, Kaiji TEXT, Nichiji TEXT,
            RaceNum TEXT, Umaban TEXT, Odds TEXT, Ninki TEXT
        )
    """)
    conn.commit()


def seed_races(conn: sqlite3.Connection) -> list[dict]:
    """レースデータを3日分（1日12R × 3日 = 36R）投入する。"""
    random.seed(42)  # 再現性のためシード固定

    race_list = []
    days = [("0105", "01"), ("0106", "02"), ("0112", "03")]  # 3開催日

    for month_day, nichiji in days:
        for race_num in range(1, 13):
            course = random.choice(COURSE_CONDITIONS)
            kyori, track_type, track_cd = course

            # 重賞レースは特定のRaceNumに配置
            race_name = RACE_NAMES.get(race_num, f"{race_num}R")
            if race_num == 1 and month_day == "0105":
                race_name = "中山金杯"
                kyori = "2000"
                track_cd = "01"

            num_horses = random.randint(8, 18)
            hasso_time = f"{9 + race_num}:{random.choice(['00', '10', '25', '35', '50'])}"

            race_data = {
                "Year": "2025", "MonthDay": month_day, "JyoCD": "06",
                "Kaiji": "01", "Nichiji": nichiji, "RaceNum": f"{race_num:02d}",
                "RaceName": race_name, "Kyori": kyori, "TrackCD": track_cd,
                "TenkoBaba": random.choice(["10", "11", "12", "20"]),  # 晴良/晴稍/曇良/雨良
                "Syubetucd": "11" if race_num <= 8 else "12",  # サラ系2歳/3歳以上
                "JyokenCD": "A1" if race_num == 1 and month_day == "0105" else "XX",
                "HassoTime": hasso_time,
                "LapTime": "",
                "Ninkiord": "",
                "num_horses": num_horses,
            }

            conn.execute(
                """INSERT INTO NL_RA VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    race_data["Year"], race_data["MonthDay"], race_data["JyoCD"],
                    race_data["Kaiji"], race_data["Nichiji"], race_data["RaceNum"],
                    race_data["RaceName"], race_data["Kyori"], race_data["TrackCD"],
                    race_data["TenkoBaba"], race_data["Syubetucd"], race_data["JyokenCD"],
                    race_data["HassoTime"], race_data["LapTime"], race_data["Ninkiord"],
                ),
            )
            race_list.append(race_data)

    conn.commit()
    return race_list


def seed_entries(conn: sqlite3.Connection, race_list: list[dict]) -> None:
    """出走馬データを投入する。"""
    random.seed(42)
    used_ketto = set()
    ketto_counter = 1000

    for race in race_list:
        num_horses = race["num_horses"]
        favorite_idx = random.randint(0, min(2, num_horses - 1))
        odds_list = _random_odds(num_horses, favorite_idx)

        # 人気順にソート用のインデックス
        odds_sorted_indices = sorted(range(num_horses), key=lambda i: odds_list[i])

        horses = random.sample(HORSE_NAMES, min(num_horses, len(HORSE_NAMES)))
        jockeys = random.choices(JOCKEY_NAMES, k=num_horses)

        for i in range(num_horses):
            ketto_counter += 1
            ketto_num = f"{ketto_counter:010d}"

            umaban = f"{i + 1:02d}"
            wakuban = f"{(i // 2) + 1:01d}"
            ninki = str(odds_sorted_indices.index(i) + 1)

            # スピード指数（60〜120、人気馬ほど高い傾向）
            base_speed = 90 - (int(ninki) * 3) + random.randint(-5, 10)
            speed_index = max(50, min(130, base_speed))

            # 着順（人気馬が必ず勝つわけではないリアルな分布）
            time_str = _random_time(race["Kyori"])

            conn.execute(
                """INSERT INTO NL_SE VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    race["Year"], race["MonthDay"], race["JyoCD"],
                    race["Kaiji"], race["Nichiji"], race["RaceNum"],
                    umaban, wakuban, ketto_num,
                    horses[i] if i < len(horses) else f"馬{i+1}",
                    random.choice(["1", "2", "3"]),  # 牡/牝/セン
                    str(random.randint(2, 8)),  # 馬齢
                    str(random.choice([52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0])),  # 斤量
                    jockeys[i],
                    f"オーナー{random.randint(1, 30)}",
                    f"調教師{random.randint(1, 20)}",
                    _random_weight(),
                    time_str,
                    str(random.randint(1, num_horses)),  # 着順
                    ninki,
                    str(random.randint(40, 80)),  # IDM
                    str(speed_index),
                ),
            )

            # 単勝オッズ
            conn.execute(
                """INSERT INTO NL_O1 VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    race["Year"], race["MonthDay"], race["JyoCD"],
                    race["Kaiji"], race["Nichiji"], race["RaceNum"],
                    umaban, str(odds_list[i]), ninki,
                ),
            )

            # 馬マスタ（NL_HR, NL_UM）
            if ketto_num not in used_ketto:
                horse_name = horses[i] if i < len(horses) else f"馬{i+1}"
                sex_cd = random.choice(["1", "2", "3"])
                conn.execute(
                    """INSERT INTO NL_HR VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        ketto_num, horse_name, sex_cd,
                        f"20{random.randint(18, 23)}0{random.randint(1, 9):01d}{random.randint(10, 28):02d}",
                        f"父馬{random.randint(1, 50)}", f"母馬{random.randint(1, 50)}",
                        f"オーナー{random.randint(1, 30)}", f"調教師{random.randint(1, 20)}",
                        "美浦" if random.random() > 0.5 else "栗東",
                    ),
                )
                conn.execute(
                    """INSERT INTO NL_UM VALUES (?,?,?,?,?,?)""",
                    (
                        ketto_num, horse_name, sex_cd,
                        random.choice(["01", "02", "03", "04", "05", "07"]),  # 毛色
                        "01",  # 品種
                        "日本" if random.random() > 0.2 else random.choice(["米国", "英国", "仏国"]),
                    ),
                )
                used_ketto.add(ketto_num)

    conn.commit()


def seed_jockeys(conn: sqlite3.Connection) -> None:
    """騎手マスタを投入する。"""
    for i, name in enumerate(JOCKEY_NAMES):
        conn.execute(
            """INSERT INTO NL_KS VALUES (?,?,?,?,?)""",
            (
                f"{i + 1:05d}", name, "1",  # 1=免許あり
                "1",  # 男性
                f"19{random.randint(80, 99)}{random.randint(1, 12):02d}{random.randint(1, 28):02d}",
            ),
        )
    conn.commit()


def main(db_path: str = "./data/demo.db") -> None:
    """メイン処理：ダミーデータを生成してDBに投入する。"""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # 既存DBを削除して再作成
    if path.exists():
        path.unlink()
        print(f"既存DB削除: {path}")

    conn = sqlite3.connect(str(path))
    try:
        # 1. JVLinkテーブル作成
        create_jvlink_tables(conn)
        print("JVLinkテーブル作成完了")

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

        # 4. 出走馬・オッズデータ投入
        seed_entries(conn, race_list)
        total_entries = conn.execute("SELECT COUNT(*) FROM NL_SE").fetchone()[0]
        print(f"出走馬データ投入: {total_entries}頭")

        # 5. 騎手マスタ投入
        seed_jockeys(conn)
        print(f"騎手マスタ投入: {len(JOCKEY_NAMES)}名")

        # 6. サマリー出力
        print(f"\n=== ダミーデータ生成完了 ===")
        print(f"DBパス: {path.resolve()}")
        for table in ["NL_RA", "NL_SE", "NL_HR", "NL_UM", "NL_KS", "NL_O1"]:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,}件")

    finally:
        conn.close()


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "./data/demo.db"
    main(target)
