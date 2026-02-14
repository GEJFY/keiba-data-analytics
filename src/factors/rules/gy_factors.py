"""GY指数方式 初期ファクタールール定義。

技術仕様書セクション1.3の10カテゴリーに基づく初期ファクター群。
各ファクターはsql_expressionフィールドにPython式を格納し、
ScoringEngine経由でevaluator.evaluate_rule()で評価される。

対応データソース: JVLinkToSQLite の NL_SE_RACE_UMA / NL_RA_RACE テーブル。
IDM/SpeedIndex は JVLink に存在しないため、DMJyuni（JRA公式マイニング予想順位）
および HaronTimeL3（上がり3F）を代替利用する。

式の中で使用可能な変数:
    馬データ: Umaban, Wakuban, SexCD, Barei, Futan, Ninki, KakuteiJyuni, Odds
    レース: Kyori, TrackCD, TenkoCD, RaceNum
    派生: weight, weight_diff, num_entries, gate_position
    JRA AI: dm_rank, last_3f, last_3f_rank
    脚質: running_style, corner4_pos, is_front_runner, is_closer
    前走: prev_jyuni, prev_last_3f, prev_last_3f_rank,
          prev_running_style, prev_corner4_pos, prev_is_front_runner, prev_is_closer
    フラグ: is_favorite, is_longshot, is_turf, is_dirt, is_sprint, is_mile, is_middle, is_long
            is_inner_gate, is_outer_gate, is_male, is_female, is_gelding
"""

# ファクタールール定義
# 各ルールは factor_rules テーブルに登録され、
# ScoringEngine が APPROVED & is_active=1 のものを自動取得して評価する。
GY_INITIAL_FACTORS: list[dict] = [
    # ===== カテゴリー 1: 過去レース評価 =====
    {
        "rule_name": "前走上位着順減点",
        "category": "form",
        "description": "前走1-3着の馬は過大評価されやすいため減点。逆張りの基本。",
        "sql_expression": "-1 if prev_jyuni > 0 and prev_jyuni <= 3 else 0",
        "weight": 1.0,
    },
    {
        "rule_name": "前走大敗加点",
        "category": "form",
        "description": "前走10着以下の馬は過小評価されやすいため加点。",
        "sql_expression": "1 if prev_jyuni > 0 and prev_jyuni >= 10 else 0",
        "weight": 0.8,
    },
    {
        "rule_name": "前走中位安定",
        "category": "form",
        "description": "前走4-6着の馬は安定感があり適正評価されにくい。",
        "sql_expression": "0.5 if prev_jyuni > 0 and 4 <= prev_jyuni <= 6 else 0",
        "weight": 0.5,
    },
    # ===== カテゴリー 2: マイニング予想（DM）=====
    {
        "rule_name": "DM予想上位",
        "category": "dm",
        "description": "JRA公式マイニング予想順位上位3頭に加点。",
        "sql_expression": "1 if dm_rank <= 3 else 0",
        "weight": 1.5,
    },
    {
        "rule_name": "穴馬DM高評価",
        "category": "dm",
        "description": "人気薄だがDM予想順位が高い馬。最大のバリュー源。",
        "sql_expression": "2 if is_longshot and dm_rank <= 5 else 0",
        "weight": 2.0,
    },
    {
        "rule_name": "人気馬DM低評価",
        "category": "dm",
        "description": "人気があるがDM予想順位が低い馬は過大評価。",
        "sql_expression": "-1 if is_favorite and dm_rank > num_entries // 2 else 0",
        "weight": 1.2,
    },
    # ===== カテゴリー 3: 枠順 =====
    {
        "rule_name": "内枠有利(短距離芝)",
        "category": "gate",
        "description": "短距離芝レースでは内枠有利の傾向がある。",
        "sql_expression": "1 if is_turf and is_sprint and is_inner_gate else 0",
        "weight": 0.8,
    },
    {
        "rule_name": "外枠不利(短距離芝)",
        "category": "gate",
        "description": "短距離芝レースの外枠は距離ロスが大きい。",
        "sql_expression": "-0.5 if is_turf and is_sprint and is_outer_gate else 0",
        "weight": 0.6,
    },
    {
        "rule_name": "偶数枠加点",
        "category": "gate",
        "description": "偶数枠はデータ上わずかに回収率が高い傾向。",
        "sql_expression": "0.3 if Wakuban % 2 == 0 else 0",
        "weight": 0.4,
    },
    # ===== カテゴリー 4: 馬体重 =====
    {
        "rule_name": "大幅増減警戒",
        "category": "weight",
        "description": "馬体重の大幅増減(±10kg以上)は体調不安の兆候。",
        "sql_expression": "-1 if abs(weight_diff) >= 10 else 0",
        "weight": 0.7,
    },
    {
        "rule_name": "適正体重維持",
        "category": "weight",
        "description": "馬体重変動が小さい(±2kg以内)のは安定の証。",
        "sql_expression": "0.5 if abs(weight_diff) <= 2 else 0",
        "weight": 0.5,
    },
    # ===== カテゴリー 5: 性別・馬齢 =====
    {
        "rule_name": "牝馬加点(マイル以下)",
        "category": "gender",
        "description": "牝馬はマイル以下で過小評価される傾向。",
        "sql_expression": "0.5 if is_female and (is_sprint or is_mile) else 0",
        "weight": 0.6,
    },
    {
        "rule_name": "高齢馬減点",
        "category": "gender",
        "description": "7歳以上の馬は能力低下リスクが高い。",
        "sql_expression": "-0.5 if Barei >= 7 else 0",
        "weight": 0.5,
    },
    {
        "rule_name": "若馬加点",
        "category": "gender",
        "description": "3歳馬は成長余地があり過小評価されやすい。",
        "sql_expression": "0.5 if Barei == 3 else 0",
        "weight": 0.4,
    },
    # ===== カテゴリー 6: 騎手 =====
    {
        "rule_name": "人気とDM乖離",
        "category": "jockey",
        "description": "人気薄の馬にDM高評価。騎手の技量を反映している可能性。",
        "sql_expression": "1 if Ninki >= 6 and dm_rank <= 5 else 0",
        "weight": 1.0,
    },
    # ===== カテゴリー 7: コース適性 =====
    {
        "rule_name": "ダート短距離人気薄",
        "category": "course",
        "description": "ダート短距離は波乱が多く人気薄に妙味。",
        "sql_expression": "0.5 if is_dirt and is_sprint and Ninki >= 5 else 0",
        "weight": 0.7,
    },
    {
        "rule_name": "長距離適性",
        "category": "course",
        "description": "長距離レースは実力差が出やすいが穴も多い。",
        "sql_expression": "0.5 if is_long and not is_favorite else 0",
        "weight": 0.5,
    },
    # ===== カテゴリー 8: 斤量 =====
    {
        "rule_name": "軽斤量加点",
        "category": "weight",
        "description": "53kg以下の軽斤量は有利に働く場合が多い。",
        "sql_expression": "0.5 if Futan <= 530 else 0",
        "weight": 0.6,
    },
    {
        "rule_name": "重斤量減点",
        "category": "weight",
        "description": "58kg以上の重斤量は負担が大きい。",
        "sql_expression": "-0.5 if Futan >= 580 else 0",
        "weight": 0.5,
    },
    # ===== カテゴリー 9: 多頭数/少頭数 =====
    {
        "rule_name": "多頭数穴馬",
        "category": "other",
        "description": "16頭以上の多頭数レースでは穴馬の期待値が高まる。",
        "sql_expression": "0.5 if num_entries >= 16 and is_longshot else 0",
        "weight": 0.7,
    },
    {
        "rule_name": "少頭数人気馬",
        "category": "other",
        "description": "8頭以下のレースは波乱が少なく実力馬が安定。",
        "sql_expression": "0.5 if num_entries <= 8 and is_favorite else 0",
        "weight": 0.5,
    },
    # ===== カテゴリー 10: 上がり3F（JVLink固有データ）=====
    {
        "rule_name": "上がり3F上位",
        "category": "speed",
        "description": "前走の上がり3Fが出走馬中上位3位以内。末脚の強さ。",
        "sql_expression": "1 if prev_last_3f_rank <= 3 else 0",
        "weight": 1.3,
    },
    {
        "rule_name": "穴馬末脚",
        "category": "speed",
        "description": "人気薄だが末脚が速い馬。バリュー候補。",
        "sql_expression": "1.5 if is_longshot and prev_last_3f_rank <= 5 else 0",
        "weight": 1.5,
    },
    # ===== カテゴリー 11: 脚質（JVLink固有データ）=====
    {
        "rule_name": "逃げ先行有利(短距離)",
        "category": "pace",
        "description": "短距離レースでは逃げ・先行脚質が有利。",
        "sql_expression": "0.5 if prev_is_front_runner and is_sprint else 0",
        "weight": 0.7,
    },
    {
        "rule_name": "差し追込有利(長距離)",
        "category": "pace",
        "description": "長距離レースでは差し・追込脚質が有利。",
        "sql_expression": "0.5 if prev_is_closer and is_long else 0",
        "weight": 0.6,
    },
    {
        "rule_name": "4角好位置",
        "category": "pace",
        "description": "4コーナーで5番手以内の好位置取り。",
        "sql_expression": "0.5 if prev_corner4_pos > 0 and prev_corner4_pos <= 5 else 0",
        "weight": 0.5,
    },
    # ===== 追加ファクター 26-45: 技術仕様書ターゲット達成用 =====
    # --- コース特性 (course) ---
    {
        "rule_name": "芝中距離内枠有利",
        "category": "course",
        "description": "芝中距離(1800-2200m)では内枠が距離ロスを抑えられるため有利。",
        "sql_expression": "0.8 if is_turf and is_middle and is_inner_gate else 0",
        "weight": 0.7,
    },
    {
        "rule_name": "ダート外枠有利",
        "category": "course",
        "description": "ダートレースでは砂を被りにくい外枠が有利な傾向。",
        "sql_expression": "0.5 if is_dirt and is_outer_gate else 0",
        "weight": 0.6,
    },
    {
        "rule_name": "芝長距離スタミナ要求",
        "category": "course",
        "description": "芝長距離は持続力勝負。前走差し追込で好走した馬を評価。",
        "sql_expression": "0.8 if is_turf and is_long and prev_is_closer and prev_jyuni > 0 and prev_jyuni <= 5 else 0",
        "weight": 0.8,
    },
    # --- オッズ・市場シグナル (odds) ---
    {
        "rule_name": "中穴ゾーン妙味",
        "category": "odds",
        "description": "オッズ10-30倍の中穴ゾーンは回収率が最も高い領域。",
        "sql_expression": "1 if Odds >= 100 and Odds <= 300 else 0",
        "weight": 1.0,
    },
    {
        "rule_name": "過剰人気検出",
        "category": "odds",
        "description": "1番人気でオッズ2倍以下は過剰人気。実力以上に支持されている。",
        "sql_expression": "-0.8 if Ninki == 1 and Odds < 20 else 0",
        "weight": 0.9,
    },
    {
        "rule_name": "DM乖離バリュー",
        "category": "odds",
        "description": "DM順位が人気より大幅に高い馬はオッズに織り込まれていないバリュー。",
        "sql_expression": "1.5 if Ninki - dm_rank >= 5 else (0.5 if Ninki - dm_rank >= 3 else 0)",
        "weight": 1.2,
    },
    # --- ペース分析 (pace) ---
    {
        "rule_name": "逃げ馬不在時先行有利",
        "category": "pace",
        "description": "逃げ馬が少ない(前走逃げ馬自身)場合、スローペースで先行有利。",
        "sql_expression": "0.8 if prev_is_front_runner and prev_corner4_pos > 0 and prev_corner4_pos <= 3 else 0",
        "weight": 0.7,
    },
    {
        "rule_name": "末脚強化傾向",
        "category": "pace",
        "description": "前走上がり3Fが良化している馬は調子上向き。",
        "sql_expression": "0.8 if prev_last_3f_rank > 0 and prev_last_3f_rank <= 3 and prev_is_closer else 0",
        "weight": 0.8,
    },
    # --- 斤量分析 (weight) ---
    {
        "rule_name": "斤量対人気乖離",
        "category": "weight",
        "description": "軽斤量なのに人気薄の馬はハンデ面で恵まれている穴候補。",
        "sql_expression": "1.0 if Futan <= 530 and Ninki >= 8 else 0",
        "weight": 0.9,
    },
    {
        "rule_name": "重斤量人気馬危険",
        "category": "weight",
        "description": "重斤量(57kg以上)で1-3番人気の馬は実力あっても斤量負けリスク。",
        "sql_expression": "-0.5 if Futan >= 570 and Ninki <= 3 else 0",
        "weight": 0.6,
    },
    # --- 複合ファクター (combo) ---
    {
        "rule_name": "穴馬三重シグナル",
        "category": "combo",
        "description": "DM上位+末脚上位+人気薄の三重シグナルは最強バリュー。",
        "sql_expression": "2.0 if dm_rank <= 5 and prev_last_3f_rank <= 5 and is_longshot else 0",
        "weight": 1.5,
    },
    {
        "rule_name": "内枠先行短距離複合",
        "category": "combo",
        "description": "短距離で内枠かつ前走先行脚質は高い期待値。",
        "sql_expression": "1.0 if is_sprint and is_inner_gate and prev_is_front_runner else 0",
        "weight": 0.9,
    },
    {
        "rule_name": "DM好走前走敗退",
        "category": "combo",
        "description": "前走負けたがDMは高評価。巻き返し候補の複合シグナル。",
        "sql_expression": "1.2 if prev_jyuni > 0 and prev_jyuni >= 7 and dm_rank <= 4 else 0",
        "weight": 1.3,
    },
    # --- 前走間隔・クラス変動 (form) ---
    {
        "rule_name": "前走好走後人気落ち",
        "category": "form",
        "description": "前走5着以内だが今回人気薄の馬。クラス変動で過小評価の可能性。",
        "sql_expression": "0.8 if prev_jyuni > 0 and prev_jyuni <= 5 and Ninki >= 7 else 0",
        "weight": 0.8,
    },
    {
        "rule_name": "前走大敗DM復活",
        "category": "form",
        "description": "前走10着以下から今回DM上位評価。条件替わりで復活候補。",
        "sql_expression": "1.0 if prev_jyuni > 0 and prev_jyuni >= 10 and dm_rank <= 5 else 0",
        "weight": 1.0,
    },
    # --- 天候・馬場 (other) ---
    {
        "rule_name": "重馬場ダート差し警戒",
        "category": "other",
        "description": "ダート戦で前走差し追込脚質は重馬場時にパワー不足リスク。",
        "sql_expression": "-0.3 if is_dirt and prev_is_closer and TenkoCD >= 2 else 0",
        "weight": 0.5,
    },
    # --- 多頭数・レース番号 (other) ---
    {
        "rule_name": "最終レース波乱傾向",
        "category": "other",
        "description": "メインレース後の最終レース(11-12R)は波乱が多い傾向。穴馬加点。",
        "sql_expression": "0.5 if RaceNum >= 11 and is_longshot else 0",
        "weight": 0.6,
    },
    # --- セン馬 (gender) ---
    {
        "rule_name": "セン馬安定力",
        "category": "gender",
        "description": "セン馬は気性が安定しており、特にダートで堅実な走りをする傾向。",
        "sql_expression": "0.5 if is_gelding and is_dirt else 0",
        "weight": 0.5,
    },
    {
        "rule_name": "セン馬高齢健在",
        "category": "gender",
        "description": "セン馬は衰えが遅く、7歳以上でも走れる馬が多い。高齢減点を相殺。",
        "sql_expression": "0.5 if is_gelding and Barei >= 7 else 0",
        "weight": 0.6,
    },
]
