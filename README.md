# Keiba Data Analytics

競馬定量投資プラットフォーム — GY指数方式バリュー投資戦略 (by Go Yoshizawa)

## 概要

JRA-VAN DataLabの競馬データを定量的に分析し、期待値ベースの投資判断を支援・自動化するシステム。

- **GY指数方式**: 約45個のファクターで各馬をスコアリングし、確率校正後にEV(期待値)を算出
- **バリュー投資**: EV > 1.05 の馬のみを投票対象とするエッジ重視戦略
- **Quarter Kelly**: Kelly基準の25%で資金管理、ドローダウン制限・日次上限の多層リスク制御
- **LLM連携**: Azure AI Foundry / GCP Vertex AI 経由のLLMでファクター自動生成・レース分析
- **自動投票**: dryrun / ipatgo CSV / Selenium の3方式に対応、結果自動照合
- **通知**: Slack Webhook / SMTP Email / コンソールの3チャネル通知

## アーキテクチャ

```text
Data Ingestion → Store → Validation → Scoring → Backtest → Betting → Reconcile → Dashboard
(JVLink Sync)  (SQLite)  (Validator)  (GY指数)   (検証)    (自動投票) (結果照合)  (Streamlit)
                                         ↑                                ↓
                                    Calibration                     Notification
                                     Trainer                    (Slack/Email/Log)
```

## セットアップ

### 前提条件

- Python 3.11以上
- JVLinkToSQLite.exe（JRA-VAN DataLab連携、別途入手）
- Git

### かんたんセットアップ（推奨）

```text
setup.bat をダブルクリック
```

以下を自動で実行します（済みの項目はスキップ）:

1. Python バージョン確認
2. 仮想環境 (.venv) 作成
3. 依存パッケージインストール
4. 設定ファイル配置 (config.yaml, .env)
5. ダミーデータ生成 (demo.db)
6. テスト実行 (271件)

### 日常起動

```text
run.bat をダブルクリック → メニューから選択
```

| メニュー | 説明 | コマンドライン |
| --- | --- | --- |
| 1. ダッシュボード | Streamlit UI 起動 | `run.bat dashboard` |
| 2. テスト | pytest 271件実行 | `run.bat test` |
| 3. デモ | デモシナリオ実行 | `run.bat demo` |
| 4. ダミーデータ再生成 | demo.db を再作成 | `run.bat seed` |

### 手動セットアップ

```powershell
git clone https://github.com/GEJFY/keiba-data-analytics.git
cd keiba-data-analytics

python -m venv .venv
.venv\Scripts\activate

pip install -e ".[dev]"

copy config\config.example.yaml config\config.yaml
copy .env.example .env
```

### 設定

`config/config.yaml` と `.env` を編集:

| 項目 | 設定場所 | 説明 |
| --- | --- | --- |
| DB_PATH | config.yaml | JVLinkToSQLite DBのパス |
| AZURE_OPENAI_ENDPOINT | .env | Azure AI Foundry エンドポイント |
| AZURE_OPENAI_API_KEY | .env | Azure AI Foundry APIキー |
| GCP_PROJECT_ID | .env | GCP プロジェクトID |

## テスト

```powershell
# run.bat からも実行可能
run.bat test

# 直接実行
pytest tests/ -v

# カバレッジ付き
pytest tests/ --cov=src --cov-report=term-missing
```

## プロジェクト構成

```text
keiba-data-analytics/
├── setup.bat           # 初回セットアップ（ダブルクリック）
├── run.bat             # 日常起動メニュー（ダブルクリック）
├── src/
│   ├── data/           # DB接続、JVLinkプロバイダー、データ検証、同期管理
│   ├── factors/        # ファクタールール管理・ライフサイクル
│   ├── scoring/        # GY指数スコアリング、確率校正、校正トレーナー
│   ├── strategy/       # 戦略プラグイン基底 + GY_VALUE実装
│   ├── backtest/       # バックテストエンジン、KPI計算
│   ├── betting/        # 資金管理、安全機構、投票実行、結果照合
│   ├── llm_gateway/    # LLM Gateway（Azure/Vertex）
│   ├── agents/         # AIエージェント（分析・提案・レポート・NL・アラート・リサーチ）
│   ├── notifications/  # 通知システム（Slack/Email/Console）
│   └── dashboard/      # Streamlitダッシュボード
├── tests/              # pytestテスト（271テスト）
├── scripts/            # DB初期化、ダミーデータ生成、ファクター登録、デモ
├── config/             # YAML設定ファイル
├── docs/               # 調査資料
└── data/               # DBファイル（gitignore対象）
```

## 主要モジュール

### データ層

| モジュール | 説明 |
| --- | --- |
| `src/data/db.py` | SQLite接続管理（WALモード、自動コミット/ロールバック） |
| `src/data/provider.py` | JVLink DBアダプタ（race_key解析、SQL AS正規化、インジェクション防止） |
| `src/data/validator.py` | データ品質検証（必須テーブル・カラム・NULL値チェック） |
| `src/data/jvlink_sync.py` | JVLinkToSQLite同期管理（exe実行・ログ記録・データ検証） |

### スコアリング・ファクター

| モジュール | 説明 |
| --- | --- |
| `src/factors/registry.py` | ファクタールールCRUD、ステータス遷移管理 |
| `src/factors/lifecycle.py` | 劣化検知、一括DEPRECATED化 |
| `src/scoring/engine.py` | GY指数スコアリング、EV算出、スコア永続化 |
| `src/scoring/evaluator.py` | ファクター式評価（安全なeval + コンテキスト変数構築） |
| `src/scoring/calibration.py` | Platt Scaling / Isotonic Regression 確率校正 |
| `src/scoring/calibration_trainer.py` | 校正モデル訓練（Brier Score / ECE評価） |

### 投資・投票

| モジュール | 説明 |
| --- | --- |
| `src/strategy/plugins/gy_value.py` | GY_VALUE戦略（ScoringEngine→EV判定→Quarter Kelly） |
| `src/betting/bankroll.py` | Quarter Kelly資金管理、ドローダウン制限 |
| `src/betting/safety.py` | 緊急停止、二重投票防止、オッズ急変検知 |
| `src/betting/executor.py` | 投票実行（dryrun/ipatgo CSV/Selenium 3方式） |
| `src/betting/result_collector.py` | レース結果収集・ベット照合（WIN/LOSE判定・払戻計算） |
| `src/backtest/engine.py` | バックテストエンジン |

### AIエージェント

| モジュール | 説明 |
| --- | --- |
| `src/agents/base.py` | AIエージェント基底（LLMフォールバック対応） |
| `src/agents/race_analysis.py` | レース分析エージェント（バリューベット解説） |
| `src/agents/factor_proposal.py` | ファクター提案エージェント（テンプレート+LLM） |
| `src/agents/report.py` | パフォーマンスレポート生成エージェント |
| `src/agents/nl_query.py` | 自然言語クエリエージェント（NL→SQL変換） |
| `src/agents/alert_interpreter.py` | アラート解釈エージェント（オッズ急変・出走取消等） |
| `src/agents/deep_research.py` | ディープリサーチエージェント（馬・騎手・コース分析） |

### インフラ

| モジュール | 説明 |
| --- | --- |
| `src/llm_gateway/gateway.py` | マルチプロバイダーLLM Gateway（Azure/Vertex） |
| `src/notifications/notifier.py` | 通知システム（Slack Webhook/SMTP Email/Console） |
| `src/dashboard/app.py` | Streamlitダッシュボード（5タブ構成） |

## 技術スタック

- **言語**: Python 3.11+
- **DB**: SQLite（WALモード） + JVLinkToSQLite連携
- **ML**: scikit-learn, LightGBM, XGBoost
- **ダッシュボード**: Streamlit + Plotly
- **LLM**: Azure AI Foundry + GCP Vertex AI
- **テスト**: pytest + coverage (閾値80%)
- **CI**: GitHub Actions（Python 3.11/3.12/3.13マトリクス）
- **Lint**: ruff + mypy (strict mode)

## 投票実行フロー

```text
ScoringEngine.score_race()
    → GY_VALUE Strategy.run()
        → BankrollManager.calculate_stake()  (Quarter Kelly)
            → SafetyChecker.pre_check()      (安全検証)
                → BetExecutor.execute_bets()  (dryrun/ipatgo/selenium)
                    → ResultCollector.reconcile_bets()  (結果照合)
                        → Notifier.notify_bet_result()  (通知送信)
```

## ファクターライフサイクル

```text
DRAFT → TESTING → APPROVED → DEPRECATED
  ↑         ↓
  └─────────┘ (差戻し)
```

- **DRAFT**: 新規作成、レビュー待ち
- **TESTING**: バックテスト検証中
- **APPROVED**: 本番運用中 (is_active=1)
- **DEPRECATED**: 無効化済み（劣化検知 or 手動無効化）

## ライセンス

MIT

詳細は [Keiba_Data_Analytics_技術仕様書.md](Keiba_Data_Analytics_技術仕様書.md) を参照。
