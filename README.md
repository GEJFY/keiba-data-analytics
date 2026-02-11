# Keiba Data Analytics

競馬定量投資プラットフォーム — 卍指数方式バリュー投資戦略

## 概要

JRA-VAN DataLabの競馬データを定量的に分析し、期待値ベースの投資判断を支援・自動化するシステム。

- **卍指数方式**: 約45個のファクターで各馬をスコアリングし、確率校正後にEV(期待値)を算出
- **バリュー投資**: EV > 1.05 の馬のみを投票対象とするエッジ重視戦略
- **Quarter Kelly**: Kelly基準の25%で資金管理、ドローダウン制限・日次上限の多層リスク制御
- **LLM連携**: Azure AI Foundry / GCP Vertex AI 経由のLLMでファクター自動生成・レース分析

## アーキテクチャ

```text
Data Ingestion → Store → Anomaly Detection → Scoring → Backtest → Betting → Dashboard
(JVLink)      (SQLite)  (Validator)        (卍指数)   (検証)    (自動投票)  (Streamlit)
```

## セットアップ

### 前提条件

- Python 3.11以上
- JVLinkToSQLite.exe（JRA-VAN DataLab連携、別途入手）
- Git

### インストール

```powershell
# リポジトリのクローン
git clone https://github.com/GEJFY/keiba-data-analytics.git
cd keiba-data-analytics

# 仮想環境の作成・有効化
python -m venv .venv
.venv\Scripts\activate

# 依存関係のインストール（開発用含む）
pip install -e ".[dev]"

# 拡張テーブルの初期化（JVLinkToSQLite DBパスを指定）
python scripts/init_db.py ./data/extension.db
```

### 設定ファイル

```powershell
# 設定ファイルのコピー
copy config\config.example.yaml config\config.yaml
copy .env.example .env
```

`config/config.yaml` と `.env` を編集し、以下を設定:

| 項目 | 設定場所 | 説明 |
| --- | --- | --- |
| DB_PATH | config.yaml | JVLinkToSQLite DBのパス |
| AZURE_OPENAI_ENDPOINT | .env | Azure AI Foundry エンドポイント |
| AZURE_OPENAI_API_KEY | .env | Azure AI Foundry APIキー |
| GCP_PROJECT_ID | .env | GCP プロジェクトID |

### ダミーデータで試す

```powershell
# ダミーデータの生成
python scripts/seed_dummy_data.py

# デモシナリオの実行
python scripts/demo_scenario.py
```

## テスト

```powershell
# 全テスト実行
pytest tests/ -v

# カバレッジ付き
pytest tests/ --cov=src --cov-report=term-missing

# 単体テストのみ
pytest tests/ -m unit -v

# 特定モジュールのテスト
pytest tests/test_factors/ -v
```

## プロジェクト構成

```text
keiba-data-analytics/
├── src/
│   ├── data/           # DB接続、JVLinkプロバイダー、データ検証
│   ├── factors/        # ファクタールール管理・ライフサイクル
│   ├── scoring/        # 卍指数スコアリングエンジン、確率校正
│   ├── strategy/       # 戦略プラグイン基底
│   ├── backtest/       # バックテストエンジン、KPI計算
│   ├── betting/        # 資金管理(Quarter Kelly)、安全機構
│   ├── llm_gateway/    # LLM Gateway（Azure/Vertex）
│   ├── agents/         # LLMエージェント（ファクター生成等）
│   └── dashboard/      # Streamlitダッシュボード
├── tests/              # pytestテスト（133テスト、カバレッジ93%+）
├── scripts/            # DB初期化、ダミーデータ生成、デモシナリオ
├── config/             # YAML設定ファイル
└── data/               # DBファイル（gitignore対象）
```

## 主要モジュール

| モジュール | 説明 |
| --- | --- |
| `src/data/db.py` | SQLite接続管理（WALモード、自動コミット/ロールバック） |
| `src/data/provider.py` | JVLink DBラッパー（race_key解析、SQLインジェクション防止） |
| `src/factors/registry.py` | ファクタールールCRUD、ステータス遷移管理 |
| `src/factors/lifecycle.py` | 劣化検知、一括DEPRECATED化 |
| `src/scoring/engine.py` | 卍指数スコアリング、EV算出 |
| `src/scoring/calibration.py` | Platt Scaling / Isotonic Regression 確率校正 |
| `src/betting/bankroll.py` | Quarter Kelly資金管理、ドローダウン制限 |
| `src/betting/safety.py` | 緊急停止、二重投票防止、オッズ急変検知 |
| `src/llm_gateway/gateway.py` | マルチプロバイダーLLM Gateway |

## 技術スタック

- **言語**: Python 3.11+
- **DB**: SQLite（WALモード） + JVLinkToSQLite連携
- **ML**: scikit-learn, LightGBM, XGBoost
- **ダッシュボード**: Streamlit + Plotly
- **LLM**: Azure AI Foundry + GCP Vertex AI
- **テスト**: pytest + coverage (目標80%以上)
- **CI**: GitHub Actions（Python 3.11/3.12マトリクス）
- **Lint**: ruff + mypy (strict mode)

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
