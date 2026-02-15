# Keiba Data Analytics — Claude Code Instructions

## Project Overview
競馬定量投資プラットフォーム — GY指数方式バリュー投資戦略。
JVLinkから取得した競馬データをSQLiteに格納し、ファクター分析・スコアリング・バックテスト・自動投票まで一貫して実行する。

## Architecture

```
src/
├── data/           # DB接続、JVLink同期、バリデーション
├── factors/        # ファクター定義・レジストリ・ライフサイクル管理
├── scoring/        # スコアリングエンジン、Weight最適化、キャリブレーション
├── backtest/       # バックテストエンジン、メトリクス、ウォークフォワード
├── strategy/       # 戦略プラグイン（GY Value、Fixed Stake）
├── betting/        # 投票実行（dryrun/ipatgo/selenium）
├── search/         # ハイパーパラメータ探索（Grid/Bayesian）
├── agents/         # LLMエージェント（分析・提案・レポート）
├── llm_gateway/    # Azure AI Foundry / Vertex AI 接続
├── automation/     # 日次パイプライン
├── notifications/  # Slack/Email通知
├── reporting/      # 税務レポート
└── dashboard/      # Streamlit ダッシュボード
    ├── app.py          # エントリーポイント
    ├── config_loader.py # DB接続・スキーマ自動マイグレーション
    ├── task_manager.py  # バックグラウンドタスク実行
    ├── components/     # 共通UIコンポーネント
    └── pages/          # 各ページ
```

## Development Rules

### Commands
- **テスト**: `python -m pytest tests/ -x -q`
- **Lint**: `ruff check src/ tests/`
- **カバレッジ**: `python -m pytest tests/ --cov=src --cov-fail-under=80`
- **ダッシュボード起動**: `run.bat` → メニュー1
- **ダミーデータ生成**: `python scripts/seed_dummy_data.py`
- **拡張DB初期化**: `python scripts/init_db.py`

### Testing
- テストカバレッジ閾値: **80%** (`pyproject.toml` の `fail_under`)
- `src/dashboard/` はカバレッジ対象外（`omit`設定）
- テストは `tests/test_{module}/` に配置
- CI: GitHub Actions — ruff lint + pytest (Python 3.11/3.12/3.13)

### Database
- **JVLink DB**: JVLinkToSQLite.exe が生成する競馬データDB
- **拡張DB**: `data/extension.db` — ファクター、スコア、バックテスト結果
- `config_loader._ensure_ext_schema()` で起動時に自動マイグレーション
- WALモード使用（OneDrive同期競合対策あり）
- テーブルスキーマは `scripts/init_db.py` が正本

### Config
- `config/config.yaml` は `.gitignore`（秘密情報含む）
- `config/config.example.yaml` がテンプレート
- API keyは `.env` ファイルまたは環境変数で管理

### Key Patterns
- **ファクターライフサイクル**: DRAFT → TESTING → APPROVED → DEPRECATED (→ DRAFTに復帰可能)
- **バックグラウンドタスク**: `TaskManager.submit()` → `show_task_progress()` → `st.toast()`
- **スキーマ変更時**: `init_db.py` と `config_loader._ensure_ext_schema()` の両方を更新
- **Streamlitページ**: `st.rerun()` の前に必ず `st.toast()` でフィードバック

### Code Style
- Python 3.11+、ruff (line-length=120)
- 日本語コメント・文書、英語コード
- 型ヒント必須（mypy strict）
- `N806` は許容（ML慣例のX等）
