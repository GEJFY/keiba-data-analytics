# Keiba Data Analytics

競馬定量投資プラットフォーム — 卍指数方式バリュー投資戦略

## 概要

JRA-VAN DataLabの競馬データを定量的に分析し、期待値ベースの投資判断を支援・自動化するシステム。

## セットアップ

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
```

## テスト実行

```bash
pytest tests/ -m unit -v
```

## 技術スタック

- Python 3.11+
- Streamlit + Plotly（ダッシュボード）
- LLM Gateway（Azure AI Foundry + GCP Vertex AI）
- SQLite（JVLinkToSQLite連携）

詳細は [Keiba_Data_Analytics_技術仕様書.md](Keiba_Data_Analytics_技術仕様書.md) を参照。
