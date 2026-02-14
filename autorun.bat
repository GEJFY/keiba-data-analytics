@echo off
REM ============================================================
REM  Keiba Data Analytics - Race Day Auto Pipeline
REM  Windows Task Scheduler から呼び出す用
REM
REM  設定:
REM    1. タスクスケジューラ → 基本タスクの作成
REM    2. トリガー: 毎週 土曜・日曜 09:00
REM    3. 操作: プログラムの開始 → このファイルのフルパス
REM    4. 条件: 電源接続時のみ（推奨）
REM
REM  追加引数はそのまま run_pipeline.py に渡される
REM  例: autorun.bat --dry-run
REM ============================================================

cd /d "%~dp0"

set "VENV_PYTHON=.venv\Scripts\python.exe"

if not exist "%VENV_PYTHON%" (
    echo [ERROR] .venv not found. Run setup.bat first. >> logs\pipeline.log
    exit /b 1
)

REM ログディレクトリ確保
if not exist "logs" mkdir "logs"

echo [%date% %time%] Pipeline started >> logs\pipeline.log

"%VENV_PYTHON%" scripts\run_pipeline.py --full %* >> logs\pipeline.log 2>&1

echo [%date% %time%] Pipeline finished (exit=%ERRORLEVEL%) >> logs\pipeline.log
