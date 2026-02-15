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

REM 日付付きログファイル（ローテーション）
set "LOG_FILE=logs\pipeline_%date:~0,4%%date:~5,2%%date:~8,2%.log"

echo [%date% %time%] Pipeline started >> "%LOG_FILE%"

"%VENV_PYTHON%" scripts\run_pipeline.py --full %* >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%date% %time%] Pipeline finished (exit=%EXIT_CODE%) >> "%LOG_FILE%"

REM 古いログを削除（30日超）
forfiles /p logs /m "pipeline_*.log" /d -30 /c "cmd /c del @path" >nul 2>&1

exit /b %EXIT_CODE%
