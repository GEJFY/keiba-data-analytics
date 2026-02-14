@echo off
setlocal EnableDelayedExpansion

REM ============================================================
REM  Keiba Data Analytics - Launcher
REM  Double-click to start
REM
REM  Usage:
REM    Double-click  -> select from menu
REM    run.bat dashboard  -> start dashboard
REM    run.bat test       -> run tests
REM    run.bat demo       -> run demo
REM    run.bat seed       -> regenerate dummy data
REM ============================================================

cd /d "%~dp0"
set "VENV_PYTHON=.venv\Scripts\python.exe"

REM --- Check setup ---
if not exist "%VENV_PYTHON%" (
    echo.
    echo   .venv not found.
    echo   Run setup.bat first.
    echo.
    pause
    goto :eof
)

REM --- Command line args skip menu ---
if /i "%~1"=="dashboard" goto :dashboard
if /i "%~1"=="test" goto :test
if /i "%~1"=="demo" goto :demo
if /i "%~1"=="seed" goto :seed
if not "%~1"=="" (
    echo [ERROR] Unknown command: %~1
    echo Usage: run.bat [dashboard^|test^|demo^|seed]
    echo.
    pause
    goto :eof
)

REM --- Menu ---
:menu
echo.
echo ============================================================
echo   Keiba Data Analytics
echo ============================================================
echo.
echo   1. Dashboard        (Streamlit)
echo   2. Run tests        (pytest)
echo   3. Demo scenario
echo   4. Regenerate data  (demo.db)
echo   0. Exit
echo.
set /p "choice=  Select [1]: "

REM Default: 1 (Dashboard)
if "%choice%"=="" set "choice=1"
if "%choice%"=="1" goto :dashboard
if "%choice%"=="2" goto :test
if "%choice%"=="3" goto :demo
if "%choice%"=="4" goto :seed
if "%choice%"=="0" goto :eof
echo   Please enter 0-4
goto :menu

REM ============================================================

:dashboard
echo.
echo   Starting dashboard...
REM Kill any existing process on port 8501 to avoid "port not available" error
for /f "tokens=5" %%p in ('netstat -aon ^| findstr ":8501.*LISTENING"') do taskkill /F /PID %%p >nul 2>&1
echo   Open http://localhost:8501 in your browser
echo   Press Ctrl+C to stop
echo.
".venv\Scripts\streamlit.exe" run src\dashboard\app.py --server.port 8501
goto :done

:test
echo.
echo   Running tests...
echo.
"%VENV_PYTHON%" -m pytest tests/ -v --tb=short
goto :done

:demo
if not exist "data\demo.db" (
    echo.
    echo   [INFO] Generating dummy data first...
    "%VENV_PYTHON%" scripts\seed_dummy_data.py
)
echo.
echo   Running demo scenario...
echo.
"%VENV_PYTHON%" scripts\demo_scenario.py
goto :done

:seed
if exist "data\demo.db" (
    echo.
    echo   Deleting old demo.db and regenerating...
    del "data\demo.db"
)
echo.
echo   Generating dummy data...
echo.
"%VENV_PYTHON%" scripts\seed_dummy_data.py
echo.
echo   [OK] Done
goto :done

:done
echo.
echo ============================================================
echo   Complete!
echo ============================================================
echo.
pause
