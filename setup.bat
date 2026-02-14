@echo off
setlocal EnableDelayedExpansion

REM ============================================================
REM  Keiba Data Analytics - Initial Setup
REM  Double-click to run (first time only)
REM  After setup, use run.bat
REM ============================================================

cd /d "%~dp0"
set "VENV_PYTHON=.venv\Scripts\python.exe"
set "VENV_STREAMLIT=.venv\Scripts\streamlit.exe"

echo.
echo ============================================================
echo   Keiba Data Analytics - Setup
echo ============================================================
echo.

REM --- Step 1: Python ---
echo [1/6] Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo   [NG] Python not found. Install Python 3.11+
    goto :fail
)
for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do echo   [OK] Python %%v

REM --- Step 2: Virtual environment ---
echo [2/6] Virtual environment...
if exist "%VENV_PYTHON%" (
    echo   [SKIP] .venv already exists
) else (
    echo   Creating .venv...
    python -m venv .venv
    if errorlevel 1 (
        echo   [NG] Failed to create .venv
        goto :fail
    )
    echo   [OK] .venv created
)

REM --- Step 3: Package install ---
echo [3/6] Packages...
if exist "%VENV_STREAMLIT%" (
    echo   [SKIP] Packages already installed
    echo          To reinstall: delete .venv folder and re-run
) else (
    echo   Installing packages... (first time: 5-10 min)
    echo   -------------------------------------------------
    "%VENV_PYTHON%" -m pip install --upgrade pip
    if errorlevel 1 (
        echo   [NG] pip upgrade failed
        goto :fail
    )
    echo.
    echo   Installing project dependencies...
    echo   -------------------------------------------------
    "%VENV_PYTHON%" -m pip install -e ".[dev]"
    if errorlevel 1 (
        echo   [NG] Package install failed
        goto :fail
    )
    echo   -------------------------------------------------
    echo   [OK] All packages installed
)

REM --- Step 4: Config files ---
echo [4/6] Config files...
set "CONFIG_CREATED=0"
if not exist "config\config.yaml" (
    if exist "config\config.example.yaml" (
        copy "config\config.example.yaml" "config\config.yaml" >nul
        echo   [OK] config/config.yaml created - edit as needed
        set "CONFIG_CREATED=1"
    )
) else (
    echo   [SKIP] config/config.yaml exists
)
if not exist ".env" (
    if exist ".env.example" (
        copy ".env.example" ".env" >nul
        echo   [OK] .env created - set API keys
        set "CONFIG_CREATED=1"
    )
) else (
    echo   [SKIP] .env exists
)

REM --- Step 5: Dummy data ---
echo [5/6] Dummy data...
if not exist "data" mkdir data
if exist "data\demo.db" (
    echo   [SKIP] demo.db already exists
    echo          To regenerate: use run.bat menu
) else (
    echo   Generating dummy data...
    "%VENV_PYTHON%" scripts\seed_dummy_data.py
    if errorlevel 1 (
        echo   [NG] Dummy data generation failed
        goto :fail
    )
    echo   [OK] demo.db generated
)

REM --- Step 6: Tests ---
echo [6/6] Running tests...
echo.
"%VENV_PYTHON%" -m pytest tests/ -q --tb=short
if errorlevel 1 (
    echo.
    echo   [WARNING] Some tests failed
) else (
    echo.
    echo   [OK] All tests PASSED
)

echo.
echo ============================================================
echo   Setup complete!
echo ============================================================
echo.
echo   Next steps:
if "!CONFIG_CREATED!"=="1" (
    echo     1. Edit config/config.yaml (DB path, LLM settings)
    echo     2. Edit .env (API keys)
    echo     3. Double-click run.bat to launch
) else (
    echo     Double-click run.bat to launch
)
echo.
goto :end

:fail
echo.
echo   [ERROR] Setup failed
echo.

:end
pause
