@echo off
setlocal EnableDelayedExpansion

echo ============================================================
echo   JVLink Data Sync  (Manual)
echo ============================================================
echo.

REM Project root
set "PROJECT_DIR=%~dp0"

REM JVLinkToSQLite exe directory
set "EXE_DIR=%PROJECT_DIR%JVLinkToSQLiteArtifact"
set "EXE=%EXE_DIR%\JVLinkToSQLite.exe"
set "SETTING_XML=%EXE_DIR%\setting.xml"

REM Output database path
set "DB_PATH=%PROJECT_DIR%data\jvlink.db"

echo   exe : %EXE%
echo   DB  : %DB_PATH%
echo   mode: Exec
echo.

if not exist "%EXE%" (
    echo [ERROR] JVLinkToSQLite.exe not found.
    echo         Expected: %EXE%
    pause
    exit /b 1
)

REM Ensure data directory exists
if not exist "%PROJECT_DIR%data" mkdir "%PROJECT_DIR%data"

REM Enable SetupData (historical bulk download) before each run.
REM The exe resets IsEnabled to false after running, so we force it true.
if not exist "%SETTING_XML%" goto skip_setup
echo   Enabling SetupData in setting.xml ...
powershell -NoProfile -Command "$f='%SETTING_XML%'; $x=New-Object System.Xml.XmlDocument; $x.Load($f); $n=$x.SelectSingleNode('//JVSetupDataUpdateSetting/IsEnabled'); if($n -and $n.InnerText -eq 'false'){$n.InnerText='true'; $x.Save($f); echo '  SetupData enabled.'}else{echo '  SetupData already enabled.'}"
:skip_setup
echo.

echo   Starting JVLinkToSQLite...
echo   (JRA-VAN terms dialog may appear on first run)
echo   (SetupData download may take a very long time - please wait)
echo.

cd /d "%EXE_DIR%"
"%EXE%" -m Exec -d "%DB_PATH%" -l Info

echo.
echo   Exit code: %ERRORLEVEL%
echo.
echo ============================================================
echo   Sync complete. You can close this window.
echo ============================================================
pause
