@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM === Resolve paths relative to this .bat ===
set "APP_DIR=%~dp0"
REM strip trailing backslash if present
if "%APP_DIR:~-1%"=="\" set "APP_DIR=%APP_DIR:~0,-1%"
set "PYTHONW=%APP_DIR%\.venv\Scripts\pythonw.exe"
set "LOGDIR=%APP_DIR%\logs"
set "LOGFILE=%LOGDIR%\bot_stdout.log"

REM === Make sure logs dir exists ===
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

REM === Bootstrap log so we know the BAT really ran ===
echo [%date% %time%] ===== Bot launch starting =====>> "%LOGFILE%" 2>&1
echo [%date% %time%] APP_DIR="%APP_DIR%" >> "%LOGFILE%" 2>&1
echo [%date% %time%] PYTHONW="%PYTHONW%" >> "%LOGFILE%" 2>&1

REM === Sanity checks ===
if not exist "%PYTHONW%" (
  echo [%date% %time%] ERROR: pythonw.exe not found at "%PYTHONW%" >> "%LOGFILE%" 2>&1
  exit /b 11
)
if not exist "%APP_DIR%\bot.py" (
  echo [%date% %time%] ERROR: bot.py not found at "%APP_DIR%\bot.py" >> "%LOGFILE%" 2>&1
  exit /b 12
)

REM === Run with repo as working dir so .env/events.db resolve ===
pushd "%APP_DIR%" >nul 2>&1
if errorlevel 1 (
  echo [%date% %time%] WARNING: pushd failed; running with absolute path >> "%LOGFILE%" 2>&1
  "%PYTHONW%" "%APP_DIR%\bot.py" >> "%LOGFILE%" 2>&1
  goto :EOF
)

"%PYTHONW%" bot.py >> "%LOGFILE%" 2>&1

popd >nul 2>&1
echo [%date% %time%] ===== Bot process exited =====>> "%LOGFILE%" 2>&1
exit /b 0
