@echo off
title Bloomberg Dashboard Refresh
cd /d "%~dp0"

echo ============================================
echo   Bloomberg Dashboard - Data Refresh
echo ============================================
echo.

echo Fetching data from Bloomberg Terminal...
python refresh_data.py
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo WARNING: Some series may have failed. Dashboard will load with available data.
    echo.
)

echo.
echo Starting local server and opening dashboard...
start "" http://localhost:8000
python serve.py
