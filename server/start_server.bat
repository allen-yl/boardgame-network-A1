@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo 安裝必要套件 (websockets)...
python -m pip install -q -r requirements.txt
echo.
python server.py %*
pause
