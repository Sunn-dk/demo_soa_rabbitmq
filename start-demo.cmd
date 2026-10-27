@echo off
cd /d "%~dp0"
docker info >nul 2>&1
if errorlevel 1 (
  echo Hay mo Docker Desktop va doi Engine san sang, sau do chay lai.
  pause
  exit /b 1
)
docker compose up -d --build --wait --wait-timeout 180
if errorlevel 1 (
  echo Khoi dong chua thanh cong. Xem thong bao loi o tren.
  pause
  exit /b 1
)
set "DEMO_ADDRESS="
for /f "delims=" %%P in ('docker compose port web 8000') do set "DEMO_ADDRESS=%%P"
if not defined DEMO_ADDRESS set "DEMO_ADDRESS=127.0.0.1:8000"
start "" "http://%DEMO_ADDRESS%"
echo Demo: http://%DEMO_ADDRESS%
echo Kafka tuy chon: docker compose --profile kafka up -d --wait --wait-timeout 240
pause
