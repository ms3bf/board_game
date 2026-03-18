@echo off
setlocal
cd /d "%~dp0"

set PORT=18474
set TAILSCALE_PORT=8081
set TAILSCALE_EXE=C:\Program Files\Tailscale\tailscale.exe
set TAILNET_URL=http://desktop-r6t9c6d.tailff1439.ts.net:%TAILSCALE_PORT%/

echo Configuring Tailscale Serve for %TAILNET_URL%
"%TAILSCALE_EXE%" serve --bg --http=%TAILSCALE_PORT% http://127.0.0.1:%PORT%
if errorlevel 1 (
  echo Tailscale Serve configuration failed. You may need to run this from an elevated terminal once.
)

start http://127.0.0.1:%PORT%
echo Tailnet URL: %TAILNET_URL%
python server.py --host 127.0.0.1 --port %PORT%
if errorlevel 1 (
  py -3 server.py --host 127.0.0.1 --port %PORT%
)

endlocal
