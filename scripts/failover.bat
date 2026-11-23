@echo off
rem Wrapper CMD para failover.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0failover.ps1" %*
exit /b %ERRORLEVEL%
