@echo off
rem Wrapper CMD para consume.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0consume.ps1" %*
exit /b %ERRORLEVEL%
