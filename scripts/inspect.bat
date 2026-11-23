@echo off
rem Wrapper CMD para inspect.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0inspect.ps1" %*
exit /b %ERRORLEVEL%
