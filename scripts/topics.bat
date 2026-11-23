@echo off
rem Wrapper CMD para topics.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0topics.ps1" %*
exit /b %ERRORLEVEL%
