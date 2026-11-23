@echo off
rem Wrapper CMD para generate.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0generate.ps1" %*
exit /b %ERRORLEVEL%
