@echo off
SETLOCAL

echo Running install.bat...
CALL scripts\windows\install.bat

echo Running install-monitoring.bat...
CALL scripts\windows\install-monitoring.bat

echo Running deploy.bat...
CALL scripts\windows\deploy.bat

ENDLOCAL
