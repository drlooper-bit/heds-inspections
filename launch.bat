@echo off
title HEDS Equipment Inspection Portal
echo ======================================================================
echo  HEAVY EQUIPMENT DEALER SOLUTIONS (HEDS)
echo  Equipment Physical Inspection & Diligence Portal
echo ======================================================================
echo.
echo Cleaning up any conflicting HEDS portal servers...
wmic process where "CommandLine like '%%heds_agent_orchestrator.py%%'" call terminate >nul 2>&1
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8081 ^| findstr LISTENING') do taskkill /f /pid %%a 2>nul
taskkill /f /im ssh.exe 2>nul
echo.
echo Starting local API ingestion engine, Google Drive sync, and SSH tunnel...
start "" python heds_agent_orchestrator.py
echo.
echo Launching front-end inspection interface locally...
start "" http://localhost:8081/index.html
echo.
echo System Running. You may close this terminal window.
echo.
