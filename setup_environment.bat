@echo off
ECHO --- ANEEL BDGD Mapper Environment Setup ---

REM Check if Python is installed
python --version >nul 2>nul
IF %ERRORLEVEL% NEQ 0 (
    ECHO Python is not found. Please install Python 3.8+ and ensure it is in your PATH.
    PAUSE
    EXIT /B 1
)

ECHO Creating Python virtual environment in folder '.venv'...
python -m venv .venv
IF %ERRORLEVEL% NEQ 0 (
    ECHO Failed to create virtual environment.
    PAUSE
    EXIT /B 1
)

ECHO Activating the environment and installing packages...
call .venv\Scripts\activate.bat
pip install -r requirements.txt

ECHO.
ECHO --- Setup Complete! ---
ECHO You can now edit 'config.py' and run the project using 'run_mapper.bat'.
PAUSE