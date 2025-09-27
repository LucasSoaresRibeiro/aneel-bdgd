@echo off
TITLE ANEEL BDGD Search Tool
ECHO --- ANEEL BDGD Dataset Search Tool ---
ECHO This tool helps you find the correct names and dates to use in your config.py file.
ECHO It will NOT download any files.
ECHO.

REM Check if virtual environment exists
IF NOT EXIST .venv (
    ECHO Virtual environment not found. Please run 'setup_environment.bat' first.
    PAUSE
    EXIT /B 1
)

ECHO Activating the environment...
call .venv\Scripts\activate.bat

ECHO.
set /p COMPANY_FILTER="Enter a company name to search for (e.g., Energisa, Celesc) (leave blank for all): "
set /p DATE_FILTER="Enter a date or year to search for (e.g., 2023-12-31, 2022) (leave blank for all): "
ECHO.

ECHO Searching with Company='%COMPANY_FILTER%' and Date='%DATE_FILTER%'...
python main.py search --company "%COMPANY_FILTER%" --date "%DATE_FILTER%"

ECHO.
ECHO --- Search Finished ---
PAUSE