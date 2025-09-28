@echo off
setlocal

ECHO --- ANEEL BDGD Dataset Search Tool ---
ECHO.

REM --- Define the name of the Conda environment ---
SET "ENV_NAME=aneel_mapper_env"

REM --- Activate the Conda environment ---
ECHO Activating the '%ENV_NAME%' Conda environment...
call conda activate %ENV_NAME%
IF %ERRORLEVEL% NEQ 0 (
    ECHO.
    ECHO ERROR: Failed to activate the Conda environment.
    ECHO Please ensure you have run 'setup_environment.bat' successfully at least once.
    PAUSE
    EXIT /B 1
)
ECHO.

REM --- Get User Input ---
set /p company_input="Enter a company name to search for (e.g., Energisa, Celesc) (leave blank for all): "
set /p date_input="Enter a date or year to search for (e.g., 2023-12-31, 2022) (leave blank for all): "
ECHO.

ECHO Searching with Company='%company_input%' and Date='%date_input%'...
ECHO.

REM --- Execute the Python Script with the Correct Arguments ---
REM The fix is to use the "--search" flag and named arguments for the filters.
python main.py --search --company_filter "%company_input%" --date_filter "%date_input%"

ECHO.
ECHO --- Search Finished ---

endlocal
PAUSE