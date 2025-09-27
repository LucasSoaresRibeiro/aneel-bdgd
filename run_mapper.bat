@echo off
TITLE ANEEL BDGD Mapper
ECHO --- Running ANEEL BDGD Downloader and Mapper (Full Pipeline) ---
ECHO This will download and process files based on your 'config.py' settings.
ECHO.

REM Check if virtual environment exists
IF NOT EXIST .venv (
    ECHO Virtual environment not found. Please run 'setup_environment.bat' first.
    PAUSE
    EXIT /B 1
)

ECHO Activating the environment...
call .venv\Scripts\activate.bat

ECHO Running the main script (main.py)...
ECHO This may take a long time depending on your filters and internet speed.

set "COMPANY_FILTER_ARG="
set "DATE_FILTER_ARG="
set "GRID_SIZE_ARG="
set "OUTPUT_FILENAME_FOR_MAIN_PY="
set "OUTPUT_FILENAME_FOR_START_CMD=output\aneel_bdgd.html" REM Default from config.py

IF NOT "%1"=="" (
    SET "COMPANY_FILTER_ARG=--company_filter %1"
)
IF NOT "%2"=="" (
    SET "DATE_FILTER_ARG=--date_filter %2"
)
IF NOT "%3"=="" (
    SET "GRID_SIZE_ARG=--grid_size %3"
)
IF NOT "%4"=="" (
    SET "OUTPUT_FILENAME_FOR_MAIN_PY=--output_filename %4"
    SET "OUTPUT_FILENAME_FOR_START_CMD=%4"
)

python main.py %COMPANY_FILTER_ARG% %DATE_FILTER_ARG% %GRID_SIZE_ARG% %OUTPUT_FILENAME_FOR_MAIN_PY%

ECHO.
ECHO --- Script Finished ---
start "" "%OUTPUT_FILENAME_FOR_START_CMD%"
PAUSE