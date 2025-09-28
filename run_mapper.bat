@echo off
setlocal

ECHO --- Running ANEEL BDGD Downloader and Mapper (Full Pipeline) ---
ECHO This will download and process files based on your 'config.py' settings.
ECHO Command-line arguments will override the settings in the config file.
ECHO.

REM --- Define the name of the Conda environment ---
SET "ENV_NAME=aneel_mapper_env"

REM --- Activate the Conda environment ---
ECHO Activating the environment...
call conda activate %ENV_NAME%
IF %ERRORLEVEL% NEQ 0 (
    ECHO.
    ECHO ERROR: Failed to activate the Conda environment '%ENV_NAME%'.
    ECHO Please ensure you have run 'setup_environment.bat' successfully.
    PAUSE
    EXIT /B 1
)
ECHO.

ECHO Running the main script (main.py)...
ECHO This may take a long time depending on your filters and internet speed.

REM --- Build the command dynamically to handle optional arguments ---
set "CMD=python main.py"

if not "%~1"=="" (
    set "CMD=%CMD% --company_filter "%~1""
)
if not "%~2"=="" (
    set "CMD=%CMD% --date_filter "%~2""
)
if not "%~3"=="" (
    set "CMD=%CMD% --grid_size "%~3""
)
if not "%~4"=="" (
    set "CMD=%CMD% --output_filename "%~4""
)

REM --- Execute the dynamically built command ---
ECHO.
ECHO Executing command: %CMD%
ECHO.
%CMD%

ECHO.
ECHO --- Script Finished ---

endlocal
PAUSE