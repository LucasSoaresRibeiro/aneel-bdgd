@echo off
setlocal

ECHO ---------------------------------------------------
ECHO --- ANEEL BDGD Mapper Conda Environment Setup ---
ECHO ---------------------------------------------------
ECHO This script will create a dedicated and isolated Conda environment
ECHO for this project, installing all necessary packages automatically.
ECHO.

REM --- Step 1: Verify Conda is installed and accessible ---
ECHO Verifying that Conda is available...
conda --version >nul 2>nul
IF %ERRORLEVEL% NEQ 0 (
    ECHO.
    ECHO ERROR: Conda not found in your system's PATH.
    ECHO This script requires Anaconda or Miniconda.
    ECHO Please install it from https://www.anaconda.com/download and try again.
    ECHO.
    PAUSE
    EXIT /B 1
)
ECHO Conda installation found.
ECHO.

REM --- Step 2: Define and Create the Conda Environment ---
SET "ENV_NAME=aneel_mapper_env"
ECHO The Conda environment will be named: %ENV_NAME%
ECHO.

REM Check if the environment already exists to avoid errors and save time
conda env list | findstr /B /C:"%ENV_NAME% " >nul
IF %ERRORLEVEL% EQU 0 (
    ECHO Environment '%ENV_NAME%' already exists. Skipping creation.
) ELSE (
    ECHO Creating new Conda environment '%ENV_NAME%' with Python 3.9...
    ECHO This may take a few minutes. Please be patient.
    conda create --name %ENV_NAME% python=3.9 -y
    IF %ERRORLEVEL% NEQ 0 (
        ECHO ERROR: Failed to create the Conda environment. Please check your Conda installation.
        PAUSE
        EXIT /B 1
    )
)
ECHO.

REM --- Step 3: Install All Packages in One Go ---
ECHO Installing all required packages into '%ENV_NAME%'...
ECHO This includes libspatialite and all Python dependencies from the conda-forge channel.
ECHO This is the most reliable method and may also take a few minutes.
conda install --name %ENV_NAME% -c conda-forge libspatialite geopandas fiona pyproj shapely folium tqdm pandas matplotlib sqlalchemy geoalchemy2 requests -y
IF %ERRORLEVEL% NEQ 0 (
    ECHO ERROR: Failed to install packages into the Conda environment.
    ECHO Please check your internet connection and try again.
    PAUSE
    EXIT /B 1
)
ECHO.

REM --- Step 4: Final Instructions ---
ECHO -----------------------------------------------------------------
ECHO                       SETUP COMPLETE!
ECHO -----------------------------------------------------------------
ECHO An isolated environment named '%ENV_NAME%' has been created and configured.
ECHO.
ECHO TO RUN THE PROJECT, FOLLOW THESE STEPS:
ECHO.
ECHO 1. Open a new Anaconda Prompt (or terminal).
ECHO.
ECHO 2. Activate the new environment with this exact command:
ECHO    conda activate %ENV_NAME%
ECHO.
ECHO 3. Navigate to your project directory (e.g., cd C:\path\to\your\project).
ECHO.
ECHO 4. Run the main Python script:
ECHO    python main.py
ECHO.
ECHO -----------------------------------------------------------------
ECHO.

endlocal
PAUSE