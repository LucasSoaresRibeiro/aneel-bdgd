# ANEEL BDGD Downloader and Mapper

This project is a standalone Python application that automates the entire workflow of fetching, processing, and visualizing public energy distribution data from Brazil's National Electric Energy Agency (ANEEL).

It uses the official ANEEL Open Data API to find and download File Geodatabase (FGDB) datasets. To handle massive datasets that won't fit into memory, it **leverages a powerful, disk-based SQLite database with the SpatiaLite extension**. This approach ensures the pipeline is scalable and has low memory usage. The script processes the data, performs energy consumption analytics, and generates a final, interactive thematic map that can be opened in any web browser.

## Sample Generated Maps

- [aneel_bdgd_ceral_1km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_ceral_1km.html)
- [aneel_bdgd_ceral_5km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_ceral_5km.html)
- [aneel_bdgd_ceral_10km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_ceral_10km.html)
- [aneel_bdgd_ceral_20km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_ceral_20km.html)
- [aneel_bdgd_energisa_10km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_energisa_10km.html)
- [aneel_bdgd_energisa_20km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_energisa_20km.html)
- [aneel_bdgd_energisa_30km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_energisa_30km.html)
- [aneel_bdgd_energisa_50km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_energisa_50km.html)

## Energy Distribution Companies in Brazil

Check these online maps to discover energy distribution companies from Brazil:
- https://sigel.aneel.gov.br/portal/apps/mapviewer/index.html?webmap=49bf6df3ecc9426fa3e32ef25d954d00
  
- https://app.powerbi.com/view?r=eyJrIjoiNDI4ODJiODctYTUyYS00OTgxLWE4MzktMDczYTlmMDU0ODYxIiwidCI6IjQwZDZmOWI4LWVjYTctNDZhMi05MmQ0LWVhNGU5YzAxNzBlMSIsImMiOjR9&pageName=ReportSection

## Features

-   **Automated Downloader:** Fetches data directly from the ANEEL API based on user-defined filters with built-in retries for network reliability.
-   **Data Discovery Tool:** Includes a search script to help users find the correct filter terms (company names, dates) before downloading.
-   **Disk-Based Processing:** Uses a persistent SQLite database with SpatiaLite to process datasets that are too large to fit in RAM, ensuring scalability and low memory usage.
-   **Optimized for Conda:** Includes a smart setup script that automatically creates a dedicated Conda environment and installs all complex dependencies (including SpatiaLite) with a single click.
-   **Fully Configurable:** All settings, from data filters to map themes, are controlled in a single, easy-to-edit `config.py` file.
-   **Optional Reprojection:** Users can choose whether to reproject all source data to WGS84 or to process it in its original coordinate system.
-   **Thematic Mapping:** Generates an interactive grid map with a custom legend. It explicitly styles cells with zero-values to avoid visual errors. Users can configure the map to show:
    -   Sum or mean of total energy (`ENE_TOT`).
    -   Sum or mean of installed capacity (`DEM`).
    -   A simple count of consumer units (density map).
-   **Standalone Project:** Packaged with simple `.bat` scripts for easy setup and execution on Windows.

## Project Structure
ANEEL_Mapper_Project/
│
├── 📁 data/
│ ├── 📄 aneel_data.db (The database file is automatically created here)
│ └── (Downloaded and extracted files are also stored here)
│
├── 📁 output/
│ └── aggregated_map.html (The final interactive map is saved here)
│
├── 📜 config.py # <-- All user settings go here
├── 📜 main.py # <-- Main Python script with all logic
├── 📜 requirements.txt # <-- List of Python packages (used by Conda)
├── 📜 setup_environment.bat # <-- One-time setup script for Conda
├── 📜 run_mapper.bat # <-- Script to run the full analysis
├── 📜 search_datasets.bat # <-- Script to search for data
└── 📜 README.md # <-- This file
code
Code
## Prerequisites

-   **Windows 10/11**
-   **Anaconda or Miniconda** installed. This is required for the automated setup to work correctly. You can download it from [anaconda.com/download](https://www.anaconda.com/download).

## Setup Instructions

This setup only needs to be performed **once**.

1.  **Place the Project:** Unzip or place the `ANEEL_Mapper_Project` folder in a convenient location (e.g., your Desktop or `C:\Projects`).

2.  **Run the Setup Script:** Double-click the `setup_environment.bat` file.
    -   A command prompt window will appear.
    -   The script will automatically detect your Conda installation and create a new, isolated environment named `aneel_mapper_env`.
    -   It will then install all required packages (like `geopandas`, `folium`, and the critical `libspatialite` library) into this environment. This may take several minutes.
    -   Once it's finished, you will see a "SETUP COMPLETE!" message. Press any key to close the window.

## How to Use

The project is designed to be a simple 3-step process: **Search, Configure, Run**.

### Step 1: Search for Datasets (Optional but Recommended)

Before downloading, you can find out what datasets are available.

1.  Double-click `search_datasets.bat`.
2.  The script will prompt you to enter a company name (e.g., `Energisa`) and/or a date (e.g., `2023-12-31`). You can leave these blank to see all available datasets.
3.  The script will list all matching datasets, showing their official `Title` and `Name`.
4.  Note down the exact text you want to use for filtering. For example, for "Energisa Acre", you might use `Energisa` as the company filter and `2023-12-31` as the date filter.

### Step 2: Configure the Analysis

1.  Open the `config.py` file in any text editor (like Notepad, VS Code, etc.).
2.  Edit the settings based on your needs. See the "Configuration Details" section below for a full explanation of each option.
    -   **Set your download filters** (`COMPANY_FILTER`, `DATE_FILTER`).
    -   **Choose your map theme** (`AGGREGATION_COLUMN`, `AGGREGATION_FUNCTION`).
    -   Adjust the grid size (`GRID_CELL_SIZE`) if desired.
3.  **Save and close** the `config.py` file.

### Step 3: Run the Full Pipeline

1.  Double-click `run_mapper.bat`.
    -   This script automatically activates the correct Conda environment and runs the `main.py` script.
    -   You can also run it from the command line and pass optional parameters to override the settings in `config.py`.
    -   **Examples:**
        -   `.\run_mapper.bat "CERAL_ARARUAMA" "2024" "1" "output/aneel_bdgd_ceral_1km.html"`
        -   `.\run_mapper.bat "CERAL_ARARUAMA" "2024" "5" "output/aneel_bdgd_ceral_5km.html"`
        -   `.\run_mapper.bat "CERAL_ARARUAMA" "2024" "10" "output/aneel_bdgd_ceral_10km.html"`
        -   `.\run_mapper.bat "CERAL_ARARUAMA" "2024" "20" "output/aneel_bdgd_ceral_20km.html"`
        -   `.\run_mapper.bat "ENERGISA_AC" "2023-12-31" "10" "output/aneel_bdgd_energisa_10km.html"`
        -   `.\run_mapper.bat "ENERGISA_AC" "2023-12-31" "20" "output/aneel_bdgd_energisa_20km.html"`
        -   `.\run_mapper.bat "ENERGISA_AC" "2023-12-31" "30" "output/aneel_bdgd_energisa_30km.html"`
        -   `.\run_mapper.bat "ENERGISA_AC" "2023-12-31" "50" "output/aneel_bdgd_energisa_50km.html"`
        -   `.\run_mapper.bat --company_filter "ENERGISA"` (only overrides the company filter)
2.  The script will start the full process based on your `config.py` settings (or overridden parameters):
    -   It will download the filtered `.zip` files.
    -   It will extract the `.gdb` folders.
    -   It will process and load all data into the SQLite database.
    -   It will generate the final interactive map.
3.  When the script is finished, you will see a "SUCCESS" message.

### Step 4: View the Output

1.  Navigate to the `output` folder inside the project directory.
2.  You will find your `.html` map file.
3.  **Double-click this file** to open it in your default web browser (Chrome, Firefox, Edge). You can now zoom, pan, and explore your interactive map.

---

## Configuration Details (`config.py`)

| Setting                  | Description                                                                                                                                                             | Example                               |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| `COMPANY_FILTER`         | Filters datasets by a keyword in the title, name, or tags. Leave as `""` to include all companies.                                                                      | `"Energisa"`                          |
| `DATE_FILTER`            | Filters datasets by a string in the filename. Useful for selecting a specific year or date. Leave as `""` for all dates.                                                | `"2023-12-31"`                        |
| `MAX_DOWNLOADS`          | Limits the number of files to download. Useful for testing. Set to `None` to download all matching files.                                                                 | `5`                                   |
| `REPROJECT_TO_WGS84`     | `True`: Reprojects all data to WGS84 (EPSG:4326) for global consistency. `False`: Uses the original CRS from the source files (faster, but only works if all files share the same CRS). | `True`                                |
| `AGGREGATION_COLUMN`     | The data column to be visualized on the map. Ignored if `AGGREGATION_FUNCTION` is `'count'`. Options: `'ENE_TOT'`, `'DEM'`.                                               | `'ENE_TOT'`                           |
| `AGGREGATION_FUNCTION`   | The calculation to perform on the aggregation column. Options: `'sum'`, `'mean'`, `'count'`.                                                                              | `'sum'`                               |
| `GRID_CELL_SIZE`         | The size of each grid square in **kilometers**. The script converts this to degrees for map generation.                                                                   | `5.0`                                 |
| `OUTPUT_FILENAME`        | The path and name for the final output map.                                                                                                                             | `"output/aggregated_map.html"`        |

## License

This project is licensed under the MIT License.