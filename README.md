# ANEEL BDGD Downloader and Mapper

This project is a standalone Python application that automates the entire workflow of fetching, processing, and visualizing public energy distribution data from Brazil's National Electric Energy Agency (ANEEL).

It uses the official ANEEL Open Data API to find and download File Geodatabase (FGDB) datasets, processes the raw data in memory to join consumer information with spatial locations, performs energy consumption analytics, and generates a final, interactive thematic map that can be opened in any web browser.

## Sample Generated Maps

- [aneel_bdgd_ceral_1km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_ceral_1km.html)
- [aneel_bdgd_ceral_20km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_ceral_20km.html)
- [aneel_bdgd_ceral_40km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_ceral_40km.html)
- [aneel_bdgd_energisa_50km](https://lucassoaresribeiro.github.io/aneel-bdgd/output/aneel_bdgd_energisa_50km.html)

## Energy Distribution Companies in Brazil

Check these online maps to discover energy distribution companies from Brazil:
- https://sigel.aneel.gov.br/portal/apps/mapviewer/index.html?webmap=49bf6df3ecc9426fa3e32ef25d954d00
  
- https://app.powerbi.com/view?r=eyJrIjoiNDI4ODJiODctYTUyYS00OTgxLWE4MzktMDczYTlmMDU0ODYxIiwidCI6IjQwZDZmOWI4LWVjYTctNDZhMi05MmQ0LWVhNGU5YzAxNzBlMSIsImMiOjR9&pageName=ReportSection

## Features

-   **Automated Downloader:** Fetches data directly from the ANEEL API based on user-defined filters.
-   **Data Discovery Tool:** Includes a search script to help users find the correct filter terms (company names, dates) before downloading.
-   **In-Memory Processing:** No need for a separate database. All data is loaded, joined, and aggregated directly in memory using GeoPandas.
-   **Fully Configurable:** All settings, from data filters to map themes, are controlled in a single, easy-to-edit `config.py` file.
-   **Thematic Mapping:** Generates an interactive grid map. Users can configure the map to show:
    -   Sum or mean of total energy (`ENE_TOT`).
    -   Sum or mean of installed capacity (`DEM`).
    -   A simple count of consumer units (density map).
-   **Metric Grids:** Automatically reprojects data to a local UTM zone to allow for intuitive grid cell sizes in meters (e.g., 1km x 1km squares).
-   **Standalone Project:** Packaged with simple `.bat` scripts for easy setup and execution on Windows, with no Python knowledge required for the end-user.

## Project Structure

```
ANEEL_Mapper_Project/
│
├── 📁 data/
│   └── (This folder is automatically created to store downloaded and extracted files)
│
├── 📁 output/
│   └── aggregated_map.html  (The final interactive map is saved here)
│
├── 📜 config.py                 # <-- All user settings go here
├── 📜 main.py                   # <-- Main Python script with all logic
├── 📜 requirements.txt          # <-- List of Python packages needed
├── 📜 setup_environment.bat      # <-- One-time setup script
├── 📜 run_mapper.bat             # <-- Script to run the full analysis
├── 📜 search_datasets.bat        # <-- Script to search for data
└── 📜 README.md                  # <-- This file
```

## Prerequisites

-   **Windows 10/11**
-   **Python 3.8 or higher** installed.
    -   During installation, **it is crucial to check the box that says "Add Python to PATH"**.

## Setup Instructions

This setup only needs to be performed **once**.

1.  **Place the Project:** Unzip or place the `ANEEL_Mapper_Project` folder in a convenient location (e.g., your Desktop or `C:\Projects`).

2.  **Run the Setup Script:** Double-click the `setup_environment.bat` file.
    -   A command prompt window will appear.
    -   The script will automatically create a Python virtual environment inside the project folder (in a directory named `.venv`).
    -   It will then install all the required Python packages (`geopandas`, `folium`, etc.). This may take a few minutes.
    -   Once it's finished, you will see a "Setup Complete!" message. Press any key to close the window.

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
    -   You can also run it from the command line and pass optional parameters to override the settings in `config.py`. The order of parameters is `COMPANY_FILTER`, `DATE_FILTER`, `GRID_CELL_SIZE`, and `OUTPUT_FILENAME`. If you want to skip a parameter but provide subsequent ones, use empty quotes `""`.
    -   **Examples:**
        -   `.\run_mapper.bat "CERAL_ARARUAMA" "2024-12-31" "40" "output/aneel_bdgd_ceral_40km.html"`
        -   `.\run_mapper.bat "ENERGISA_AC" "2023-12-31" "50" "output/aneel_bdgd_energisa_50km.html"`
        -   `.\run_mapper.bat "Light" "2024-12-31" "50" "output/aneel_bdgd_light_50km.html"`
        -   `.\run_mapper.bat "Energisa"` (only overrides COMPANY_FILTER)
2.  The script will activate the environment and start the full process based on your `config.py` settings (or overridden parameters):
    -   It will download the filtered `.zip` files (this can take a long time).
    -   It will extract the `.gdb` folders.
    -   It will process and aggregate all the data in memory.
    -   It will generate the final interactive map.
3.  When the script is finished, you will see a "SUCCESS" message.

### Step 4: View the Output

1.  Navigate to the `output` folder inside the project directory.
2.  You will find an `aggregated_map.html` file.
3.  **Double-click this file** to open it in your default web browser (Chrome, Firefox, Edge). You can now zoom, pan, and explore your interactive map.

---

## Configuration Details (`config.py`)

| Setting                  | Description                                                                                                                                                             | Example                               |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| `COMPANY_FILTER`         | Filters datasets by a keyword in the title or name. Leave as `""` to include all companies.                                                                             | `"Energisa"`                          |
| `DATE_FILTER`            | Filters datasets by a string in the filename. Useful for selecting a specific year or date. Leave as `""` for all dates.                                                | `"2023-12-31"`                        |
| `MAX_DOWNLOADS`          | Limits the number of files to download. Useful for testing. Set to `None` to download all matching files.                                                                 | `5`                                   |
| `AGGREGATION_COLUMN`     | The data column to be visualized on the map. Ignored if `AGGREGATION_FUNCTION` is `'count'`. Options: `'ENE_TOT'`, `'DEM'`.                                               | `'ENE_TOT'`                           |
| `AGGREGATION_FUNCTION`   | The calculation to perform on the aggregation column. Options: `'sum'`, `'mean'`, `'count'`.                                                                              | `'sum'`                               |
| `GRID_CELL_SIZE`         | The size of each grid square in the units defined below.                                                                                                                | `5000`                                |
| `GRID_CELL_UNITS`        | The units for the grid size. `'meters'` is recommended for intuitive analysis. Options: `'meters'`, `'degrees'`.                                                         | `'meters'`                            |
| `TARGET_CRS_EPSG`        | The EPSG code for the projected coordinate system to use for metric analysis. **Required if `GRID_CELL_UNITS` is `'meters'`.** Find the code for your region of interest. | `'EPSG:31983'`                        |
| `OUTPUT_FILENAME`        | The path and name for the final output map.                                                                                                                             | `"output/aggregated_map.html"`        |

## License

This project is licensed under the MIT License.
