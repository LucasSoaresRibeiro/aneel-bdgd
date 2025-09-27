# --------------------------------------------------------------------------
# CONFIGURATION FILE FOR ANEEL BDGD DOWNLOADER AND MAPPER
# --------------------------------------------------------------------------

# --- Download Settings ---
# Filter datasets by company name (e.g., 'Energisa', 'CELESC'). Leave as "" to download all.
# COMPANY_FILTER = "ENERGISA_AC"
COMPANY_FILTER = "CERAL_ARARUAMA"
# COMPANY_FILTER = "LIGHT"
# COMPANY_FILTER = ""

# Filter datasets by a tag or string in the filename (e.g., '2023-12-31', '2022'). Leave as "" for no date filter.
DATE_FILTER = "2024"
# DATE_FILTER = "2024-12-31"

# Set a maximum number of files to download. Set to None to download all matching files.
MAX_DOWNLOADS = None # Example: 5 to test with 5 files

# --- Data Processing Settings ---
# Names of the layers and columns for the join operation.
SPATIAL_LAYER = 'PONNOT'
SPATIAL_KEY = 'COD_ID'      # Key in the PONNOT spatial layer
CONSUMER_KEY = 'PN_CON'     # Key in the UC_tab tables
CONSUMER_LAYERS = ['UCAT_tab', 'UCMT_tab', 'UCBT_tab']

# --- Thematic Map Settings (NEW) ---
# The column from the data to be aggregated and displayed on the map.
# This value is used if AGGREGATION_FUNCTION is 'sum' or 'mean'.
# Common options: 'ENE_TOT' (total energy), 'DEM' (demand/installed capacity).
AGGREGATION_COLUMN = 'ENE_TOT'

# The function used to aggregate the data within each grid cell.
# Valid options are: 'sum', 'mean', or 'count'.
# If you use 'count', the map will show the number of consumer units per cell,
# and the AGGREGATION_COLUMN value will be ignored.
AGGREGATION_FUNCTION = 'sum'

# --- Grid Map Parameters ---
# The size of each square grid cell for the aggregation map.
# GRID_CELL_SIZE = 10  # 10 = 10km x 10km grid
# GRID_CELL_SIZE = 5  # 5 = 5km x 5km grid
# GRID_CELL_SIZE = 3  # 3 = 3km x 3km grid
GRID_CELL_SIZE = 1  # 1 = 1km x 1km grid

# The units for the grid cell size ('meters' or 'degrees'). 'meters' is recommended.
GRID_CELL_UNITS = 'meters'

# The target EPSG code for a projected coordinate system (required for 'meters').
# Example: 'EPSG:31983' for SIRGAS 2000 / UTM Zone 23S (covers a large part of Brazil).
TARGET_CRS_EPSG = 'EPSG:31983'

# --- Folder and File Settings ---
# Names of the local directories for storing data.
DOWNLOAD_DIR = "data/downloads"
EXTRACT_DIR = "data/extracted"

# Name of the final interactive map file.
OUTPUT_FILENAME = "output/aneel_bdgd.html"