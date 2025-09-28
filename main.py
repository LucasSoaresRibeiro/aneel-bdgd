import os
import sys
import requests
import zipfile
import logging
import pandas as pd
import geopandas as gpd
import fiona
import warnings
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine, text, event
from shapely.geometry import Polygon
import folium
from tqdm import tqdm
import time
import argparse
import pyproj # <-- FIX: Add this import
import branca.colormap as cm

# Import settings from the configuration file
import config

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore', 'Normalized/laundered field name')

def is_valid_geometry(geom):
    """ Check if a geometry is valid and not empty. """
    return geom is not None and not geom.is_empty and geom.is_valid

class ANEEL_Pipeline:
    def __init__(self):
        self.session = requests.Session()
        self.api_base = "https://dadosabertos-aneel.opendata.arcgis.com/api/search/v1/collections/dataset/items"
        self.db_path = os.path.join(config.EXTRACT_DIR, 'aneel_data.db')
        self.engine = None
        self.base_crs = None
        self.spatialite_path = None
        os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)
        os.makedirs(config.EXTRACT_DIR, exist_ok=True)

    def _load_spatialite(self, dbapi_conn, connection_record):
        if self.spatialite_path:
            dbapi_conn.enable_load_extension(True)
            dbapi_conn.load_extension(self.spatialite_path)

    def _initialize_database(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
            logger.info(f"Removed existing database at {self.db_path}")
        possible_paths = ['mod_spatialite', '/usr/lib/x86_64-linux-gnu/mod_spatialite.so', '/usr/local/lib/mod_spatialite.so']
        temp_engine = create_engine(f'sqlite:///')
        for path in possible_paths:
            try:
                with temp_engine.connect() as conn:
                    conn.connection.enable_load_extension(True)
                    conn.connection.load_extension(path)
                self.spatialite_path = path
                logger.info(f"Successfully located SpatiaLite at: {path}")
                break
            except Exception as e:
                logger.debug(f"SpatiaLite not found at {path}: {e}")
        if not self.spatialite_path:
            logger.error("FATAL: SpatiaLite extension could not be found. Please ensure it's installed correctly.")
            sys.exit(1)
        self.engine = create_engine(f'sqlite:///{self.db_path}')
        event.listen(self.engine, "connect", self._load_spatialite)
        with self.engine.connect() as conn:
            conn.execute(text("SELECT InitSpatialMetaData(1);"))
            logger.info("Database initialized with SpatiaLite metadata tables.")

    def search_and_filter(self, company_filter, date_filter):
        logger.info("Searching for all File Geodatabase datasets...")
        all_features = []
        startindex = 1
        max_retries = 3
        while True:
            params = {'type': "File Geodatabase", 'limit': 100, 'startindex': startindex}
            response = None
            for attempt in range(max_retries):
                try:
                    response = self.session.get(self.api_base, params=params, timeout=30)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    logger.warning(f"API request failed on attempt {attempt + 1}/{max_retries}. Retrying... Error: {e}")
                    time.sleep(2)
            if response is None:
                logger.error("API requests failed after multiple retries. Aborting search.")
                return []
            data = response.json()
            features = data.get('features', [])
            if not features: break
            all_features.extend(features)
            if len(features) < 100: break
            startindex += 100
            time.sleep(0.5)
        logger.info(f"Found {len(all_features)} total datasets from API. Applying filters...")
        filtered_features = []
        company_upper = company_filter.upper() if company_filter else ""
        date_upper = date_filter.upper() if date_filter else ""
        for feature in all_features:
            if not feature.get('id'): continue
            props = feature.get('properties', {})
            if company_upper:
                searchable_content = " ".join([props.get('title', ''), props.get('name', ''), " ".join(props.get('tags', []))]).upper()
                if company_upper not in searchable_content: continue
            if date_upper:
                if date_upper not in props.get('name', '').upper(): continue
            filtered_features.append(feature)
        logger.info(f"Found {len(filtered_features)} datasets matching your criteria after filtering.")
        return filtered_features

    def download_and_extract_from_features(self, features):
        extracted_gdb_paths = []
        for i, feature in enumerate(features, 1):
            props = feature['properties']; dataset_id = feature.get('id')
            file_url = f"https://www.arcgis.com/sharing/rest/content/items/{dataset_id}/data"
            filename = "".join(c for c in props.get('name', f"{dataset_id}.zip") if c.isalnum() or c in ('-', '_', '.'))
            if not filename.endswith('.zip'): filename += '.zip'
            zip_path = os.path.join(config.DOWNLOAD_DIR, filename)
            if not os.path.exists(zip_path):
                logger.info(f"Downloading ({i}/{len(features)}): {filename}")
                try:
                    r = self.session.get(file_url, stream=True); r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0))
                    with open(zip_path, 'wb') as f, tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as pbar:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk: f.write(chunk); pbar.update(len(chunk))
                except Exception as e: logger.error(f"Failed to download {filename}. Error: {e}"); continue
            else: logger.info(f"File {filename} already exists, skipping download.")
            extract_path = os.path.join(config.EXTRACT_DIR, os.path.splitext(filename)[0])
            if not os.path.exists(extract_path):
                logger.info(f"Extracting {filename}...");
                try:
                    with zipfile.ZipFile(zip_path, 'r') as z: z.extractall(extract_path)
                except Exception as e: logger.error(f"Failed to extract {filename}. Error: {e}"); continue
            for root, dirs, _ in os.walk(extract_path):
                for d in dirs:
                    if d.endswith('.gdb'): extracted_gdb_paths.append(os.path.join(root, d))
        logger.info(f"Download and extraction complete. Found {len(extracted_gdb_paths)} GDBs.")
        return list(set(extracted_gdb_paths))

    def load_and_union_data(self, gdb_paths):
        logger.info("\n--- Loading and Unioning Data with Disk-Based SQLite/SpatiaLite ---")
        self._initialize_database()
        
        data_was_inserted = False
        
        if config.REPROJECT_TO_WGS84:
            self.base_crs = 'EPSG:4326'
            logger.info(f"Configuration set to reproject all geometries to CRS: {self.base_crs}")
        else:
            self.base_crs = None
            logger.info("Configuration set to use original CRS from source files.")

        def to_wkb_safe(geom):
            if not is_valid_geometry(geom): return None
            try: return geom.wkb
            except Exception: return None

        for gdb_path in gdb_paths:
            logger.info(f"\n--- Processing: {os.path.basename(gdb_path)} ---")
            try:
                with fiona.open(gdb_path, 'r', layer=config.SPATIAL_LAYER) as source:
                    if self.base_crs is None and not config.REPROJECT_TO_WGS84:
                        self.base_crs = source.crs
                        logger.info(f"Base CRS for pipeline established as: {str(self.base_crs)}")

                    spatial_gdf = gpd.GeoDataFrame.from_features(source, crs=source.crs)
                    spatial_gdf = spatial_gdf[spatial_gdf['geometry'].apply(is_valid_geometry)]
                    if spatial_gdf.empty: logger.warning("  - No valid geometries in source. Skipping."); continue

                    if config.REPROJECT_TO_WGS84 and spatial_gdf.crs.to_epsg() != 4326:
                        logger.info(f"  - Reprojecting from {spatial_gdf.crs.name} to EPSG:4326...")
                        spatial_gdf = spatial_gdf.to_crs('EPSG:4326')
                    elif not config.REPROJECT_TO_WGS84 and spatial_gdf.crs != self.base_crs:
                        logger.warning(f"  - CRS mismatch! Expected {str(self.base_crs)} but found {spatial_gdf.crs.name}.")

                    spatial_gdf['geometry'] = spatial_gdf['geometry'].apply(to_wkb_safe)
                    spatial_gdf.dropna(subset=['geometry'], inplace=True)
                    if spatial_gdf.empty: logger.warning("  - No valid geometries after conversion. Skipping."); continue
                    spatial_gdf.to_sql('spatial_temp', self.engine, if_exists='replace', index=False, dtype={'geometry': sqlalchemy.types.BLOB})

                consumer_dfs = [gpd.read_file(gdb_path, layer=t) for t in config.CONSUMER_LAYERS if t in fiona.listlayers(gdb_path)]
                if not consumer_dfs: logger.warning("  - No consumer layers found. Skipping."); continue
                consumer_df = pd.concat(consumer_dfs, ignore_index=True)
                consumer_df.to_sql('consumer_temp', self.engine, if_exists='replace', index=False)

                with self.engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        logger.info("  - Creating indexes on temporary tables for faster joins...")
                        conn.execute(text(f'CREATE INDEX idx_spatial_key ON spatial_temp("{config.SPATIAL_KEY}");'))
                        conn.execute(text(f'CREATE INDEX idx_consumer_key ON consumer_temp("{config.CONSUMER_KEY}");'))

                        inspector = sqlalchemy.inspect(self.engine)
                        if not inspector.has_table("processed_data"):
                            join_query = f"""CREATE TABLE processed_data AS SELECT s.*, c.* FROM spatial_temp AS s INNER JOIN consumer_temp AS c ON s."{config.SPATIAL_KEY}" = c."{config.CONSUMER_KEY}";"""
                        else:
                            join_query = f"""INSERT INTO processed_data SELECT s.*, c.* FROM spatial_temp AS s INNER JOIN consumer_temp AS c ON s."{config.SPATIAL_KEY}" = c."{config.CONSUMER_KEY}";"""
                        
                        logger.info("  - Performing indexed join...")
                        conn.execute(text(join_query))
                        trans.commit()
                        data_was_inserted = True
                        count_query = "SELECT count(*) FROM processed_data"
                        total_records = conn.execute(text(count_query)).scalar()
                        logger.info(f"  - Successfully joined data. Total records now: {total_records}")
                    except Exception as e:
                        trans.rollback(); logger.error(f"  - FAILED to join data in DB. Error: {e}")
            except Exception as e:
                logger.error(f"  - FAILED to process GDB file. Error: {e}")
        
        if not data_was_inserted:
            logger.warning("\nNo data was loaded. Skipping spatial index creation."); return self

        with self.engine.connect() as conn:
            logger.info("Creating spatial index and coordinate columns...")
            try:
                trans = conn.begin()
                srid = pyproj.CRS(self.base_crs).to_epsg()
                conn.execute(text("SELECT DiscardGeometryColumn('processed_data', 'geom');"))
                conn.execute(text(f"SELECT AddGeometryColumn('processed_data', 'geom', {srid}, 'POINT', 2)"))
                conn.execute(text(f"UPDATE processed_data SET geom = GeomFromWKB(geometry, {srid})"))
                conn.execute(text("SELECT CreateSpatialIndex('processed_data', 'geom')"))
                
                # --- NEW: Add and populate latitude and longitude columns ---
                logger.info("  - Pre-calculating longitude and latitude columns...")
                conn.execute(text("ALTER TABLE processed_data ADD COLUMN longitude REAL;"))
                conn.execute(text("ALTER TABLE processed_data ADD COLUMN latitude REAL;"))
                
                # Determine the geometry to use for coordinate extraction (transform if necessary)
                coord_geom = "Transform(geom, 4326)" if srid != 4326 else "geom"
                
                # Populate the new columns
                conn.execute(text(f"UPDATE processed_data SET longitude = ST_X({coord_geom});"))
                conn.execute(text(f"UPDATE processed_data SET latitude = ST_Y({coord_geom});"))

                trans.commit()
                logger.info("Spatial index and coordinate columns created successfully.")
            except Exception as e:
                trans.rollback(); logger.error(f"FAILED to create spatial index/columns. Error: {e}")
        return self

    def process_analytics(self):
        if not self.engine: logger.warning("Skipping analytics: DB not available."); return self
        logger.info("\n--- Executing analytics directly in the database ---")
        ene_cols = [f'ENE_{str(i).zfill(2)}' for i in range(1, 13)]
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(text("ALTER TABLE processed_data ADD COLUMN ENE_TOT REAL;"))
                conn.execute(text("ALTER TABLE processed_data ADD COLUMN ENE_MED REAL;"))
                conn.execute(text("ALTER TABLE processed_data ADD COLUMN DEM REAL;"))
                table_info = pd.read_sql("PRAGMA table_info(processed_data);", conn)
                existing_cols = table_info['name'].tolist()
                for col in ene_cols:
                    if col not in existing_cols:
                        conn.execute(text(f"ALTER TABLE processed_data ADD COLUMN {col} REAL DEFAULT 0;"))
                sum_expression = " + ".join([f"COALESCE(CAST({col} AS REAL), 0)" for col in ene_cols])
                conn.execute(text(f"UPDATE processed_data SET ENE_TOT = {sum_expression};"))
                conn.execute(text("UPDATE processed_data SET ENE_MED = ENE_TOT / 12.0;"))
                conn.execute(text("UPDATE processed_data SET DEM = COALESCE(CAST(CAR_INST AS REAL), 0);"))
                trans.commit()
                logger.info("Analytics complete. Columns ENE_TOT, ENE_MED, DEM created.")
            except Exception as e:
                trans.rollback(); logger.error(f"Failed to process analytics in database. Error: {e}")
        return self

    def generate_grid_map_v1(self, grid_cell_size_arg=None):
        if not self.engine: 
            logger.warning("Cannot generate map: DB not available."); return None
        logger.info("\n--- Generating map using chunk-based grid aggregation ---")
        grid_cell_size_km = grid_cell_size_arg if grid_cell_size_arg is not None else config.GRID_CELL_SIZE
        agg_col = config.AGGREGATION_COLUMN
        agg_func = config.AGGREGATION_FUNCTION.lower()
        needs_transform = pyproj.CRS(self.base_crs).to_epsg() != 4326
        with self.engine.connect() as conn:
            bounds_query_geom = "Transform(geom, 4326)" if needs_transform else "geom"
            query = text(f"SELECT Min(MbrMinX({bounds_query_geom})), Min(MbrMinY({bounds_query_geom})), Max(MbrMaxX({bounds_query_geom})), Max(MbrMaxY({bounds_query_geom})) FROM processed_data;")
            try:
                xmin, ymin, xmax, ymax = conn.execute(query).fetchone()
            except sqlalchemy.exc.OperationalError as e:
                logger.error(f"Could not calculate data bounds. Was the spatial index created correctly? Error: {e}"); return None
        if not all((xmin, ymin, xmax, ymax)):
            logger.warning("Could not determine data bounds from database. Cannot generate map."); return None
        grid_cell_size_deg = grid_cell_size_km / 111.32
        logger.info(f"Generating grid ({grid_cell_size_km}km ≈ {grid_cell_size_deg:.4f} degrees)...")
        grid_cells = [Polygon([(x0, y0), (x0 + grid_cell_size_deg, y0), (x0 + grid_cell_size_deg, y0 + grid_cell_size_deg), (x0, y0 + grid_cell_size_deg)]) for x0 in np.arange(xmin, xmax, grid_cell_size_deg) for y0 in np.arange(ymin, ymax, grid_cell_size_deg)]
        grid_gdf = gpd.GeoDataFrame(grid_cells, columns=['geometry'], crs='EPSG:4326')
        grid_gdf[agg_col] = 0; grid_gdf['point_count'] = 0
        logger.info(f"Aggregating data for {len(grid_gdf)} grid cells...")
        with self.engine.connect() as conn:
            srid = pyproj.CRS(self.base_crs).to_epsg()
            for i, cell in tqdm(grid_gdf.iterrows(), total=len(grid_gdf)):
                cell_wkt = cell.geometry.wkt
                st_contains_geom = f"Transform(GeomFromText('{cell_wkt}', 4326), {srid})" if needs_transform else f"GeomFromText('{cell_wkt}', 4326)"
                query = text(f""" SELECT {agg_func.upper()}({agg_col}) as agg_val, COUNT(*) as p_count FROM processed_data WHERE ST_Contains({st_contains_geom}, geom); """)
                result = conn.execute(query).fetchone()
                agg_val, p_count = result if result else (0, 0)
                if p_count > 0:
                    grid_gdf.at[i, agg_col] = agg_val if agg_val is not None else 0
                    grid_gdf.at[i, 'point_count'] = p_count
        grid_with_data = grid_gdf[grid_gdf['point_count'] > 0].copy()
        if grid_with_data.empty:
            logger.warning("No data points fell within the grid. Cannot create map."); return None
        map_center = [grid_with_data.geometry.centroid.y.mean(), grid_with_data.geometry.centroid.x.mean()]
        m = folium.Map(location=map_center, zoom_start=6, tiles='CartoDB positron')
        non_zero_data = grid_with_data[grid_with_data[agg_col] > 0]
        min_val = non_zero_data[agg_col].min() if not non_zero_data.empty else 0
        max_val = non_zero_data[agg_col].max() if not non_zero_data.empty else 0
        if min_val == max_val: min_val = max_val * 0.9 if max_val > 0 else 0
        colormap = cm.linear.YlOrRd_09.scale(min_val, max_val)
        colormap.caption = f'{agg_func.capitalize()} of {agg_col} per Grid Cell'
        def style_function(feature):
            value = feature['properties'][agg_col]
            if value > 0:
                return {'fillColor': colormap(value), 'color': 'black', 'weight': 0.5, 'fillOpacity': 0.7}
            else:
                return {'fillColor': '#D3D3D3', 'color': 'black', 'weight': 0.5, 'fillOpacity': 0.5}
        folium.GeoJson(grid_with_data, style_function=style_function, name='Aggregated Data',
            tooltip=folium.GeoJsonTooltip(fields=[agg_col, 'point_count'], aliases=[f'{agg_col}:', 'Point Count:'], localize=True)
        ).add_to(m)
        colormap.add_to(m)
        folium.LayerControl().add_to(m)
        return m

    def generate_grid_map_v2_database(self, grid_cell_size_arg=None):
        if not self.engine:
            logger.warning("Cannot generate map: Database engine not available."); return None
        logger.info("\n--- Generating map using high-performance in-database aggregation ---")
        grid_cell_size_km = grid_cell_size_arg if grid_cell_size_arg is not None else config.GRID_CELL_SIZE
        agg_col = config.AGGREGATION_COLUMN
        agg_func = config.AGGREGATION_FUNCTION.lower()
        
        # 1. Get data bounds from the database
        with self.engine.connect() as conn:
            srid = pyproj.CRS(self.base_crs).to_epsg()
            needs_transform = srid != 4326
            bounds_query_geom = f"Transform(geom, 4326)" if needs_transform else "geom"
            query = text(f"SELECT Min(MbrMinX({bounds_query_geom})), Min(MbrMinY({bounds_query_geom})), Max(MbrMaxX({bounds_query_geom})), Max(MbrMaxY({bounds_query_geom})) FROM processed_data;")
            try:
                xmin, ymin, xmax, ymax = conn.execute(query).fetchone()
            except sqlalchemy.exc.OperationalError as e:
                logger.error(f"Could not calculate data bounds. Was the spatial index created correctly? Error: {e}"); return None

        if not all((xmin, ymin, xmax, ymax)):
            logger.warning("Could not determine data bounds from database. Cannot generate map."); return None

        # 2. Create grid cells in Python
        grid_cell_size_deg = grid_cell_size_km / 111.32
        logger.info(f"Generating {grid_cell_size_km}km grid cells...")
        grid_cells = [Polygon([(x0, y0), (x0 + grid_cell_size_deg, y0), (x0 + grid_cell_size_deg, y0 + grid_cell_size_deg), (x0, y0 + grid_cell_size_deg)]) for x0 in np.arange(xmin, xmax, grid_cell_size_deg) for y0 in np.arange(ymin, ymax, grid_cell_size_deg)]
        grid_gdf = gpd.GeoDataFrame(grid_cells, columns=['geometry'], crs='EPSG:4326')
        grid_gdf['grid_id'] = range(len(grid_gdf)) # Add a unique ID for joining

        # 3. Upload grid to a temporary spatial table in the database
        logger.info(f"Uploading {len(grid_gdf)} grid cells to the database for processing...")
        grid_gdf['geometry_wkb'] = grid_gdf['geometry'].apply(lambda geom: geom.wkb)
        grid_gdf.drop(columns='geometry').to_sql('grid_temp', self.engine, if_exists='replace', index=False)

        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(text("SELECT DiscardGeometryColumn('grid_temp', 'geom');"))
                conn.execute(text(f"SELECT AddGeometryColumn('grid_temp', 'geom', 4326, 'POLYGON', 2)"))
                conn.execute(text(f"UPDATE grid_temp SET geom = GeomFromWKB(geometry_wkb, 4326)"))
                conn.execute(text("SELECT CreateSpatialIndex('grid_temp', 'geom')"))
                trans.commit()
                logger.info("Spatial index created on temporary grid table.")
            except Exception as e:
                trans.rollback(); logger.error(f"FAILED to create spatial grid table. Error: {e}"); return None

        # 4. Perform the entire aggregation with a single, powerful SQL query
        logger.info("Performing high-performance spatial join and aggregation... (This may take a moment)")
        
        # The geometry that the points will be joined against
        join_geom = f"Transform(g.geom, {srid})" if needs_transform else "g.geom"

        aggregation_query = f"""
            SELECT
                g.grid_id,
                {agg_func.upper()}(p."{agg_col}") AS agg_value,
                COUNT(p.rowid) AS point_count
            FROM
                grid_temp AS g
            JOIN
                processed_data AS p ON ST_Contains({join_geom}, p.geom)
            GROUP BY
                g.grid_id;
        """
        
        agg_results_df = pd.read_sql(aggregation_query, self.engine)
        logger.info(f"Aggregation complete. Found data in {len(agg_results_df)} grid cells.")

        if agg_results_df.empty:
            logger.warning("No data points fell within the grid. Cannot create map."); return None
            
        # 5. Merge the results back to the GeoDataFrame and create the map
        grid_with_data = grid_gdf.merge(agg_results_df, on='grid_id')
        grid_with_data.rename(columns={'agg_value': agg_col}, inplace=True)
        
        map_center = [grid_with_data.geometry.centroid.y.mean(), grid_with_data.geometry.centroid.x.mean()]
        m = folium.Map(location=map_center, zoom_start=6, tiles='CartoDB positron')
        
        non_zero_data = grid_with_data[grid_with_data[agg_col] > 0]
        min_val = non_zero_data[agg_col].min() if not non_zero_data.empty else 0
        max_val = non_zero_data[agg_col].max() if not non_zero_data.empty else 0
        if min_val == max_val: min_val = max_val * 0.9 if max_val > 0 else 0
        colormap = cm.linear.YlOrRd_09.scale(min_val, max_val)
        colormap.caption = f'{agg_func.capitalize()} of {agg_col} per Grid Cell'

        def style_function(feature):
            value = feature['properties'][agg_col]
            if value > 0:
                return {'fillColor': colormap(value), 'color': 'black', 'weight': 0.5, 'fillOpacity': 0.7}
            else:
                return {'fillColor': '#D3D3D3', 'color': 'black', 'weight': 0.5, 'fillOpacity': 0.5}

        folium.GeoJson(grid_with_data, style_function=style_function, name='Aggregated Data',
            tooltip=folium.GeoJsonTooltip(fields=['grid_id', agg_col, 'point_count'], aliases=['Grid ID:', f'{agg_col}:', 'Point Count:'], localize=True)
        ).add_to(m)
        colormap.add_to(m)
        folium.LayerControl().add_to(m)
        return m

    def generate_grid_map(self, grid_cell_size_arg=None):
        if not self.engine:
            logger.warning("Cannot generate map: Database engine not available."); return None
        logger.info("\n--- Generating map using high-performance arithmetic grid aggregation ---")
        grid_cell_size_km = grid_cell_size_arg if grid_cell_size_arg is not None else config.GRID_CELL_SIZE
        agg_col = config.AGGREGATION_COLUMN
        agg_func = config.AGGREGATION_FUNCTION.lower()
        
        # 1. Get the data bounds from the pre-calculated coordinate columns.
        logger.info("Calculating grid bounds from coordinate columns...")
        with self.engine.connect() as conn:
            query = text("SELECT MIN(longitude), MIN(latitude), MAX(longitude), MAX(latitude) FROM processed_data;")
            try:
                xmin, ymin, xmax, ymax = conn.execute(query).fetchone()
            except sqlalchemy.exc.OperationalError as e:
                logger.error(f"Could not calculate data bounds. Are longitude/latitude columns missing? Error: {e}"); return None

        if not all((xmin, ymin, xmax, ymax)):
            logger.warning("Could not determine data bounds from database. Cannot generate map."); return None

        # 2. Construct and execute a single query that calculates grid indices and aggregates.
        grid_cell_size_deg = grid_cell_size_km / 111.32
        logger.info("Performing high-speed arithmetic aggregation in the database...")

        # This query avoids all spatial operations and uses pure arithmetic for binning.
        aggregation_query = f"""
            SELECT
                CAST(FLOOR((longitude - {xmin}) / {grid_cell_size_deg}) AS INTEGER) as grid_x_index,
                CAST(FLOOR((latitude - {ymin}) / {grid_cell_size_deg}) AS INTEGER) as grid_y_index,
                {agg_func.upper()}("{agg_col}") AS {agg_col},
                COUNT(*) as point_count
            FROM
                processed_data
            WHERE
                longitude IS NOT NULL AND latitude IS NOT NULL
            GROUP BY
                grid_x_index, grid_y_index;
        """
        logger.info("Aggregation query:")
        logger.info(aggregation_query)
        
        agg_results_df = pd.read_sql(aggregation_query, self.engine)
        logger.info(f"Aggregation complete. Found data in {len(agg_results_df)} grid cells.")

        if agg_results_df.empty:
            logger.warning("No data points fell within the grid. Cannot create map."); return None
            
        # 3. In Python, reconstruct the grid cell polygons from the indices.
        logger.info("Reconstructing grid geometries for mapping...")
        geometries = []
        for index, row in agg_results_df.iterrows():
            ix, iy = row['grid_x_index'], row['grid_y_index']
            cell_xmin = xmin + (ix * grid_cell_size_deg)
            cell_ymin = ymin + (iy * grid_cell_size_deg)
            cell_xmax = cell_xmin + grid_cell_size_deg
            cell_ymax = cell_ymin + grid_cell_size_deg
            geometries.append(Polygon([(cell_xmin, cell_ymin), (cell_xmax, cell_ymin), (cell_xmax, cell_ymax), (cell_xmin, cell_ymax)]))

        # 4. Create the final GeoDataFrame
        grid_with_data = gpd.GeoDataFrame(
            agg_results_df,
            geometry=geometries,
            crs='EPSG:4326'
        )
        
        # 5. Create the map using the robust manual styling method
        map_center = [grid_with_data.geometry.centroid.y.mean(), grid_with_data.geometry.centroid.x.mean()]
        m = folium.Map(location=map_center, zoom_start=6, tiles='CartoDB positron')
        
        non_zero_data = grid_with_data[grid_with_data[agg_col] > 0]
        min_val = non_zero_data[agg_col].min() if not non_zero_data.empty else 0
        max_val = non_zero_data[agg_col].max() if not non_zero_data.empty else 0
        if min_val == max_val: min_val = max_val * 0.9 if max_val > 0 else 0
        colormap = cm.linear.YlOrRd_09.scale(min_val, max_val)
        colormap.caption = f'{agg_func.capitalize()} of {agg_col} per Grid Cell'

        def style_function(feature):
            value = feature['properties'][agg_col]
            if value > 0:
                return {'fillColor': colormap(value), 'color': 'black', 'weight': 0.5, 'fillOpacity': 0.7}
            else:
                return {'fillColor': '#D3D3D3', 'color': 'black', 'weight': 0.5, 'fillOpacity': 0.5}

        folium.GeoJson(grid_with_data, style_function=style_function, name='Aggregated Data',
            tooltip=folium.GeoJsonTooltip(fields=[agg_col, 'point_count'], aliases=[f'{agg_col}:', 'Point Count:'], localize=True)
        ).add_to(m)
        colormap.add_to(m)
        folium.LayerControl().add_to(m)
        return m

def run_full_pipeline(company_filter_arg=None, date_filter_arg=None, grid_size_arg=None, output_filename_arg=None):
    logger.info("--- Starting ANEEL BDGD Full Pipeline ---")
    pipeline = ANEEL_Pipeline()

    # --- FIX: Use the argument only if it's a non-empty string, otherwise use the config ---
    company_filter = company_filter_arg if company_filter_arg else config.COMPANY_FILTER
    date_filter = date_filter_arg if date_filter_arg else config.DATE_FILTER
    grid_size = grid_size_arg if grid_size_arg else config.GRID_CELL_SIZE
    output_filename = output_filename_arg if output_filename_arg else config.OUTPUT_FILENAME
    
    features_to_download = pipeline.search_and_filter(company_filter, date_filter)
    if not features_to_download: 
        logger.warning("No datasets found matching filters. Exiting.")
        return

    if config.MAX_DOWNLOADS: 
        features_to_download = features_to_download[:config.MAX_DOWNLOADS]
    
    gdb_paths = pipeline.download_and_extract_from_features(features_to_download)
    if gdb_paths:
        pipeline.load_and_union_data(gdb_paths)
        pipeline.process_analytics()
        interactive_map = pipeline.generate_grid_map(grid_size) # Use the corrected variable
        if interactive_map:
            # Use the corrected variable
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
            interactive_map.save(output_filename)
            logger.info(f"\n--- SUCCESS ---")
            logger.info(f"Interactive map saved to: {os.path.abspath(output_filename)}")
    else:
        logger.warning("No geodatabases were found or processed. Exiting.")

def perform_search(company_filter, date_filter):
    logger.info(f"--- Search Mode: Finding datasets ---")
    logger.info(f"Company filter: '{company_filter}', Date filter: '{date_filter}'")
    pipeline = ANEEL_Pipeline()
    results = pipeline.search_and_filter(company_filter, date_filter)
    if not results:
        logger.info("No datasets found matching your criteria.")
    else:
        logger.info(f"Found {len(results)} matching datasets:")
        print("-" * 50)
        for feature in results:
            props = feature['properties']
            print(f"Title: {props.get('title')}")
            print(f"  - Name for Date Filter: {props.get('name')}")
            print(f"  - Tags: {props.get('tags')}")
            print(f"  - Size: {props.get('size', 0) / 1024 / 1024:.2f} MB")
        print("-" * 50)
        logger.info("Use the 'Title' or parts of it for the COMPANY_FILTER in config.py")
        logger.info("Use parts of the 'Name' (like '2023-12-31') for the DATE_FILTER in config.py")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ANEEL BDGD Downloader and Mapper")
    parser.add_argument("--company_filter", type=str, help="Filter by company name (e.g., 'Light').")
    parser.add_argument("--date_filter", type=str, help="Filter by date (e.g., '2024-12-31').")
    parser.add_argument("--grid_size", type=float, help="Grid cell size in kilometers (e.g., 1.0 for 1km).")
    parser.add_argument("--output_filename", type=str, help="Output HTML filename (e.g., 'output/map.html').")
    parser.add_argument("--search", action="store_true", help="Run in search mode to find datasets.")
    args = parser.parse_args()
    if args.search:
        company = args.company_filter if args.company_filter else ""
        date = args.date_filter if args.date_filter else ""
        perform_search(company, date)
    else:
        run_full_pipeline(args.company_filter, args.date_filter, args.grid_size, args.output_filename)