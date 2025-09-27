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
import shapely.wkb as wkb
from shapely.geometry import Polygon
import folium
from tqdm import tqdm
import time
import argparse

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
    """
    A unified pipeline to download, process, and map ANEEL BDGD data.
    """
    def __init__(self):
        self.session = requests.Session()
        self.api_base = "https://dadosabertos-aneel.opendata.arcgis.com/api/search/v1/collections/dataset/items"
        os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)
        os.makedirs(config.EXTRACT_DIR, exist_ok=True)
        self.processed_gdf = gpd.GeoDataFrame()

    def search_and_filter(self, company_filter, date_filter):
        logger.info("Searching for all File Geodatabase datasets...")
        all_features = []
        startindex = 1
        while True:
            params = {'type': "File Geodatabase", 'limit': 100, 'startindex': startindex}
            response = self.session.get(self.api_base, params=params); response.raise_for_status()
            data = response.json(); features = data.get('features', [])
            if not features: break
            all_features.extend(features)
            if len(features) < 100: break
            startindex += 100; time.sleep(0.5)
        logger.info(f"Found {len(all_features)} total datasets. Applying filters...")
        filtered_features = []
        for feature in all_features:
            props = feature['properties']; title = props.get('title', '').upper(); name = props.get('name', '').upper()
            if company_filter and company_filter.upper() not in title and company_filter.upper() not in name: continue
            if date_filter and date_filter not in name: continue
            filtered_features.append(feature)
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
        logger.info("\n--- Loading and Unioning Data with SQLite Optimization ---")
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        base_crs = None
        all_joined_gdfs = []

        for gdb_path in gdb_paths:
            logger.info(f"\n--- Processing: {os.path.basename(gdb_path)} ---")
            try:
                with fiona.open(gdb_path, 'r', layer=config.SPATIAL_LAYER) as source:
                    spatial_gdf = gpd.GeoDataFrame.from_features([f for f in source], crs=source.crs)
                
                if base_crs is None: base_crs = spatial_gdf.crs; logger.info(f"  - Base CRS established as: {base_crs.name}")
                if spatial_gdf.crs != base_crs: spatial_gdf = spatial_gdf.to_crs(base_crs)
                
                spatial_gdf = spatial_gdf[spatial_gdf['geometry'].apply(is_valid_geometry)]
                if spatial_gdf.empty: logger.warning(f"  - No valid geometries in {config.SPATIAL_LAYER}. Skipping."); continue
                
                consumer_dfs = [gpd.read_file(gdb_path, layer=t) for t in config.CONSUMER_LAYERS if t in fiona.listlayers(gdb_path)]
                if not consumer_dfs: logger.warning(f"  - No consumer layers found. Skipping."); continue
                consumer_df = pd.concat(consumer_dfs, ignore_index=True)
                
                spatial_gdf[config.SPATIAL_KEY] = spatial_gdf[config.SPATIAL_KEY].astype(str)
                consumer_df[config.CONSUMER_KEY] = consumer_df[config.CONSUMER_KEY].astype(str)

                spatial_gdf['geometry_wkb'] = spatial_gdf['geometry'].apply(wkb.dumps)
                spatial_df_no_geom = spatial_gdf.drop(columns='geometry')

                spatial_df_no_geom.to_sql("spatial_temp", engine, if_exists='replace', index=False)
                consumer_df.to_sql("consumer_temp", engine, if_exists='replace', index=False)

                join_query = f"""
                    SELECT s.*, c.* FROM "spatial_temp" AS s
                    INNER JOIN "consumer_temp" AS c ON s."{config.SPATIAL_KEY}" = c."{config.CONSUMER_KEY}"
                """
                joined_df = pd.read_sql(join_query, engine)
                
                # Check for duplicate columns after join and remove them
                joined_df = joined_df.loc[:,~joined_df.columns.duplicated()]

                joined_gdf = gpd.GeoDataFrame(joined_df, geometry=joined_df['geometry_wkb'].apply(wkb.loads), crs=base_crs)
                joined_gdf = joined_gdf.drop(columns='geometry_wkb')
                
                all_joined_gdfs.append(joined_gdf)
                logger.info(f"  - Successfully processed and joined {len(joined_gdf)} records.")
            except Exception as e:
                logger.error(f"  - FAILED to process GDB. Error: {e}")
        
        if all_joined_gdfs:
            self.processed_gdf = pd.concat(all_joined_gdfs, ignore_index=True)
            self.processed_gdf = gpd.GeoDataFrame(self.processed_gdf, crs=base_crs, geometry='geometry')
            logger.info(f"\nUnion complete. Total records: {len(self.processed_gdf)}. Final CRS: {self.processed_gdf.crs.name}")
        else:
            logger.warning("\nNo data was successfully processed.")
        return self

    def process_analytics(self):
        if self.processed_gdf.empty: logger.warning("Skipping analytics: No data available."); return self
        logger.info("\nExecuting analytics on the unioned dataset...")
        df = self.processed_gdf; ene_cols = [f'ENE_{str(i).zfill(2)}' for i in range(1, 13)]
        for col in ene_cols:
            if col not in df.columns: df[col] = 0
        df[ene_cols] = df[ene_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
        df['ENE_TOT'] = df[ene_cols].sum(axis=1); df['ENE_MED'] = df['ENE_TOT'] / 12; df['DEM'] = pd.to_numeric(df.get('CAR_INST', 0), errors='coerce').fillna(0)
        self.processed_gdf = df; logger.info("Analytics complete.")
        return self

    def generate_grid_map(self, grid_cell_size_arg=None):
        """Generates an advanced interactive grid map using settings from config.py."""
        if self.processed_gdf.empty: logger.warning("Cannot generate map: No data available."); return None
        
        grid_cell_size = grid_cell_size_arg if grid_cell_size_arg is not None else config.GRID_CELL_SIZE
        agg_col = config.AGGREGATION_COLUMN; agg_func = config.AGGREGATION_FUNCTION.lower()
        
        logger.info(f"\nGenerating interactive grid map: '{agg_func}' of '{agg_col}' per cell...")
        points_data = self.processed_gdf.copy()
        
        if config.GRID_CELL_UNITS.lower() == 'meters':
            points_data = points_data.to_crs(config.TARGET_CRS_EPSG)

        xmin, ymin, xmax, ymax = points_data.total_bounds
        grid_cells = [Polygon([(x0, y0), (x0 + grid_cell_size, y0), (x0 + grid_cell_size, y0 + grid_cell_size), (x0, y0 + grid_cell_size)]) for x0 in np.arange(xmin, xmax, grid_cell_size) for y0 in np.arange(ymin, ymax, grid_cell_size)]
        grid = gpd.GeoDataFrame(grid_cells, columns=['geometry'], crs=points_data.crs)
        merged = gpd.sjoin(points_data, grid, how='left', predicate='within')

        if agg_func == 'count':
            agg = merged.groupby('index_right').agg(point_count=('geometry', 'count'))
            map_col = 'point_count'
        else:
            agg = merged.groupby('index_right').agg(agg_result=(agg_col, agg_func), point_count=('geometry', 'count')).rename(columns={'agg_result': agg_col})
            map_col = agg_col

        grid_with_data = grid.merge(agg, left_index=True, right_index=True, how='left').fillna(0)
        grid_with_data = grid_with_data[grid_with_data['point_count'] > 0]
        
        grid_for_map = grid_with_data.to_crs(epsg=4326)
        
        try:
            map_center_projected = grid_for_map.to_crs(config.TARGET_CRS_EPSG).centroid.union_all().centroid
            map_center_gdf = gpd.GeoDataFrame(geometry=[map_center_projected], crs=config.TARGET_CRS_EPSG).to_crs(epsg=4326)
            map_center = [map_center_gdf.geometry.y.iloc[0], map_center_gdf.geometry.x.iloc[0]]
        except:
            map_center = [grid_for_map.geometry.centroid.y.mean(), grid_for_map.geometry.centroid.x.mean()]
            
        m = folium.Map(location=map_center, zoom_start=6, tiles=None)

        folium.TileLayer('OpenStreetMap', name='Street Map').add_to(m); folium.TileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', attr='Esri', name='Esri Satellite').add_to(m); folium.TileLayer('CartoDB positron', name='Light Map').add_to(m)
        
        choropleth_data = pd.DataFrame(grid_for_map.drop(columns='geometry')).reset_index()

        # --- THIS IS THE CORRECTED CHOROPLETH CALL ---
        folium.Choropleth(
            geo_data=grid_for_map.to_json(), # Pass the GeoDataFrame directly
            name=f'Aggregated {map_col}',
            data=choropleth_data,
            columns=['index', map_col], # Use the new 'index' column as the key
            key_on='feature.id',        # Folium links this to the GeoDataFrame's index
            fill_color='YlOrRd',
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name=f'{agg_func.capitalize()} of {map_col} per Grid Cell',
        ).add_to(m)

        tooltip = folium.GeoJsonTooltip(fields=[map_col, 'point_count'], aliases=[f'{map_col}:', 'Point Count:'])
        folium.GeoJson(grid_for_map, style_function=lambda x: {'fillOpacity': 0, 'weight': 0}, tooltip=tooltip).add_to(m)

        folium.LayerControl().add_to(m)
        return m

def run_full_pipeline(company_filter_arg=None, date_filter_arg=None, grid_size_arg=None, output_filename_arg=None):
    logger.info("--- Starting ANEEL BDGD Full Pipeline ---")
    pipeline = ANEEL_Pipeline()

    company_filter = company_filter_arg if company_filter_arg is not None else config.COMPANY_FILTER
    date_filter = date_filter_arg if date_filter_arg is not None else config.DATE_FILTER
    
    features_to_download = pipeline.search_and_filter(company_filter, date_filter)
    if config.MAX_DOWNLOADS: features_to_download = features_to_download[:config.MAX_DOWNLOADS]
    gdb_paths = pipeline.download_and_extract_from_features(features_to_download)
    if gdb_paths:
        pipeline.load_and_union_data(gdb_paths)
        pipeline.process_analytics()
        interactive_map = pipeline.generate_grid_map(grid_size_arg)
        if interactive_map:
            output_filename = output_filename_arg if output_filename_arg is not None else config.OUTPUT_FILENAME
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
    parser.add_argument("--grid_size", type=float, help="Grid cell size in meters or degrees (e.g., 1000 for 1km).")
    parser.add_argument("--output_filename", type=str, help="Output HTML filename (e.g., 'output/map.html').")
    parser.add_argument("--search", action="store_true", help="Run in search mode to find datasets.")

    args = parser.parse_args()

    if args.search:
        company = args.company_filter if args.company_filter else ""
        date = args.date_filter if args.date_filter else ""
        perform_search(company, date)
    else:
        run_full_pipeline(args.company_filter, args.date_filter, args.grid_size, args.output_filename)