import pandas as pd
from datetime import datetime
import os
import warnings

import geopandas as gpd
from shapely.geometry import Point
import random

# Get water quality data from satellite images for the particular features - get from RWQ server API

# Approx. function for getting data --* TODO
# def getDataFromRWQ(loc_ID, wq_feature):
#     #1. Check database if the data exists. Set the starting date to the date after last record
#     if loc_ID.exists:
#         first_day = get_last_day_in_DB + 1
#     else:
#         first_day = pd.to_datetime("2015-06-23")     # Date of Sentinel 2 launch
#
#     today = datetime.today().date()
#     ts = pd.date_range(start=first_day, end=today, freq='D')
#
#     for image_date in ts:
#         try:
#             get_image_from_RWQ_API(loc_ID, wq_feature, image_date) # get image from RWQ API
#             add_image_to_database   # add image to database
#         except:
#             pass
#     rastry by se měly ukládat do nějaké složky, např. raster/osm_id
#     všechny rastry by měly mít formát názvu: osm_id_yyy-mm-dd
#     return

def getVectorLyrs(in_file, col_name, poly_id, lake_buffer=-20, n_points_km=20, p_buff=10, overwrite=False):
    """
    Extract vector data from original vector layer. The output includes random points and their buffer, original polygon of the selected water reservoir and polygon with inner buffer. The data are converted to GPKG format. The names of output data are defined as follows: \n
    * poly_id_reservoir.gpkg - original polygon of the selected reservoir
    * poly_id_buffer.gpkg - polygon with inner buffer because the removing the edge effect
    * poly_id_points.gpkg - random point layer
    * poly_id_bpoints.gpkg - point layer with buffer
    \n
    poly_id in the names is replaced by id number, e.g. 11092220_buffer.gpkg

    :param in_file: Input vector polygon layer
    :param col_name: Name of the column with IDs (e.g. osm_id)
    :param poly_id: ID of the selected water reservoir (e.g. 11092220).
    :param lake_buffer: Buffer width in m. Default -20 m. The value should be negative.
    :param n_points_km: Number of random points per square km
    :param p_buff: Radius of the buffer around the point in m. Default 10 m.
    :param overwrite: Overwrite data if exists. Default is False.
    :return:
    """

    # TODO: přidat odkaz na soubory do SQL databáze

    # Get the polygon index
    poly_id = str(poly_id)

    # Make directory for new vector files

    cwd_dir = os.getcwd()
    lake_dir = os.path.join(cwd_dir, "vectors", poly_id)

    if os.path.exists(lake_dir):
        if overwrite is False:
            warnings.warn("The vector data already exists in folder: {vectdir}".format(vectdir=lake_dir), stacklevel=3)
            return
        else:
            warnings.warn("The vector data already exists in folder: {vectdir}. The data will be rewritten!".format(vectdir=lake_dir), stacklevel=3)
    else:
        os.mkdir(lake_dir)

    # Define vector layers names and paths
    f_orig_poly = os.path.join(lake_dir, poly_id + '_reservoir.gpkg')
    f_buff_poly = os.path.join(lake_dir, poly_id + '_buffer.gpkg')
    f_rand_points = os.path.join(lake_dir, poly_id + '_points.gpkg')
    f_buff_points = os.path.join(lake_dir, poly_id + '_bpoints.gpkg')

    # Reading vector file
    gdf = gpd.read_file(in_file)

    # Get a the selected polygon using OSM ID
    gdf_select = gdf.query("{col_name} == '{poly_id}'".format(col_name=col_name, poly_id=poly_id))  # TODO Nahradit importem z SQL

    # Get original CRS of the layer
    try:
        epsg_orig = gdf.crs()
    except:
        epsg_orig = 'epsg:4326'

    # Convert selected layer to UTM CRS
    epsg_new = gdf.estimate_utm_crs()
    gdf_select_utm = gdf_select.to_crs(epsg_new)

    # Remove buffer zone of the selected water reservoir
    gdf_select_utm_buff = gdf_select_utm.buffer(lake_buffer)

    # Cover the layer to the original CRS
    gdf_wgs_buff = gdf_select_utm_buff.to_crs(epsg_orig)

    # Get bounds and geometry from the layer
    gdf_wgs_buff_bounds = gdf_wgs_buff.bounds
    gdf_wgs_buff_geo = gdf_wgs_buff.geometry.iloc[0]

    # Calculate area of the reservoir
    area = gdf_select_utm.area.values[0] / 10000

    # Get number of points
    n_points = int(area * n_points_km / 100)

    # A list for random points
    random_points = []

    # Random points creation
    for _ in range(n_points):
        while True:
            # Getting random coordinates
            x = random.uniform(gdf_wgs_buff_bounds.iloc[0, 0], gdf_wgs_buff_bounds.iloc[0, 2])
            y = random.uniform(gdf_wgs_buff_bounds.iloc[0, 1], gdf_wgs_buff_bounds.iloc[0, 3])
            bod = Point(x, y)
            # Pokud je bod uvnitř polygonu, přidáme ho do seznamu
            if bod.within(gdf_wgs_buff_geo):
                random_points.append(bod)
                break

    # Set up a GeoDataFrame of random points
    gdf_points = gpd.GeoDataFrame(geometry=random_points)

    # Set CRS of the points layer
    gdf_points = gdf_points.set_crs(epsg_orig)
    gdf_points_utm = gdf_points.to_crs(epsg_new)

    # Make buffer around the points
    gdf_points_utm_buff = gdf_points_utm.buffer(p_buff)
    gdf_points_wgs_buff = gdf_points_utm_buff.to_crs(epsg_orig)

    # Uložení souborů do adresáře - Vectors/osm_id      # TODO Nebude se ukládat, jen se využijí vrstvy
    gdf_points.to_file(f_rand_points)
    gdf_points_wgs_buff.to_file(f_buff_points)
    gdf_select.to_file(f_orig_poly)
    gdf_wgs_buff.to_file(f_buff_poly)

    return


