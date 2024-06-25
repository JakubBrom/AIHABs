# Imports

import os
import time
import logging
import json as jsonmod
from glob import glob
import geopandas as gpd
import pandas as pd
import user_inputs as inp
import requests

from datetime import datetime, timedelta
from sqlalchemy import create_engine, exc, text
from shapely.geometry import Polygon, shape

from AIHABs_wrappers import measure_execution_time

def get_keycloak(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": inp.username,
        "password": inp.password,
        "grant_type": "password",
        }
    try:
        r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        data=data,
        )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
            )
    return r.json()["access_token"]

@measure_execution_time
def getSentinel2DataList(osm_id, start_date, user, db_name, db_table, vect_db_table, cloud_cover=10, wb_coverage=0.75, data_collection="SENTINEL-2", product_type='S2MSI1C', time_zone='GMT'):
    """
    The funcion retrieves Sentinel-2 data images based on specified criteria from a Copernicus Dataspace API. The
    function takes various parameters such as OSM object id, start date, user credentials, database details,
    cloud cover percentage, product type, and time zone. The function connects to a PostGIS database,
    queries a polygon from the database based on the OSM id, prepares a polygon for the Copernicus Dataspace request and
    defines time periods, iterates over time periods to fetch Sentinel-2 data images list, processes the data,
    filters images based on water body bounds coverage, and uploads the selected images to the database. Finally,
    it returns a DataFrame containing the selected Sentinel-2 data images list.

    Parameters:
    :param osm_id: OSM object id
    :param start_date: Start date for querying Sentinel 2 data
    :param user: Database user
    :param db_name: Database name
    :param db_table: Output database table name
    :param vect_db_table: Database table name with reservoirs polygons
    :param cloud_cover: Maximum cloud cover percentage
    :param wb_coverage: Minimum coverage of the overlapping area of the water body over the image footprint. Default
    is 0.75
    :param data_collection: Type of data collection (default is 'SENTINEL-2')
    :param product_type: Type of Sentinel-2 product (default is 'S2MSI1C')
    :param time_zone: Time zone for date calculations (default is 'GMT')

    Returns:
    :return DataFrame: List of Sentinel-2 data images meeting specified criteria
    """

    # Connect to PostGIS
    engine = create_engine('postgresql://{user}@/{db_name}'.format(user=user, db_name=db_name))

    # Get polygon of the reservoir from the DB
    sql_query = "SELECT * FROM {db_table} WHERE osm_id = '{osm_id}'".format(osm_id=str(osm_id), db_table=vect_db_table)
    gdf = gpd.read_postgis(sql_query, engine, geom_col='geometry')
    bounds = gdf.total_bounds

    # Define polygon for the Dataspace request
    polygon_bounds = Polygon([(bounds[0], bounds[1]), (bounds[0], bounds[3]), (bounds[2], bounds[3]), (bounds[2], bounds[1])])
    id_bounds = gpd.GeoDataFrame(geometry=[polygon_bounds], crs='epsg:4326')

    # Define polygon for the Dataspace request
    aoi = "POLYGON(({minx} {miny},{minx} {maxy},{maxx} {maxy},{maxx} {miny},{minx} {miny}))'".format(minx=str(bounds[0]), miny=str(bounds[1]), maxx=str(bounds[2]), maxy=str(bounds[3]))

    # Define time period
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.now().date()        # Up to today

    # Set time windows
    n_days = (end_date - start_date).days
    n_chunks = n_days // 20

    if n_chunks == 0:
        slots = [(start_date, end_date)]
    else:
        st_date = start_date - timedelta(days=1)
        tdelta = (end_date - st_date) / n_chunks

        edges = [(st_date + i * tdelta) for i in range(n_chunks)]

        slots = [((edges[i] + timedelta(days=1)).isoformat(), edges[i + 1].isoformat()) for i in range(len(edges) - 1)]
        slots.append(((edges[-1] + timedelta(days=1)).isoformat(), end_date.isoformat()))

    # Iterate the requests on the Dataspace API over the time periods
    data_list = pd.DataFrame()  # Initial dataframe
    for i in slots:

        # Get time period
        first_date = i[0]
        last_date = i[1]

        # Get list of the Sentinel 2 data for the osm_id and time period
        json = requests.get(f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}' "                            
                            f"and OData.CSC.Intersects(area=geography'SRID=4326;{aoi}) "
                            f"and ContentDate/Start gt {first_date}T00:00:00.000Z "
                            f"and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt {cloud_cover}) "
                            f"and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{product_type}') "
                            f"and ContentDate/Start lt {last_date}T00:00:00.000Z").json()
        # Add list of the data to the dataframe
        data_list_i = pd.DataFrame.from_dict(json['value'])

        try:
            data_list = pd.concat([data_list, data_list_i])
        except:
            data_list = data_list_i

    # Add OSM id column to the dataset, rename the column Id to img_id
    data_list['osm_id'] = osm_id
    data_list.rename(columns={'Id': 'img_id'}, inplace=True)

    # Create GeoDataFrame from the data_list
    geometry = data_list['GeoFootprint'].apply(lambda x: shape(x))
    gdf = gpd.GeoDataFrame(data_list, geometry=geometry, crs='epsg:4326')

    # Add date column
    gdf['isodate'] = (pd.to_datetime(gdf['ContentDate'].apply(lambda x: x['Start'])))
    gdf['date'] = gdf['isodate'].dt.tz_localize(None).dt.date

    # Select only the columns that we need
    data_select = gdf[['img_id', 'osm_id', 'Name', 'date', 'geometry', 'Online']]

    # Clip the images geometries to the id_bounds
    osm_id_clip = gpd.clip(data_select, id_bounds)

    # Calculate area of the clipped geometries and area of osm_id polygon
    osm_id_clip['area'] = osm_id_clip.geometry.area
    id_bounds['area'] = id_bounds.geometry.area
    id_area = id_bounds['area'].iloc[0]

    # Calculate percentage of the clipped area to the osm_id area
    osm_id_clip['overlap_perc'] = osm_id_clip['area'] / float(id_area)

    # Merge the dataframes
    data_select = pd.merge(data_select, osm_id_clip[['img_id', 'overlap_perc']], on='img_id')

    # Select the images with coverage of water reservoir bounds higher than wb_coverage
    img_list = data_select[data_select['overlap_perc'] > wb_coverage]

    # Upload the data to the database
    img_list.to_postgis(name=db_table, con=engine, if_exists='replace', index=False)
    engine.dispose()

    return img_list

def compareImageLists(user, db_name, db_table1, db_table2):
    """
    Compare image list (db_table1) with list of existing raster data (db_table2) in the PostgreSQL database and
    return the list of missing images. The list of missing images is returned as a GeoDataFrame containing the image
    footprint and other images features.

    Parameters:
    :param user: The username for the PostgreSQL database connection.
    :param db_name: The name of the PostgreSQL database.
    :param db_table1: The name of the table containing the image list gathered from the Dataspace API.
    :param db_table2: The name of the table containing the raster data.

    Returns:
    :return: A GeoDataFrame containing the missing images with their geometry, CRS and other features.
    """

    engine = create_engine('postgresql://{user}@/{db_name}'.format(user=user, db_name=db_name))

    # # Porovnání tabulek se seznamem scén podle Id snímku
    sql_query = ("SELECT img_id FROM {db_table1} "
                 "EXCEPT "
                 "SELECT img_id FROM {db_table2}").format(db_table1=db_table1, db_table2=db_table2)

    sql_query2 = ("SELECT * FROM {db_table1}".format(db_table1=db_table1))

    df = pd.read_sql(sql_query, con=engine)
    gdf = gpd.read_postgis(sql_query2, engine, geom_col='geometry')

    df_missing = pd.merge(df, gdf, on='img_id', how='left')
    gdf_missing = gpd.GeoDataFrame(df_missing, geometry='geometry', crs='epsg:4326')

    gdf_missing.to_postgis(name='missing_images', con=engine, if_exists='replace', index=False)
    engine.dispose()

    return gdf_missing

def getSentinel2Data():
    """
    Get Sentinel-2 data from the Dataspace API and upload it to the database.

    Parameters:

    Returns:
    :return:
    """
    pass
#
#
#
#
# access_token = get_keycloak("username", "password")
# session = requests.Session()
# session.headers.update({'Authorization': f'Bearer {access_token}'})
# lookedup_tiles = json['value']
# for var in lookedup_tiles:
#     try:
#         if "MSIL1C" in var['Name']:
#             # logging.info("Row found: " + str(count))
#             myfilename = var['Name']
#             logging.info("File OK: " + myfilename)
#             # mytile = myfilename.split("_")[-2]
#             # foundtiles_dict[mytile] = [point['LONG'], point['LAT']]
#             url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products(" + var['Id'] + ")/$value"
#             response = session.get(url, allow_redirects=False)
#             while response.status_code in (301, 302, 303, 307):
#                 logging.info("response: " + str(response.status_code))
#                 url = response.headers['Location']
#                 logging.info("next line ...")
#                 response = session.get(url, allow_redirects=False)
#                 logging.info("Last line ...")
#             file = session.get(url, verify=False, allow_redirects=True)
#             with open(f""+var['Name']+".zip", 'wb') as p:
#                 p.write(file.content)
#     except:
#         pass
#         # count = count + 1
# breakpoint()
# ###############################################################################
# # Verifying downloaded files
# ###############################################################
#
# keycloak_token = get_keycloak("username", "password")
# with open('list_downloaded_files.txt', 'w') as file:
#    file.write(jsonmod.dumps(foundtiles_dict))
#
# filelist=glob("S2*.SAFE.zip")
# corrupted = []
# for var in filelist:
#    if int(os.path.getsize(var)/1024) < 10:
#       corrupted.append(var)
#
# count = 2
# for point in corrupted:
#         logging.info("Currpted Row: " + str(count))
#         tile_retry = point.split("_")[-2]
#         if (count+3)%4 == 0:
#             keycloak_token = get_keycloak("username", "password")
#         mylong = foundtiles_dict[tile_retry][0]
#         mylat = foundtiles_dict[tile_retry][1]
#         aoi = "POINT(" + mylong + " " + mylat + ")'"
#         print("Retrying to get the point:", mylong, mylat)
#         json = requests.get(f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}' \
#                             and OData.CSC.Intersects(area=geography'SRID=4326;{aoi}) and ContentDate/Start gt {start_date} \
#                                 and ContentDate/Start lt {end_date}").json()
#         session = requests.Session()
#         session.headers.update({'Authorization': f'Bearer {keycloak_token}'})
#         lookedup_tiles = json['value']
#         for var in lookedup_tiles:
#             try:
#                 if "MSIL2A" in var['Name']:
#                     logging.info("Currpted Row found: " + str(count))
#                     myfilename = var['Name']
#                     logging.info("Currpted File OK: " + myfilename)
#                     url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products(" + var['Id'] + ")/$value"
#                     response = session.get(url, allow_redirects=False)
#                     while response.status_code in (301, 302, 303, 307):
#                         logging.info("Currpted response: " + str(response.status_code))
#                         url = response.headers['Location']
#                         logging.info("Currpted next line ...")
#                         response = session.get(url, allow_redirects=False)
#                         logging.info("Currpted Last line ...")
#                     file = session.get(url, verify=False, allow_redirects=True)
#                     with open(f""+var['Name']+".zip", 'wb') as p:
#                         p.write(file.content)
#             except:
#                 pass
#         count = count + 1

if __name__ == '__main__':

    t1 = time.time()

    # Připojení k databázi PostGIS
    start_date = '2024-01-01'


    # TODO: redefinovat logging
    # logging.basicConfig(filename='log_sentinel_download' + str(start_date) + '.log',
    #                     filemode='a',
    #                     format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    #                     level=logging.INFO)

    osm_id = 11092220
    user = 'jakub'
    db_name = 'AIHABs'
    db_table = 'img_list'
    db_table2 = 'jako_vysledky'

    vect_db_table = 'water_reservoirs'


    # data_select = getSentinel2DataList(osm_id, start_date, user, db_name, db_table, vect_db_table, cloud_cover=30)
    #
    # print(data_select)

    chybejici_snimky = compareImageLists(user=user, db_name=db_name, db_table1=db_table, db_table2=db_table2)

    print(chybejici_snimky)

    # engine = create_engine('postgresql://{user}@/{db_name}'.format(user=user, db_name=db_name))
    #
    # sql_read = ("SELECT * FROM {db_table1}".format(db_table1=db_table1))
    # df_dat = pd.read_sql(sql_read, con=engine)
    # # df_dat.to_sql(db_table1, engine, if_exists='append', index=False)
    #
    # start_date = df_dat['ContentDate'].apply(jsonmod.load)
    #
    #
    # # # Porovnání tabulek se seznamem scén podle Id snímku
    # # sql_query = ("SELECT img_id FROM {db_table2} "
    # #              "EXCEPT "
    # #              "SELECT img_id FROM {db_table1}").format(db_table1=db_table1, db_table2=db_table2)
    # #
    # # df = pd.read_sql(sql_query, con=engine)
    # # print(df)
    #
    # engine.dispose()

    t2 = time.time()
    print(t2-t1)

    # TODO: do tabulky s rastry ukládat Id snímku --> potřeba přejmenovat Id sloupec

