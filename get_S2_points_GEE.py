import ee

import geopandas as gpd
import pandas as pd

from shapely.geometry import Point
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from AIHABs_wrappers import measure_execution_time
from get_random_points import get_sampling_points
from get_meteo import getLastDateInDB


@measure_execution_time
def process_sentinel2_points_data(point_layer, start_date, end_date, db_name, user, db_table):
    """
    Function to fetch Sentinel-2 data for random points within the water reservoir polygon and time
    period using Google Earth Engine. The result is GeoDataFrame with Sentinel-2 data for each point and in the PostGIS database.

    :param point_layer: The randomly selected points within the reservoir polygon
    :param start_date: Start date in the format 'YYYY-MM-dd'
    :param end_date: End date in the format 'YYYY-MM-dd'
    :param db_name: Database name
    :param user: Database user
    :param db_table: Table with the results
    :return: GeoDataFrame
    """

    engine = create_engine('postgresql://{user}@/{db_name}'.format(user=user, db_name=db_name))

    point_collection = ee.FeatureCollection(point_layer.__geo_interface__)

    def extract_values(image):
        pixel_values = image.sampleRegions(collection=point_collection, scale=10)
        pixel_values = pixel_values.map(lambda feature: feature.set({
            'image_id': image.id(),
            'date': image.date().format('YYYY-MM-dd'),
            'cloud_cover': image.get('CLOUDY_PIXEL_PERCENTAGE'),
            'cloud_shadow': image.get('CLOUD_SHADOW_PERCENTAGE')
        }))

        return pixel_values

    collection = [
        'COPERNICUS/S2_SR_HARMONIZED',
        'COPERNICUS/S2_CLOUD_PROBABILITY']
    s2_outputs_list = []
    for col in collection:
        s2_output = ee.ImageCollection(col).filterBounds(point_collection).filterDate(start_date, end_date)
        sampled_points = s2_output.map(extract_values)

        results = sampled_points.flatten().getInfo()

        features = []
        for idx, value in enumerate(results['features']):
            properties = value['properties']
            features.append(properties)

        s2_outputs_df = pd.DataFrame(features)

        s2_outputs_list.append(s2_outputs_df)

    df_all = pd.concat([s2_outputs_list[0], s2_outputs_list[1]['probability']], axis=1)

    if df_all.get('date') is not None:
        df_all['date'] = pd.to_datetime(df_all['date']).dt.date
        df_all = df_all.dropna(axis=0, how='any')
        # Převedení na GeoDataFrame
        geometries = [Point(xy) for xy in zip(df_all['lon'], df_all['lat'])]

        gdf_out = gpd.GeoDataFrame(df_all, geometry=geometries, crs='epsg:4326')

        gdf_out.to_postgis(db_table, con=engine, if_exists='append', index=False)
        engine.dispose()

    else:
        df_all = pd.DataFrame()
        gdf_out = df_all

    return gdf_out


@measure_execution_time
def get_sentinel2_data(ee_project, osm_id, db_name, user, db_table_reservoirs, db_table_points, db_table_S2_points_data,
                       start_date=None, end_date=None, n_points_max=5000, n_processes=10):
    """
    This function is a wrapper for the process_sentinel2_points_data function. Function for managing of the Sentinel-2
    data fetching for random points within the water reservoir polygon and time period using Google Earth Engine.
    The data are stored in the PostGIS Database.

    :param ee_project: Google Earth Engine project
    :param osm_id: OSM object id
    :param db_name: Database name
    :param user: Database user
    :param db_table_reservoirs: PostGIS database table with water reservoirs
    :param db_table_points: PostGIS database table with random points within reservoir polygon
    :param db_table_S2_points_data: PostGIS database table with Sentinel-2 data for particular points
    :param start_date: Start date. Default is None - last date in the GEE database or in the PostGIS table
    :param end_date: End date. Default is None - last date in the GEE database or current date
    :param n_points_max: Maximum number of points. Default 5000
    :param n_processes: Number of parallel fetching of time windows. Default 10
    :return:
    """

    ee.Authenticate()
    ee.Initialize(project=ee_project)

    # Connect to PostGIS
    engine = create_engine('postgresql://{user}@/{db_name}'.format(user=user, db_name=db_name))

    # get points
    point_layer = get_sampling_points(osm_id, db_name, user, db_table_reservoirs, db_table_points)
    point_layer['lat'] = point_layer.geometry.y
    point_layer['lon'] = point_layer.geometry.x

    # Vytvořte oblast zájmu (AOI) jako obdélníkový polygon
    minx, miny, maxx, maxy = point_layer.total_bounds
    aoi = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

    # Create chunks for time series
    # Get possible length of steps (chunks)
    n_points: int = len(point_layer)

    step_length = int(n_points_max) // n_points

    # Check if table exists and create new one if not
    query = text(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{tab_name}')".format(
            tab_name=db_table_S2_points_data))

    with engine.connect() as connection:
        result = connection.execute(query)
        exists = result.scalar()

    # Set start date
    if exists:
        # Get last date from database
        st_date = getLastDateInDB(osm_id, db_name, user, db_table_S2_points_data)
    else:
        st_date = None

    if st_date is not None:
        st_date = st_date + timedelta(days=1)
    else:
        if start_date is None:
            # Get collection
            collection = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')

            # Filter by the AOI
            filtered_collection = collection.filterBounds(aoi)

            # Get the oldest image
            earliest_image = filtered_collection.sort('system:time_start').first()

            # get the date for the oldest image
            earliest_date = ee.Date(earliest_image.get('system:time_start')).format('YYYY-MM-dd').getInfo()
            start_date = earliest_date

        st_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    # Set end date
    if end_date is None:
        end_date = datetime.now().date()  # Up to today
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    print('Data for period from {st_date} to {end_date} will be downloaded'.format(st_date=st_date, end_date=end_date))

    # Set time windows
    n_days = (end_date - st_date).days
    n_chunks = n_days // step_length + 1

    # Create time windows
    t_delta = int((end_date - st_date).days / n_chunks)

    if t_delta < 2:
        t_delta = 2

    freq = '{tdelta}D'.format(tdelta=t_delta)
    date_range = pd.date_range(start=st_date, end=end_date, freq=freq)
    slots = [(date_range[i].date().isoformat(), (date_range[i + 1] - timedelta(days=1)).date().isoformat()) for i in
             range(
        len(date_range) - 1)]
    slots.append((date_range[-1].date().isoformat(), end_date.isoformat()))

    print(slots)

    # Get Sentinel-2 data for each time window using Multiprocessing
    if n_chunks > n_processes:
        n_processes = n_processes
    else:
        n_processes = n_chunks

    def worker(args):
        return process_sentinel2_points_data(*args)

    with ThreadPoolExecutor(max_workers=n_processes) as executor:
        executor.map(worker, [(point_layer, start.format('YYYY-MM-dd'), end.format('YYYY-MM-dd'), db_name, user, db_table_S2_points_data)
                                             for start, end in slots])

    engine.dispose()

    return
