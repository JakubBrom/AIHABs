import json
import os
import time
import openeo
import tempfile
import warnings
import scipy.signal
import uuid

import pandas as pd

import numpy as np
import geopandas as gpd

from datetime import datetime, timedelta
from sqlalchemy import create_engine, exc, text
from shapely.geometry import Point

from AIHABs_wrappers import measure_execution_time
from get_random_points import get_sampling_points
from get_meteo import getLastDateInDB


def authenticate_OEO():
    # Authenticate
    connection = openeo.connect(url="openeo.dataspace.copernicus.eu")
    connection.authenticate_oidc()

    return connection


@measure_execution_time
def process_s2_points_OEO(point_layer, start_date, end_date, db_name, user, db_table, max_cc=30, cloud_mask=True):
    """
    The function processes Sentinel-2 satellite data from the Copernicus Dataspace Ecosystem. The function
    retrieves data based on the specified parameters (cloud mask) for randomly selected points within the reservoir (
    point layer). The S2 data are downloaded for defined time period. The data are stored to the PostGIS database.
    The output is a GeoDataFrame.

    Parameters:
    :param point_layer: Point layer (GeoDataFrame)
    :param start_date: Start date
    :param end_date: End date
    :param db_name: Database name
    :param user: Database user
    :param db_table: Database table
    :param max_cc: Maximum cloud cover
    :param cloud_mask: Apply cloud mask
    :return: GeoDataFrame with Sentinel-2 data for the randomly selected points for the defined time period
    """

    # Authenticate
    connection = authenticate_OEO()

    # Transform input GeoDataFrame layer into json
    points = json.loads(point_layer.to_json())

    # Connect to PostGIS
    engine = create_engine('postgresql://{user}@/{db_name}'.format(user=user, db_name=db_name))

    # Get bands names
    collection_info = connection.describe_collection("SENTINEL2_L2A")
    bands = collection_info['cube:dimensions']['bands']
    band_list = bands['values'][0:15]

    # Getting data
    datacube = connection.load_collection(
        "SENTINEL2_L2A",
        temporal_extent=[start_date, end_date],
        max_cloud_cover=max_cc,
        bands=band_list,
    )

    # Apply cloud mask etc.
    if cloud_mask:

        scl = datacube.band("SCL")
        mask = ~((scl == 6) | (scl == 2))

        # 2D gaussian kernel
        g = scipy.signal.windows.gaussian(11, std=1.6)
        kernel = np.outer(g, g)
        kernel = kernel / kernel.sum()

        # Morphological dilation of mask: convolution + threshold
        mask = mask.apply_kernel(kernel)
        mask = mask > 0.1

        datacube_masked = datacube.mask(mask)

    else:
        datacube_masked = datacube

    # Datacube aggregation
    aggregated = datacube_masked.aggregate_spatial(
        geometries=points,
        reducer="mean",
    )

    # Run the job
    job = aggregated.execute_batch(out_format="CSV")

    # Download the results
    csv_file = f"{uuid.uuid4()}.csv"
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp', csv_file)
    job.get_results().download_file(csv_path)
    df = pd.read_csv(csv_path)

    if df.get('date') is not None:
        # Convert date do isoformat
        df['date'] = pd.to_datetime(df['date']).dt.date

        # Remove missing values
        df_all = df.dropna(axis=0, how='any')

        # Rename columns
        df_all = df_all.rename(columns={'feature_index': 'PID'})
        for i in range(0, len(band_list)):
            df_all = df_all.rename(columns={'avg(band_{})'.format(i): band_list[i]})

        # Add OSM id
        df_all['osm_id'] = point_layer['osm_id'][0]

        # Convert to GeoDataFrame
        latlon = pd.DataFrame(point_layer['PID'])
        latlon['lat'] = point_layer.geometry.y
        latlon['lon'] = point_layer.geometry.x
        df_all = df_all.merge(latlon, on='PID', how='left')

        geometries = [Point(xy) for xy in zip(df_all['lon'], df_all['lat'])]
        gdf_out = gpd.GeoDataFrame(df_all, geometry=geometries, crs='epsg:4326')

        # Save the results to the database
        gdf_out.to_postgis(db_table, con=engine, if_exists='append', index=False)
        engine.dispose()

    else:
        df_all = pd.DataFrame()
        gdf_out = df_all

    engine.dispose()
    #os.remove(csv_path)

    return gdf_out

@measure_execution_time
def get_s2_points_OEO(osm_id, db_name, user, db_table_reservoirs, db_table_points, db_table_S2_points_data,
                       start_date=None, end_date=None, n_points_max=5000, **kwargs):
    """
    This function is a wrapper for the get_sentinel2_data function. It calls it with the defined parameters,
    manage the time windows and the database connection.

    :param osm_id: OSM water reservoir id
    :param db_name: Database name
    :param user: Database user
    :param db_table_reservoirs: Database table with water reservoirs
    :param db_table_points: Database table with points for reservoirs
    :param db_table_S2_points_data: Database table with Sentinel-2 data where the data are stored
    :param start_date: Start date
    :param end_date: End date
    :param n_points_max: Maximum number of points for water reservoir
    :param kwargs: Kwargs
    :return: None
    """

    # Connect to PostGIS
    engine = create_engine('postgresql://{user}@/{db_name}'.format(user=user, db_name=db_name))

    # Get points
    point_layer = get_sampling_points(osm_id, db_name, user, db_table_reservoirs, db_table_points)

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
            start_date = '2015-06-01'

        st_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    # Set end date
    if end_date is None:
        end_date = datetime.now().date()  # Up to today
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if st_date > end_date:
        print('Data for period from {st_date} to {end_date} are not available. Data will not be downloaded'.format(st_date=st_date,
                                                                                            end_date=end_date))

        return

    print('Data for period from {st_date} to {end_date} will be downloaded'.format(st_date=st_date, end_date=end_date))

    # Set time windows
    # Create chunks for time series
    # Get possible length of steps (chunks)
    n_points: int = len(point_layer)

    step_length = int(n_points_max) // (n_points/100)

    n_days = (end_date - st_date).days

    n_chunks = n_days // step_length + 1

    # Create time windows
    t_delta = int((end_date - st_date).days / n_chunks)

    if t_delta < 2:
        t_delta = 2

    freq = '{tdelta}D'.format(tdelta=t_delta)
    date_range = pd.date_range(start=st_date, end=end_date, freq=freq)
    slots = [(date_range[i].date().isoformat(), (date_range[i + 1] - timedelta(days=1)).date().isoformat()) for i in
             range(len(date_range) - 1)]
    slots.append((date_range[-1].date().isoformat(), end_date.isoformat()))

    for i in range(len(slots)):
        # print(slots[i][0], slots[i][1])

        try:
            process_s2_points_OEO(point_layer, slots[i][0], slots[i][1], db_name, user, db_table_S2_points_data)

        except Exception as e:
            warnings.warn("Attempt failed. Error: {error}. The time window will be splitted to smaller "
                          "windows".format(error=str(e)), stacklevel=2)

            st_in_slot = datetime.strptime(slots[i][0], "%Y-%m-%d").date()
            end_in_slot = datetime.strptime(slots[i][1], "%Y-%m-%d").date()
            n_days_window = (end_in_slot - st_in_slot).days


            if n_days_window > 30:
                t_delta_window = 30
            else:
                t_delta_window = n_days_window

            freq_window = '{tdelta}D'.format(tdelta=t_delta_window)
            date_range_window = pd.date_range(start=st_in_slot, end=end_in_slot, freq=freq_window)
            slots_window = [(date_range_window[j].date().isoformat(), (date_range_window[j + 1] - timedelta(
                days=1)).date().isoformat()) for j in range(len(date_range_window) - 1)]
            slots_window.append((date_range_window[-1].date().isoformat(), end_in_slot.isoformat()))

            #
            for slot in range(len(slots_window)):
                # Attempt to download data in the time windows. If the data are not available, the attempt will be
                # repeated 5 times. In case of an error, the function will use shorter time windows as a protection
                # of the missing data. Because there can be some blocks in the server, the function is sleeping for 1
                # second between attempts.

                max_attempts = 5
                attempt = 0
                success = False

                while attempt < max_attempts and not success:
                    try:
                        process_s2_points_OEO(point_layer, slots_window[slot][0], slots_window[slot][1], db_name, user,
                                              db_table_S2_points_data)
                        success = True
                    except Exception as e:
                        warnings.warn("Attempt {attempt} failed. Error: {error}".format(attempt=attempt, error=str(e)),
                                      stacklevel=2)
                        attempt += 1
                        time.sleep(1)       # sleep for 1 second because the possibly unblocking the server

    engine.dispose()

    return
