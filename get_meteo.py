import pandas as pd
import openmeteo_requests
import requests_cache
import geopandas as gpd

from retry_requests import retry
from sqlalchemy import create_engine, exc, text
from datetime import datetime, timedelta


def getHistoricalMeteoData(osm_id, meteo_features, user, db_name, db_table, vect_db_table, time_zone='GMT'):
    """
    Get meteodata from Open-Meteo Historical Weather API and save it to PostGIS database for the particular OSM id and its location. The function fulfill the last data in the database. The time serries is daily from 2015-06-01 till one day before today.

    :param osm_id: ID of OSM object (water reservoir)
    :param meteo_features: List of meteo features
    :param user: Postgres DB user
    :param db_name: Postgres database name
    :param db_table: Postgres database table
    :param vect_db_table: PostGIS database table with water reservoirs
    :param time_zone: Time zone. Default GMT
    :return:
    """

    # Connect to PostGIS
    engine = create_engine('postgresql://{user}@/{db_name}'.format(user=user, db_name=db_name))

    # Get latitude and longitude
    lat, lon = getLatLon(osm_id, db_name, user, vect_db_table)

    # Getting the last date from database for particular OSM id.
    try:
        last_db_date = getLastDateInDB(osm_id, db_name, user, db_table)

        if last_db_date == None:
            datum = '2015-06-01'
            last_db_date = datetime.strptime(datum, "%Y-%m-%d").date()

        else:

            # Remove last date from database.
            sql_query = text("DELETE FROM meteo_history WHERE osm_id = :val1 AND date = :val2")
            conn = engine.connect()
            conn.execute(sql_query, {'val1': str(osm_id), 'val2': last_db_date})
            conn.commit()
            conn.close()

        start_date = last_db_date

    except:
        datum = "2015-06-01"
        start_date = datetime.strptime(datum, "%Y-%m-%d").date()

    end_date = datetime.now().date() - timedelta(days=1)

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": meteo_features,
        "timezone": time_zone
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    # print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    # print(f"Elevation {response.Elevation()} m asl")
    # print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    # print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s"),

    daily = response.Daily()

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}

    # Process daily data. The order of variables needs to be the same as requested.
    for i in range(len(meteo_features)):
        daily_data[meteo_features[i]] = daily.Variables(i).ValuesAsNumpy()

    daily_meteo = pd.DataFrame(data=daily_data)

    # Handling date
    daily_meteo["date"] = daily_meteo["date"].dt.tz_localize(None).dt.date

    # Add OSM ID to dataframe
    daily_meteo["osm_id"] = str(osm_id)

    # Save data to PostGIS
    daily_meteo.to_sql(db_table, con=engine, if_exists='append', index=False)
    engine.dispose()

    return

def getPredictedMeteoData(osm_id, meteo_features, user, db_name, vect_db_table, forecast_days=10, db_tab_prefix='meteo_forec_', time_zone='GMT'):
    """
    Get meteodata forecast from Open-Meteo Forecast API in daily step for particular OSM object id.

    :param osm_id: OSM object id (water reservoir)
    :param meteo_features: List of weather variables to get
    :param user: Database user
    :param db_name: Database name
    :param vect_db_table: PostGIS database table with water reservoirs
    :param forecast_days: Number of days to forecast. Default = 10, max = 16
    :param db_tab_prefix: Prefix for database table with forecast data
    :param time_zone: Time zone. Default = 'GMT'
    :return: Dataframe with meteo data forecast
    """

    # Connect to PostGIS
    engine = create_engine('postgresql://{user}@/{db_name}'.format(user=user, db_name=db_name))

    # Get latitude and longitude
    lat, lon = getLatLon(osm_id, db_name, user, vect_db_table)

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": meteo_features,
        "timezone": time_zone,
        "forecast_days": forecast_days
    }

    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    # print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    # print(f"Elevation {response.Elevation()} m asl")
    # print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    # print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s"),

    daily = response.Daily()

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}

    # Process daily data. The order of variables needs to be the same as requested.
    for i in range(len(meteo_features)):
        daily_data[meteo_features[i]] = daily.Variables(i).ValuesAsNumpy()

    daily_forecast = pd.DataFrame(data=daily_data)
    daily_forecast["osm_id"] = str(osm_id)

    # Handling date
    daily_forecast["date"] = daily_forecast["date"].dt.tz_localize(None).dt.date

    # Save data to PostGIS
    forecast_dbtab_name = db_tab_prefix + str(osm_id)
    daily_forecast.to_sql(forecast_dbtab_name, con=engine, if_exists='replace', index=False)
    engine.dispose()

    return daily_forecast

def getLatLon(osm_id, db_name, user, db_table):
    """
    Get latitude and longitude from OSM id

    :param osm_id: OSM object id
    :param db_name: Database name
    :param user: Database user
    :param db_table: Database table
    :return: Tuple of latitude and longitude
    """

    # Připojení k databázi PostGIS
    engine = create_engine('postgresql://{}@/{}'.format(user, db_name))

    sql = text("SELECT * FROM {db_table} WHERE osm_id = '{osm_id}'".format(osm_id=str(osm_id), db_table=db_table))

    # Spuštění SQL dotazu a načtení výsledků do GeoDataFrame
    gdf = gpd.read_postgis(sql, engine, geom_col='geometry')

    # Get latitude and longitude
    centroid = gdf.geometry.centroid
    lon = centroid.x.mean()
    lat = centroid.y.mean()

    engine.dispose()

    return lat, lon

def getLastDateInDB(osm_id, db_name, user, db_table):
    """
    Get last date from OSM id

    :param osm_id: OSM object id
    :param db_name: Database name
    :param user: Database user
    :param db_table: Database table
    :return: Last date in the database table for particular OSM id
    """

    try:
        # Connect to PostGIS
        engine = create_engine('postgresql://{}@/{}'.format(user, db_name))
        connection = engine.connect()

        # Define SQL query
        sql_query = text("SELECT MAX(date) FROM {db_table} WHERE osm_id = '{osm_id}'".format(osm_id=str(osm_id), db_table=db_table))

        # Running SQL query, conversion to DataFrame
        df = pd.read_sql(sql_query, connection)
        connection.close()
        engine.dispose()

        last_date = df.iloc[0,0]

    except exc.NoSuchTableError:
        print("The table does not exist. The last date will be 2015-06-01")
        last_date = '2015-06-01'

    return last_date

if __name__ == '__main__':

    # Připojení k databázi PostGIS
    user = 'jakub'
    db_name = 'AIHABs'
    db_table = 'meteo_history'
    vect_db_table = 'water_reservoirs'

    # Definice proměnných

    osm_id = 26133284

    meteo_features = ["weather_code", "temperature_2m_max", "temperature_2m_min", "daylight_duration", "sunshine_duration",
                  "precipitation_sum", "wind_speed_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum"]

    getHistoricalMeteoData(osm_id, meteo_features, user, db_name, db_table, vect_db_table)
    getPredictedMeteoData(osm_id, meteo_features, user, db_name, vect_db_table, forecast_days=6)




