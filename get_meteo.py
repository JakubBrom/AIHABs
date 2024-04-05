import pandas as pd
import numpy as np

import openmeteo_requests
import requests_cache
from retry_requests import retry

from matplotlib import pyplot as plt

def getHistoricalMeteoData(lat, lon, start_date, end_date, meteo_features, time_zone='GMT'):
    """
    Get meteodata from Open-Meteo Historical Weather API.

    :param lat: Latitude (dec.)
    :param lon: Longitude (dec.)
    :param start_date: The start of the time series.
    :param end_date: The end of the time series.
    :param meteo_features: List of meteo features
    :param time_zone: Time zone. Deafult GMT
    :return: Pandas DataFrame
    """

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

    return daily_meteo

def getPredictedMeteoData(lat, lon, meteo_features, forecast_days=10, time_zone='GMT'):
    """
    Get meteodata forecast from Open-Meteo Forecast API

    :param lat: Latitude (dec.)
    :param lon: Longitude
    :param meteo_features: List of meteo features
    :param forecast_days: Number of days predicted (max 16)
    :param time_zone: Time zone. Default GMT
    :return: Pandas DataFrame
    """

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

    # # Handling date
    # daily_forecast["date"] = daily_forecast["date"].dt.tz_localize(None).dt.date

    return daily_forecast


if __name__ == '__main__':

    meteo_features = ["weather_code", "temperature_2m_max", "temperature_2m_min", "daylight_duration", "sunshine_duration",
                  "precipitation_sum", "wind_speed_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum"]

    df_history = getHistoricalMeteoData(50, 15, '2023-06-01', '2023-07-01', meteo_features)
    df_forec = getPredictedMeteoData(lat=50, lon=15, meteo_features=meteo_features,forecast_days=10)
    print(df_forec)
    plt.plot(df_forec["temperature_2m_max"])
    plt.show()

# TODO: Možná bude potřeba pořešit formát data/času v df - jenom datum

