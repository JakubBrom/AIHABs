import os
import warnings

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import random
import datetime
import get_meteo as gm
from prophet import Prophet
from prophet.plot import plot_plotly, plot_components_plotly
import time

def is_nfl_season(ds):
    date = pd.to_datetime(ds)
    return (date.month > 10 or date.month < 4)


if __name__ == '__main__':

    in_file = "data_test/priklad_orlik.csv"
    in_meteo = "data_test/meteo_test.csv"
    pred_meteo = "data_test/meteo_test_predict.csv"

    # Reading WQ data
    cha_dat = pd.read_csv(in_file)
    cha_dat["date"] = pd.to_datetime(cha_dat["date"], format="%Y-%m-%d")

    # Reading meteo data --> TODO: vytvořit složku meteo, do který by se data ukládala - definovat podle ID nádrže, definovat souřadnice podle souřadnic nádrže nebo přebírat z SQL databáze
    meteo_features = ["weather_code", "temperature_2m_max", "temperature_2m_min", "daylight_duration", "sunshine_duration",
                  "precipitation_sum", "wind_speed_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum"]

    if os.path.exists(in_meteo):
        met_dat = pd.read_csv(in_meteo)
    else:
        met_dat = gm.getHistoricalMeteoData(50, 15, '2015-06-01', '2019-10-26', meteo_features)
        met_dat.to_csv(in_meteo, index = False, index_label=False)

    met_dat["date"] = pd.to_datetime(met_dat["date"], format="%Y-%m-%d")

    # merging meteodata and WQ data
    df_all = pd.merge(met_dat,cha_dat, how="outer", on="date")

    # selection of the data for prediction
    df_sel = df_all.loc[:, ["date", "b15", "weather_code", "temperature_2m_max", "temperature_2m_min", "daylight_duration", "sunshine_duration",
                  "precipitation_sum", "wind_speed_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum"]].copy()
    df_sel.rename(columns={"date": "ds", "b15": "y"}, inplace=True)

    # reading data for prediction
    if os.path.exists(pred_meteo):
        pred_dat = pd.read_csv(pred_meteo)
    else:
        pred_dat = gm.getHistoricalMeteoData(50, 15, '2015-07-04', '2020-10-12', meteo_features)
        pred_dat.to_csv(pred_meteo, index = False, index_label=False)

    pred_dat["date"] = pd.to_datetime(pred_dat["date"], format="%Y-%m-%d")

    # Predikce
    t0 = time.time()
    # Set max value
    # df_sel["cap"] = 800
    # Set min value
    df_sel["floor"] = 0

    df_sel['on_season'] = df_sel['ds'].apply(is_nfl_season)
    df_sel['off_season'] = ~df_sel['ds'].apply(is_nfl_season)

    m = Prophet(growth="flat")   # dal by se specifikovat changepoint
    m.add_seasonality(name='weekly_on_season', period=7, fourier_order=5, condition_name='on_season')
    m.add_seasonality(name='weekly_off_season', period=7, fourier_order=5, condition_name='off_season')

    # m.add_regressor("weather_code")
    # m.add_regressor("temperature_2m_max")
    # m.add_regressor("temperature_2m_min")
    # m.add_regressor("daylight_duration")
    # m.add_regressor("sunshine_duration")
    # m.add_regressor("precipitation_sum")
    # m.add_regressor("wind_speed_10m_max")
    # m.add_regressor("wind_direction_10m_dominant")
    # m.add_regressor("shortwave_radiation_sum")

    m.fit(df_sel)

    future = m.make_future_dataframe(periods=20)
    # future['cap'] = 800
    future['floor'] = 0
    future['on_season'] = future['ds'].apply(is_nfl_season)
    future['off_season'] = ~future['ds'].apply(is_nfl_season)

    # future["weather_code"] = pred_dat["weather_code"]
    # future["temperature_2m_max"] = pred_dat["temperature_2m_max"]
    # future["temperature_2m_min"] = pred_dat["temperature_2m_min"]
    # future["daylight_duration"] = pred_dat["daylight_duration"]
    # future["sunshine_duration"] = pred_dat["sunshine_duration"]
    # future["precipitation_sum"] = pred_dat["precipitation_sum"]
    # future["wind_speed_10m_max"] = pred_dat["wind_speed_10m_max"]
    # future["wind_direction_10m_dominant"] = pred_dat["wind_direction_10m_dominant"]
    # future["shortwave_radiation_sum"] = pred_dat["shortwave_radiation_sum"]

    future.tail()

    forecast = m.predict(future)
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()

    fig1 = m.plot(forecast)
    fig1.savefig("img1.jpg")
    fig2 = m.plot_components(forecast)
    fig2.savefig("img2.jpg")
    t1 = time.time()

    t2 = t1 - t0

    print(t2)