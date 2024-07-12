from unittest import TestCase
from get_meteo import getHistoricalMeteoData, getPredictedMeteoData, getLastDateInDB, getLatLon

class Test(TestCase):
    user = 'jakub'
    db_name = 'AIHABs'
    db_table = 'meteo_history'
    db_table_forecast = 'meteo_forecast'
    db_table_reservoirs = 'water_reservoirs'

    # Definice proměnných

    osm_id = 6640987

    meteo_features = ["weather_code", "temperature_2m_max", "temperature_2m_min", "daylight_duration",
                      "sunshine_duration", "precipitation_sum", "wind_speed_10m_max", "wind_direction_10m_dominant",
                      "shortwave_radiation_sum"]

    def test_get_historical_meteo_data(self):
        getHistoricalMeteoData(self.osm_id, self.meteo_features, self.user, self.db_name, self.db_table, self.db_table_reservoirs)

    def test_get_predicted_meteo_data(self):
        getPredictedMeteoData(self.osm_id, self.meteo_features, self.user, self.db_name, self.db_table_forecast, self.db_table_reservoirs)

    def test_get_lat_lon(self):
        getLatLon(self.osm_id, self.db_name, self.user, self.db_table_reservoirs)

    def test_get_last_date_in_db(self):
        getLastDateInDB(self.osm_id, self.db_name, self.user, self.db_table)
