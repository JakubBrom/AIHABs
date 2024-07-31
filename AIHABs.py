# Imports
from get_S2_points_OpenEO import authenticate_OEO, get_s2_points_OEO
from calculate_features import calculate_feature
from get_meteo import getHistoricalMeteoData, getPredictedMeteoData

class AIHABs:

    def __init__(self):

        # Authenticate after starting the program
        authenticate_OEO()          # The OpenEO needs to be authenticated first

        self.db_name = "postgres"
        self.user = "postgres"
        self.db_table_reservoirs = "water_reservoirs"
        self.db_table_points = "selected_points"
        self.db_table_S2_points_data = "s2_points_eo_data"  # db_bands_table
        self.db_features_table = "wq_points_results"
        self.db_models = "models_table"
        self.db_table_forecast = "meteo_forecast"
        self.db_table_history = "meteo_history"

        self.model_name = None,
        self.default_model = False

        self.osm_id: str = "123456"
        self.feature = "ChlA"
        self.meteo_features = ["weather_code", "temperature_2m_max", "temperature_2m_min", "daylight_duration",
                      "sunshine_duration", "precipitation_sum", "wind_speed_10m_max", "wind_direction_10m_dominant",
                      "shortwave_radiation_sum"]
        self.forecast_days = 16


    def run_analyse(self):

        # get Sentinel-2 data
        get_s2_points_OEO(self.osm_id, self.db_name, self.user, self.db_table_reservoirs, self.db_table_points, self.db_table_S2_points_data)

        # calculate WQ features --> new AI models
        calculate_feature(self.feature, self.osm_id, self.db_name, self.user, self.db_table_S2_points_data, self.db_features_table, self.db_models)

        # get meteodata
        # get historical meteodata
        getHistoricalMeteoData(self.osm_id, self.meteo_features, self.user, self.db_name, self.db_table_history, self.db_table_reservoirs)
        # get predicted meteodata
        getPredictedMeteoData(self.osm_id, self.meteo_features, self.user, self.db_name, self.db_table_forecast, self.db_table_reservoirs, self.forecast_days)

        # parse data together
        # parse Sentinel and meteodata


        # inputation of missing values (based on AI)

        # run AI time series analysis
        pass

