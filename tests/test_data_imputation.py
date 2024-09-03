from unittest import TestCase
from data_imputation import create_dataset, data_imputation
from matplotlib import pyplot as plt


class Test(TestCase):
    osm_id: str = 15444638
    feature = 'ChlA'

    db_name = 'postgres'
    user = 'postgres'
    db_table = 's2_points_eo_data'

    db_wq_results = 'wq_points_results'
    db_table_history = 'meteo_history'
    db_table_forecast = 'meteo_forecast'
    model_id = 'f0f13295-2068-436a-a900-a7fff15ec9a7'

    def test_create_dataset(self):
        dataset, gdf = create_dataset(self.db_name, self.user, self.osm_id, self.feature, self.model_id,
                                      self.db_wq_results, self.db_table_history, freq='D')
        print(gdf)
        print(dataset)


    def test_data_imputation(self):
        df_imputed, df_smooth = data_imputation(self.db_name, self.user, self.osm_id, self.feature, self.model_id, self.db_wq_results, self.db_table_history, freq='W', t_shift=1)
        print(df_imputed)
        print(df_smooth)

        # Plotting results
        fig_list = [0, 15, 23, 39]

        for i in range(len(fig_list)):
            try:
                plt.figure(figsize=(20, 6))
                plt.title(f'Reservoir no.: {self.osm_id}; Point no.: {fig_list[i]}')
                # plt.plot(df_full[i], 'o')
                plt.plot(df_imputed['feature_value'].where(df_imputed['PID'] == fig_list[i]), alpha=0.2)
                plt.plot(df_smooth['feature_value'].where(df_smooth['PID'] == fig_list[i]), alpha=0.8)
            except:
                pass

        plt.show()