from unittest import TestCase
from calculate_features import calculate_feature, get_wq_db_last_date, select_model

class Test(TestCase):
    feature = 'ChlA'
    osm_id = '1346653'
    db_name = 'AIHABs'
    user = 'jakub'
    db_bands_table = 's2_points_eo_data'
    db_features_table = 'wq_points_results'
    db_models = 'models_table'
    model_name = 'AI_model_testx'
    default = True

    def test_get_wq_db_last_date(self):
        get_wq_db_last_date(self.osm_id, self.feature, self.db_name, self.user, self.db_features_table)

    def test_select_model(self):
        model, model_id = select_model(self.db_name, self.user, self.db_models, self.feature, self.osm_id,
                                    self.model_name, self.default)
        print(model_id)

    def test_calculate_feature(self):
        calculate_feature(self.feature, self.osm_id, self.db_name, self.user, self.db_bands_table,
                          self.db_features_table, self.db_models, self.model_name, self.default)
