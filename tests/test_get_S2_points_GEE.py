from unittest import TestCase
from get_S2_points_GEE import process_sentinel2_points_data, get_sentinel2_data
from get_random_points import get_sampling_points

class Test(TestCase):
    ee_project = 'ee-bromjakub'
    osm_id = 15444638  # 1239458
    user = 'jakub'
    db_name = 'AIHABs'
    db_table_points = 'selected_points'
    db_table_reservoirs = 'water_reservoirs'
    db_table_S2_points_data = 's2_points_data'
    start_date = '2020-01-01'
    end_date = '2021-01-01'
    def test_process_sentinel2_points_data(self):
        point_layer = get_sampling_points(self.osm_id, self.db_name, self.user, self.db_table_reservoirs, self.db_table_points)
        point_layer['lat'] = point_layer.geometry.y
        point_layer['lon'] = point_layer.geometry.x
        process_sentinel2_points_data(point_layer, self.start_date, self.end_date, self.db_name, self.user,
                                      self.db_table_S2_points_data)

    def test_get_sentinel2_data(self):
        get_sentinel2_data(self.ee_project, self.osm_id, self.db_name, self.user, self.db_table_reservoirs, self.db_table_points,
                           self.db_table_S2_points_data)
