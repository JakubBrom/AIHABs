from unittest import TestCase
from get_S2_points_OpenEO import get_s2_points_OEO, process_s2_points_OEO, get_sampling_points


class Test_S2_OpenEO(TestCase):
    osm_id = 1239458

    start_date = '2023-04-01'
    end_date = '2024-05-01'
    db_name = 'AIHABs'
    user = 'jakub'
    db_table = 's2_points_eo_data'

    db_table_reservoirs = 'water_reservoirs'
    db_table_points = 'selected_points'

    def test_get_sampling_points(self):
        get_sampling_points(self.osm_id, self.db_name, self.user, self.db_table_reservoirs, self.db_table_points)

    def test_get_s2_points_oeo(self):
        get_s2_points_OEO(self.osm_id, self.db_name, self.user, self.db_table_reservoirs, self.db_table_points, self.db_table)

    def test_process_s2_points_OEO(self):
        point_layer = get_sampling_points(self.osm_id, self.db_name, self.user, self.db_table_reservoirs, self.db_table_points)
        process_s2_points_OEO(point_layer, self.start_date, self.end_date, self.db_name, self.user, self.db_table)