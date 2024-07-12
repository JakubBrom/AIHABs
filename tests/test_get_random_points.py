from unittest import TestCase
from get_random_points import get_sampling_points

class Test(TestCase):
    osm_id = 1239458
    db_name = 'AIHABs'
    user = 'jakub'
    db_table_reservoirs = 'water_reservoirs'
    db_table_points = 'selected_points'
    def test_get_sampling_points(self):
        get_sampling_points(self.osm_id, self.db_name, self.user, self.db_table_reservoirs, self.db_table_points)
