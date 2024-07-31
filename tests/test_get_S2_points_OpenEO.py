from unittest import TestCase
from get_S2_points_OpenEO import get_s2_points_OEO, process_s2_points_OEO, get_sampling_points, check_job_error


class Test_S2_OpenEO(TestCase):
    osm_id = 3120599

    start_date = '2023-04-01'
    end_date = '2023-07-01'
    db_name = 'postgres'
    user = 'postgres'
    db_table = 's2_points_eo_data'

    db_table_reservoirs = 'water_reservoirs'
    db_table_points = 'selected_points'

    # logid = "j-2407228d5a0645c7957bc497278e759b"  # NoData --> F
    # logid = "j-2407237396d449fcbd7e86cec3ab96ba"  # OK --> F
    # logid = "j-240723f249704050804b772f742bb01a" # Spark --> T
    # logid = None  # None --> T

    logid = "j-2407236d154f4823baa5b61c43b9bd6f"

    def test_get_sampling_points(self):
        get_sampling_points(self.osm_id, self.db_name, self.user, self.db_table_reservoirs, self.db_table_points)

    def test_get_s2_points_oeo(self):
        get_s2_points_OEO(self.osm_id, self.db_name, self.user, self.db_table_reservoirs, self.db_table_points, self.db_table)

    def test_process_s2_points_OEO(self):
        point_layer = get_sampling_points(self.osm_id, self.db_name, self.user, self.db_table_reservoirs, self.db_table_points)
        process_s2_points_OEO(self.osm_id, point_layer, self.start_date, self.end_date, self.db_name, self.user, self.db_table)

    def test_check_job_error(self):
        data_available = check_job_error(self.logid)
        print(data_available)