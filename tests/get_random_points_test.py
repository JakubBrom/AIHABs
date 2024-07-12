import unittest
import geopandas as gpd
from pathlib import Path
import os

from get_random_points import point_mesh
from matplotlib import pyplot as plt

class MyTestCase(unittest.TestCase):

    def test_point_mesh(self):

        current_dir = Path(__file__).parent
        project_dir = current_dir.parent
        data_dir = project_dir / 'data_test'

        # Získání cesty k souboru 'soubor.txt' v adresáři 'data'
        polygon_path = data_dir / 'Vanern_wgs.gpkg'

        polygon = gpd.read_file(polygon_path)
        points = point_mesh(polygon)

        ax = polygon.plot(color='lightgray', figsize=(8, 8))
        points.plot(ax=ax, alpha=0.5, color='orange', edgecolor='black')
        plt.title("Random points")
        plt.show()

        return


if __name__ == '__main__':
    unittest.main()