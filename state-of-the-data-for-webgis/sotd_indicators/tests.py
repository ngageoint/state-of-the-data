from src.sotd_indicators.Indicator import Indicator
from src.osm_runner import *

from arcgis.features import SpatialDataFrame
from arcgis.gis import GIS

import unittest


class TestIndicators(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.indicator = Indicator()
        cls.indicator.load_config(r'C:\Users\jeff8977\Desktop\SOTD\config.ini')
        cls.indicator.set_grid_sdf()
        cls.indicator.set_features()

    def test_poac(self):

        res = self.indicator.run_poac('ZI001_SDP')

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertEqual(res[1][0]['success'], True)

    def test_curr(self):

        res = self.indicator.run_curr('ZI001_SDV')

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertEqual(res[1][0]['success'], True)

    def test_them(self):

        res = self.indicator.run_them('ZI026_CTUU')

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertEqual(res[1][0]['success'], True)

    def test_srln(self):

        res = self.indicator.run_srln('ZI001_SDP', 'ZI001_SPS')

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertEqual(res[1][0]['success'], True)

    def test_cmpl(self):

        bbox = '(37.708132, -122.513617, 37.832132, -122.349607)'
        osm_sdf = gen_osm_sdf('line', bbox, osm_tag='highway')

        res = self.indicator.run_cmpl(osm_sdf)

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertEqual(res[1][0]['success'], True)

    def test_logc(self):

        res = self.indicator.run_logc(
            'DEFICIENCY_CNT',
            'DEFICIENCY',
            'HADR',
            r'C:\Users\jeff8977\Desktop\SOTD\src\sotd_indicators\attributes.json'
        )

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertEqual(res[1][0]['success'], True)


class TestNoUrlIndicators(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.indicator = Indicator()
        cls.indicator.load_config(r'C:\Users\jeff8977\Desktop\SOTD\config_no_urls.ini')
        cls.indicator.set_grid_sdf()
        cls.indicator.set_features()
        cls.indicator.gis_conn = GIS(
            'http://www.arcgis.com/home',
            'test',
            'test'
        )

    def test_poac(self):

        res = self.indicator.run_poac('ZI001_SDP')

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertIsInstance(res[1], str)

    def test_curr(self):

        res = self.indicator.run_curr('ZI001_SDV')

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertIsInstance(res[1], str)

    def test_them(self):

        res = self.indicator.run_them('ZI026_CTUU')

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertIsInstance(res[1], str)

    def test_srln(self):

        res = self.indicator.run_srln('ZI001_SDP', 'ZI001_SPS')

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertIsInstance(res[1], str)

    def test_cmpl(self):

        bbox = '(37.708132, -122.513617, 37.832132, -122.349607)'
        osm_sdf = gen_osm_sdf('line', bbox, osm_tag='highway')

        res = self.indicator.run_cmpl(osm_sdf)

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertIsInstance(res[1], str)

    def test_logc(self):

        res = self.indicator.run_logc(
            'DEFICIENCY_CNT',
            'DEFICIENCY',
            'HADR',
            r'C:\Users\jeff8977\Desktop\SOTD\src\sotd_indicators\attributes.json'
        )

        self.assertIsInstance(res[0], SpatialDataFrame)
        self.assertIsInstance(res[1], str)
