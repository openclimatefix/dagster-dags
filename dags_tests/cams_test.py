from nwp.assets.cams import fetch_cams_forecast_for_day
import unittest

import datetime as dt

class TestCams(unittest.TestCase):
    def test_cams(self):
        fetch_cams_forecast_for_day(dt.datetime.utcnow())

