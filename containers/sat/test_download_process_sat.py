"""Tests for the satellite processing pipeline.

Note that, since the files from EUMETSAT are so large,
they must be downloaded prior to running the tests - they
are too biug to include in the repository. As such, environment
variables must be set to authenticate with EUMETSAT
"""

import datetime as dt
import pathlib
import unittest

import download_process_sat as dps
import numpy as np
import pandas as pd
import xarray as xr
from satpy import Scene

class TestGetProductsIterator(unittest.TestCase):
    def test_get_products_iterator(self):
        sat_config = CONFIGS["seviri"]
        start = dt.datetime(2024, 1, 1, 0, 0, tzinfo=dt.UTC)
        end = dt.datetime(2024, 1, 1, 1, 0, tzinfo=dt.UTC)
        token = _gen_token()
        products = list(get_products_iterator(sat_config, start, end, token))
        self.assertEqual(len(products), 12)


class TestDownloadProcessSat(unittest.TestCase):
    paths: list[pathlib.Path]
    test_dataarrays: dict[str, xr.DataArray]

    @classmethod
    def setUpClass(cls) -> None:
        TIMESTAMP = pd.Timestamp("2024-01-01T00:00:00Z")

        token = dps._gen_token()

        for t in [TIMESTAMP + pd.Timedelta(t) for t in ["0m", "15m", "30m", "45m"]]:
            paths = dps.download_scans(
                sat_config=dps.CONFIGS["iodc"],
                folder=pathlib.Path("/tmp/test_sat_data"),
                scan_time=t,
                token=token,
            )
            cls.paths = paths

        attrs: dict = {
            "end_time": TIMESTAMP + pd.Timedelta("15m"),
            "modifiers": (),
            "orbital_parameters": {"projection_longitude": 45.5, "projection_latitude": 0.0,
                                   "projection_altitude": 35785831.0, "satellite_nominal_longitude": 45.5,
                                   "satellite_nominal_latitude": 0.0, "satellite_actual_longitude": 45.703605543834364,
                                   "satellite_actual_latitude": 7.281469039541501,
                                   "satellite_actual_altitude": 35788121.627292305},
            "reader": "seviri_l1b_native",
            "sensor": "seviri",
            "resolution": 3000.403165817,
            "start_time": dt.datetime(2024, 1, 1, 0, 0, tzinfo=dt.UTC),
            "platform_name": "Meteosat-9", "area": "Area ID: msg_seviri_iodc_3km",
        }

        cls.test_dataarrays = {
            "hrv": xr.DataArray(
                data=np.random.random((1, 1, 3712, 3712)),
                dims=["time", "variable", "x_geostationary", "y_geostationary"],
                coords={
                    "time": [pd.Timestamp("2024-01-01T00:00:00Z")],
                    "variable": ["HRV"],
                    "x_geostationary": np.arange(3712),
                    "y_geostationary": np.arange(3712),
                },
                attrs=attrs,
            ),
            "nonhrv": xr.DataArray(
                data=np.random.random((1, 11, 3712, 3712)),
                dims=["time", "variable", "x_geostationary", "y_geostationary"],
                coords={
                    "time": [pd.Timestamp("2024-01-01T00:00:00Z")],
                    "variable": [c.variable for c in dps.CHANNELS["nonhrv"]],
                    "x_geostationary": np.arange(3712),
                    "y_geostationary": np.arange(3712),
                },
                attrs=attrs,
            ),
        }

    def test_download_scans(self) -> None:
        self.assertGreater(len(self.paths), 0)

    def test_convert_scene_to_dataarray(self) -> None:
        scene = Scene(filenames={"seviri_l1b_native": [self.paths[0].as_posix()]})
        scene.load([c.variable for c in dps.CHANNELS["nonhrv"]])
        da = dps._convert_scene_to_dataarray(
            scene,
            band=dps.CHANNELS["nonhrv"][0].variable,
            area="RSS",
            calculate_osgb=False,
        )

        with self.subTest("Returned dataarray is correct shape"):
            self.assertDictEqual(
                dict(da.sizes),
                {"time": 1, "variable": 11, "x_geostationary": 3712, "y_geostationary": 3712},
            )
            self.assertIn("end_time", da.attrs)

    def test_rescale(self) -> None:
        da: xr.DataArray = dps._rescale(self.test_dataarrays["nonhrv"], channels=dps.CHANNELS["nonhrv"])

        self.assertGreater(da.values.max(), 0)
        self.assertLess(da.values.min(), 1)
        self.assertEqual(da.attrs, self.test_dataarrays["nonhrv"].attrs)

    def test_open_and_scale_data(self) -> None:
        ds: xr.Dataset | None = dps._open_and_scale_data([], self.paths[0].as_posix(), "nonhrv")

        if ds is None:
            self.fail("Dataset is None")

        ds.to_zarr("/tmp/test_sat_data/test.zarr", mode="w", consolidated=True)
        ds2 = xr.open_zarr("/tmp/test_sat_data/test.zarr")
        self.assertDictEqual(dict(ds.sizes), dict(ds2.sizes))
        self.assertNotEqual(dict(ds.attrs), {})

    def test_process_scans(self) -> None:

        out: str = dps.process_scans(
            dps.CONFIGS["iodc"],
            pathlib.Path("/tmp/test_sat_data"),
            pd.Timestamp("2024-01-01"),
            pd.Timestamp("2024-01-02"), "nonhrv",
        )

        self.assertTrue(False)

