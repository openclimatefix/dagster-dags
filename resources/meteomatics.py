import datetime as dt

import dagster as dg
import meteomatics.api
import pandas as pd
from pydantic import PrivateAttr

wind_coords = [
    (27.035, 70.515),
    (27.188, 70.661),
    (27.085, 70.638),
    (27.055, 70.72),
    (27.186, 70.81),
    (27.138, 71.024),
    (26.97, 70.917),
    (26.898, 70.996),
    (26.806, 70.732),
    (26.706, 70.81),
    (26.698, 70.875),
    (26.708, 70.982),
    (26.679, 71.027),
    (26.8, 71.128),
    (26.704, 71.127),
    (26.5, 71.285),
    (26.566, 71.369),
    (26.679, 71.452),
    (26.201, 71.295),
    (26.501, 72.512),
    (26.463, 72.836),
    (26.718, 73.049),
    (26.63, 73.581),
    (24.142, 74.731),
    (23.956, 74.625),
    (23.657, 74.772),
]

solar_coords = [
    (26.264, 71.237),
    (26.671, 71.262),
    (26.709, 71.413),
    (26.871, 71.49),
    (26.833, 71.815),
    (26.792, 72.008),
    (26.892, 72.06),
    (27.179, 71.841),
    (27.476, 71.971),
    (27.387, 72.218),
    (27.951, 72.987),
    (28.276, 73.341),
    (24.687, 75.132),
    (26.731, 73.2),
    (26.524, 72.862),
    (27.207, 74.252),
    (27.388, 72.208),
    (27.634, 72.698),
    (28.344, 73.435),
    (28.022, 73.067),
]

wind_parameters = [
    "wind_speed_10m:ms",
    "wind_speed_100m:ms",
    "wind_speed_200m:ms",
    "wind_dir_10m:d",
    "wind_dir_100m:d",
    "wind_dir_200m:d",
    "wind_gusts_10m:ms",
    "wind_gusts_100m:ms",
    "wind_gusts_200m:ms",
    "air_density_10m:kgm3",
    "air_density_25m:kgm3",
    "air_density_100m:kgm3",
    "air_density_200m:kgm3",
    "cape:Jkg",
]

solar_parameters = [
    "direct_rad:W",
    "diffuse_rad:W",
    "global_rad:W",
]


class MeteomaticsAPIResource(dg.ConfigurableResource):
    """A resource for interacting with the Meteomatics API."""

    # Authentication for the API, set via environment
    username: str
    password: str

    # Subscription limits
    _subscription_min_date: dt.datetime = PrivateAttr()
    _subscription_max_requestable_parameters = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        """Set up the resource according to subscription limits."""
        self._subscription_min_date = dt.datetime(2019, 3, 19, tzinfo=dt.UTC)
        self._subscription_max_requestable_parameters = 10

    def query_api(self, start: dt.datetime, end: dt.datetime, energy_type: str) -> pd.DataFrame:
        """Query the Meteomatics API for NWP data."""
        coords, params = (
            (wind_coords, wind_parameters)
            if energy_type == "wind"
            else (solar_coords, solar_parameters)
        )

        # Ensure subscription limits are respected
        # * Split the parameters into groups of max size
        groups = [
            params[i : i + self._subscription_max_requestable_parameters]
            for i in range(0, len(params), self._subscription_max_requestable_parameters)
        ]

        dfs: list[pd.DataFrame] = []
        try:
            for param_group in groups:
                df: pd.DataFrame = meteomatics.api.query_time_series(
                    coordinate_list=coords,
                    startdate=max(start, self._subscription_min_date),
                    enddate=max(end, self._subscription_min_date),
                    interval=dt.timedelta(minutes=15),
                    parameters=param_group,
                    username=self.username,
                    password=self.password,
                    model="ecmwf-ifs",
                )
                dfs.append(df)
        except Exception as e:
            raise dg.Failure(
                description=f"Failed to query the Meteomatics API: {e}",
            ) from e

        if len(dfs) > 1:
            return dfs[0].join(dfs[1:])
        else:
            return dfs[0]
