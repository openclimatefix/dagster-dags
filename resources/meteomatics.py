import datetime as dt

import dagster as dg
import meteomatics.api
import pandas as pd
from pydantic import PrivateAttr



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

    def query_api(self, start: dt.datetime, end: dt.datetime, coords: list[tuple[float, float]], params: list[str]) -> pd.DataFrame:
        """Query the Meteomatics API for NWP data."""

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
