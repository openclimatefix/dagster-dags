"""Dagster resource for accessing the Sheffield Solar API."""

import dataclasses
import datetime as dt
import functools
import io
import multiprocessing
import time

import dagster as dg
import pandas as pd
import requests


@dataclasses.dataclass
class SheffieldSolarRawdataRequest:
    """Parameters for the Sheffield Solar API."""

    start: dt.datetime
    end: dt.datetime
    period_mins: int = 30

    def __post_init__(self) -> None:
        """Validate the initialisation parameters."""
        # Check start and end are in the past, and end > start
        now: dt.datetime = dt.datetime.now(dt.UTC)
        base_err: str = "Cannot initialize SheffieldSolarParams object"
        if self.start > now:
            raise ValueError(f"{base_err}: start time must be in the past")
        if self.end > now:
            raise ValueError("{base_err}: end time must be in the past")
        if self.end <= self.start:
            raise ValueError("{base_err}: end time must be after start time")
        if self.period_mins not in [5, 30]:
            raise ValueError("{base_err}: period_mins must be 5 or 30")
        self.start = self.start.astimezone(dt.UTC).replace(tzinfo=None)
        self.end = self.end.astimezone(dt.UTC).replace(tzinfo=None)

    def endpoint(self) -> str:
        """Return the API endpoint for the request."""
        if self.period_mins == 5:
            return "rawdata/api/v4/reading_integrated_5mins"
        else:
            return "rawdata/api/v4/reading_integrated"


    def as_params(self, user_id: str, api_key: str) -> list[dict[str, str]]:
        """Return the request as a list of parameter dictionaries.

        Each dictionary represents one period of the request.
        """
        ticks: pd.DatetimeIndex = pd.date_range(
            start=pd.to_datetime(self.start).ceil(f"{self.period_mins}T"),
            end=pd.to_datetime(self.end).ceil(f"{self.period_mins}T"),
            freq=f"{self.period_mins}T",
            inclusive="left", # Don't include the end time
        )

        params_list: list[dict[str, str]] = [
            {
                "start_at": tick.isoformat(),
                "end_at": (tick + pd.Timedelta(minutes=self.period_mins)).isoformat(),
                "user_id": user_id,
                "api_key": api_key,
            }
            for tick in ticks
        ]

        return params_list

class SheffieldSolarAPIResource(dg.ConfigurableResource):
    """Dagster resource for accessing the Sheffield Solar API."""

    user_id: str
    api_key: str
    base_url: str = "https://api.pvlive.uk"
    delay_multiplier: int = 2
    retries: int = 5
    n_processes: int = 10

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Set up the Sheffield Solar API resource for execution."""
        self._log = context.log

    def _query(
        self,
        endpoint: str,
        params: dict[str, str],
    ) -> pd.DataFrame:
        """Query the Sheffield Solar API.

        Args:
            endpoint: The API endpoint to query.
            params: The query parameters.
        """
        url: str = f"{self.base_url}/{endpoint}"
        url += "?" + "&".join([f"{k}={v}" for k, v in params.items()])
        num_attempts: int = 1

        while num_attempts <= self.retries:
            try:
                response = requests.get(url, timeout=60*10)
            except requests.exceptions.HTTPError as e:
                time.sleep(0.5 * num_attempts * self.delay_multiplier)
                if num_attempts == self.retries:
                    raise e
                continue

            if response.status_code != 200:
                raise ValueError(f"HTTP error: {response.status_code}")
            else:
                if "Your api key is not valid" in response.text:
                    raise ValueError("Invalid API key/User ID combination")
                elif "Your account does not give access" in response.text:
                    raise ValueError("API key/User ID does not give access to requested data")
                elif "Missing user_id" in response.text:
                    raise ValueError("Missing user_id")
                else:
                    try:
                        df: pd.DataFrame = pd.read_csv(
                            io.StringIO(response.text),
                            parse_dates=True,
                        )
                        return df
                    except Exception as e:
                        raise ValueError(f"Error parsing API query result: {e}") from e

            num_attempts += 1


    def request(
        self,
        request: SheffieldSolarRawdataRequest,
    ) -> pd.DataFrame:
        """Request data from the Sheffield Solar API."""
        pool = multiprocessing.Pool(processes=self.n_processes)
        df_chunks: pd.DataFrame = pool.map(
            functools.partial(self._query, endpoint=request.endpoint()),
            request.as_params(self.user_id, self.api_key),
        )
        return pd.concat(df_chunks)

