"""Dagster resource for accessing the Sheffield Solar API."""

import abc
import dataclasses
import datetime as dt
import functools
import io
import multiprocessing
from typing import override

import dagster as dg
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry


class SheffieldSolarRequest(abc.ABC):
    """Abstract base class for Sheffield Solar API requests."""

    @abc.abstractmethod
    def endpoint(self) -> str:
        """Return the API endpoint for the request."""

    @abc.abstractmethod
    def as_query_params(self, user_id: str, api_key: str) -> list[dict[str, str]]:
        """Return the request as a set of query parameter dictionaries.

        These dictionaries are suitable for passing to the Sheffield Solar API
        as query parameters.
        """


@dataclasses.dataclass
class SheffieldSolarRawdataRequest(SheffieldSolarRequest):
    """Parameters for a rawdata request from Sheffield Solar."""

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

    @override
    def endpoint(self) -> str:
        if self.period_mins == 5:
            return "rawdata/api/v4/reading_integrated_5mins"
        else:
            return "rawdata/api/v4/reading_integrated"

    @override
    def as_query_params(self, user_id: str, api_key: str) -> list[dict[str, str]]:
        """Return the request as a list of parameter dictionaries.

        A RawdataRequest object may cover multiple HTTP requests to Sheffield
        Solar's API, depending on the period and the start and end times. This
        returns a dictionary of HTTP request parameters for each request.
        """
        ticks: pd.DatetimeIndex = pd.date_range(
            start=pd.to_datetime(self.start).ceil(f"{self.period_mins}min"),
            end=pd.to_datetime(self.end).ceil(f"{self.period_mins}min"),
            freq=f"{self.period_mins}min",
            inclusive="left", # Don't include the end time
        )

        params_list: list[dict[str, str]] = [
            {
                "start_at": tick.isoformat(),
                "end_at": (tick + pd.Timedelta(minutes=self.period_mins)).isoformat(),
                "user_id": user_id,
                "key": api_key,
            }
            for tick in ticks
        ]

        return params_list


@dataclasses.dataclass
class SheffieldSolarMetadataRequest(SheffieldSolarRequest):
    """Parameters for the Sheffield Solar API metadata request."""

    def endpoint(self) -> str:
        """Return the API endpoint for the request."""
        return "rawdata/api/v4/owner_system_params_rounded"

    def as_query_params(self, user_id: str, api_key: str) -> list[dict[str, str]]:
        """Return the request as a dictionary of parameters."""
        return [{
            "user_id": user_id,
            "key": api_key,
        }]


class SheffieldSolarAPIResource(dg.ConfigurableResource):
    """Dagster resource for accessing the Sheffield Solar API."""

    user_id: str
    api_key: str
    base_url: str = "https://api.pvlive.uk"
    n_processes: int = 1

    def setup_for_execution(self, _: dg.InitResourceContext) -> None:
        """Set up the Sheffield Solar API resource for execution."""
        self._log = dg.get_dagster_logger()

    @staticmethod
    def _query(
        url: str,
        params: dict[str, str],
    ) -> pd.DataFrame:
        """Query the Sheffield Solar API.

        Args:
            url: The URL to query.
            params: The query parameters.
            retries: The number of times to retry the query.
            backoff_factor: Multiplier to increase delay by on each retry.
        """
        full_url: str = url + "?" + "&".join([f"{k}={v}" for k, v in params.items()])
        base_err: str = "Error querying Sheffield Solar API"
        session: requests.Session = requests.Session()
        session.mount(
            prefix="https://",
            adapter=HTTPAdapter(max_retries=Retry(
                total=10,
                backoff_factor=0.1,
                backoff_jitter=0.1,
                status_forcelist=[500, 502, 503, 504],
            )),
        )

        try:
            response = session.get(url=full_url, timeout=60*60)
        except requests.exceptions.HTTPError as e:
            raise ValueError(f"{base_err}: {e}") from e

        if response.status_code != 200:
            raise ValueError(f"HTTP error: {response.status_code}")
        else:
            if "Your api key is not valid" in response.text:
                raise ValueError(f"{base_err}: invalid API key/User ID combination")
            elif "Your account does not give access" in response.text:
                raise ValueError(
                    f"{base_err}: API key/User ID does not give access to requested data",
                )
            elif "Missing user id" in response.text:
                raise ValueError(f"{base_err}: missing user_id")
            elif "Missing api key" in response.text:
                raise ValueError(f"{base_err}: missing api_key")
            else:
                try:
                    df: pd.DataFrame = pd.read_csv(
                        io.StringIO(response.text),
                        parse_dates=True,
                    )
                    return df
                except Exception as e:
                    raise ValueError(f"{base_err}: error parsing API query result: {e}") from e

    def request(
        self,
        request: SheffieldSolarRequest,
    ) -> pd.DataFrame:
        """Request data from the Sheffield Solar API."""
        pool = multiprocessing.Pool(processes=self.n_processes)
        url: str = f"{self.base_url}/{request.endpoint()}"
        self._log.debug(f"Querying Sheffield Solar API at '{url}'")

        df_chunks: pd.DataFrame = pool.map(
            functools.partial(self._query, url),
            request.as_query_params(self.user_id, self.api_key),
        )
        out_df: pd.DataFrame = pd.concat(df_chunks)
        self._log.debug(f"Pulled {len(out_df)} rows from Sheffield Solar API")
        return out_df

