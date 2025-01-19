"""Download PV generation data via Sheffield Solar's 'rawdata' API.

Copied from
https://github.com/SheffieldSolar/SS-RawData-API/blob/main/ss_rawdata_api/ss_rawdata_api.py
"""

import datetime as dt
import logging
from copy import copy
from functools import cached_property
from io import StringIO
from itertools import starmap
from multiprocessing import Pool
from time import sleep
from typing import TYPE_CHECKING, Any, Literal, TypedDict

import pandas as pd
import requests

if TYPE_CHECKING:
    from collections.abc import Collection


class ProxyDict(TypedDict):
    """ProxyDict type hint."""

    http: str
    https: str


class SSRawDataAPI:
    """Class to download PV generation data from the Sheffield Solar rawdata API."""

    base_url: str
    max_range: dt.timedelta
    proxies: ProxyDict | None
    params: dict[str, str]

    def __init__(self, user_id: int | str, api_key: str, proxies: ProxyDict | None = None) -> None:
        """Initialise the API object."""
        self.base_url = "https://api.pvlive.uk/rawdata/api/v4"
        # self.base_url = "https://staging.solar.shef.ac.uk/rawdata/api/v4"
        self.max_range = dt.timedelta(days=1)
        self.proxies = proxies
        self.params = {"user_id": str(user_id), "key": api_key}

    @cached_property
    def metadata(self) -> pd.DataFrame:
        """Get system metadata."""
        endpoint = "owner_system_params_rounded"
        metadata = _query_api(
            base_url=self.base_url, endpoint=endpoint, params=self.params, proxies=self.proxies,
        )
        return metadata

    def __download_loop(
        self,
        endpoint: str,
        start: dt.datetime,
        end: dt.datetime,
        period: Literal[5, 30],
        n_processes: int | None = 10,
    ) -> pd.DataFrame:
        """Loop through a list of parameters and query the API."""
        request_start = start
        inputs: list[Collection[str] | dict[str, str] | ProxyDict | None] = []
        while request_start <= end:
            request_end = min(
                end, request_start + self.max_range - dt.timedelta(minutes=period),
            )
            params = _compile_params(request_start, request_end, self.params)
            inputs.append([self.base_url, endpoint, params, self.proxies])  # type: ignore
            request_start += self.max_range
        if n_processes is not None and n_processes > 1:
            pool = Pool(n_processes)
            chunks = pool.starmap(_query_api, inputs)  # type: ignore
        else:
            chunks = starmap(_query_api, inputs)  # type: ignore
        return pd.concat(chunks)

    def __download_5min(
        self, start: dt.datetime, end: dt.datetime, n_processes: int | None = 10,
    ) -> pd.DataFrame:
        """Download 5 minutely data."""
        endpoint = "reading_integrated_5mins"
        data = self.__download_loop(endpoint, start, end, 5, n_processes)
        data.rename(columns={"timestamp": "datetime_GMT", "data": "generation_Wh"}, inplace=True)
        return data

    def __download_30min(
        self, start: dt.datetime, end: dt.datetime, n_processes: int | None = 10,
    ) -> pd.DataFrame:
        """Download 30 minutely data."""
        endpoint = "reading_integrated"
        start_date = dt.datetime.combine(start.date(), dt.time(0))
        end_date = dt.datetime.combine(
            (end - dt.timedelta(minutes=30)).date(), dt.time(0),
        )
        data = self.__download_loop(endpoint, start_date, end_date, 30, n_processes)
        data["date"] = pd.to_datetime(data.date, utc=True)
        data = data.melt(id_vars=["date", "ss_id"], var_name="sp_utc", value_name="generation_Wh")
        thirtymins = pd.Timedelta(minutes=30)
        data["datetime_GMT"] = data.date + data.sp_utc.str.strip("t").astype(int) * thirtymins
        data = data.loc[
            (data.datetime_GMT >= start) & (data.datetime_GMT <= end),
            ["ss_id", "datetime_GMT", "generation_Wh"],
        ]
        return data

    def download(
        self,
        start: dt.datetime,
        end: dt.datetime,
        period: Literal[5, 30],
        n_processes: int | None = 10,
    ) -> pd.DataFrame:
        """Download PV data from the SS rawdata API.

        Args:
            start: A timezone-aware datetime object. Will be corrected to the END of the half hour
                in which *start* falls, since Sheffield Solar use end of interval as convention.
            end: A timezone-aware datetime object. Will be corrected to the END of the half hour
                in which *end* falls, since Sheffield Solar use end of interval as convention.
            period: Time-resolution to retrieve, either 30 or 5 (minutely). Default is 30.
            n_processes: Number of API queries to make in parallel. Default is 10.

        Returns:
            Pandas DataFrame containing the columns ss_id, datetime_GMT, generation_Wh.
        """
        logging.info(
            "Downloading %s minutely PV data between %s and %s using %s threads",
            period,
            start,
            end,
            n_processes,
        )
        _validate_inputs(start, end, period)
        start = _nearest_interval(start, period=period)
        end = _nearest_interval(end, period=period)
        if period == 30:
            return self.__download_30min(start, end, n_processes)
        else:
            return self.__download_5min(start, end, n_processes)


def _validate_start_end(start: dt.datetime, end: dt.datetime) -> None:
    """Check start and end are tz-aware datetime.datetime."""
    type_check = not (isinstance(start, dt.datetime) and isinstance(end, dt.datetime))
    tz_check = start.tzinfo is None or end.tzinfo is None
    if type_check or tz_check:
        raise TypeError("start and end must be timezone-aware Python datetime objects.")
    if end < start:
        raise ValueError("end must be later than start.")


def _validate_inputs(start: dt.datetime, end: dt.datetime, period: Literal[5, 30]) -> None:
    """Validate common input parameters."""
    _validate_start_end(start, end)
    periods = ["5", "30"]
    if str(period) not in periods:
        raise ValueError("The period parameter must be one of: " f"{', '.join(map(str, periods))}.")


def _nearest_interval(t: dt.datetime, period: Literal[5,30]=30) -> dt.datetime:
    """Round to either the nearest 30 or 5 minute interval."""
    t_ = copy(t)
    if not (t.minute % period == 0 and t.second == 0 and t.microsecond == 0):
        offset = dt.timedelta(
            minutes=t.minute % period, seconds=t.second, microseconds=t.microsecond,
        )
        t_ = t - offset + dt.timedelta(minutes=period)
        logging.debug("Timestamp %s corrected to nearest %s mins: %s", t, period, t_)
    return t_


def _compile_params(
        start: dt.datetime | None = None,
        end: dt.datetime | None = None,
        additional_params: dict[str, str] | None = None,
    ) -> dict[str, str]:
    """Compile parameters into a Python dict, formatting where necessary."""
    params: dict[str, str] = {}
    if start is not None:
        params["start_at"] = _iso8601_ss(start)
    end = start if (start is not None and end is None) else end
    if end is not None:
        params["end_at"] = _iso8601_ss(end)
    if additional_params is not None:
        params.update(additional_params)
    return params


def _iso8601_ss(t: dt.datetime) -> str:
    """Convert TZ-aware datetime to string representation expected by the API."""
    return t.isoformat().replace("+00:00", "")


def _iso8601_fn(t: dt.datetime) -> str:
    """Convert TZ-aware datetime to string representation for use in filenames."""
    return t.strftime("%Y%m%dT%H%M%S")


def _query_api(
        base_url: str,
        endpoint: str,
        params: dict[str, str],
        proxies: ProxyDict | None = None,
    ) -> pd.DataFrame:
    """Query the API with some REST parameters."""
    url = _build_url(base_url, endpoint, params)
    return _fetch_url(url, proxies)


def _build_url(base_url: str, endpoint: str, params: dict[str, Any]) -> str:
    """Construct the appropriate URL for a given set of parameters."""
    url = f"{base_url}/{endpoint}"
    url += "?" + "&".join([f"{k}={params[k]}" for k in params])
    return url


def _fetch_url(url: str, proxies: ProxyDict | None = None) -> pd.DataFrame:
    """Fetch the URL with GET request."""
    logging.debug("Fetching %s", url)
    logging.debug("Proxies: %s", proxies)
    success = False
    try_counter = 0
    delay = 0.5
    retries = 5
    while not success and try_counter < retries + 1:
        try_counter += 1
        try:
            page = requests.get(url=url, proxies=proxies.__dict__, timeout=60)
            page.raise_for_status()
            if page.status_code == 200 and "Your api key is not valid" in page.text:
                logging.debug(page.text)
                raise Exception("The user_id and/or api_key entered are invalid.")
            if page.status_code == 200 and "Your account does not give access" in page.text:
                logging.debug(page.text)
                raise Exception(
                    "The user_id and api_key does not give access to the data "
                    "you've requested, contact Sheffield Solar "
                    "<solar@sheffield.ac.uk>.",
                )
            if page.status_code == 200 and "Missing user_id" in page.text:
                logging.debug(page.text)
                raise Exception(
                    "The user_id and api_key does not give access to the data "
                    "you've requested, contact Sheffield Solar "
                    "<solar@sheffield.ac.uk>.",
                )
            success = True
        except requests.exceptions.HTTPError:
            sleep(delay)
            delay *= 2
            continue
    if not success:
        raise Exception("Error communicating with the Sheffield Solar API.")
    try:
        return pd.read_csv(StringIO(page.text), parse_dates=True)
    except Exception as e:
        raise Exception("Error communicating with the Sheffield Solar API") from e
