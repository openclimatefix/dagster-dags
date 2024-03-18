import datetime as dt
import os
import pathlib

import dagster as dg
import meteomatics.api as mmapi
import pandas as pd
import xarray as xr
import zarr
from ocf_blosc2 import Blosc2

from constants import LOCATIONS_BY_ENVIRONMENT

env = os.getenv("ENVIRONMENT", "local")
BASE_PATH = LOCATIONS_BY_ENVIRONMENT[env].NWP_ZARR_FOLDER

username = os.environ.get("METEOMATICS_USERNAME")
password = os.environ.get("METEOMATICS_PASSWORD")

wind_coordinates = [
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

solar_coordinates = [
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
    "cape:Jkg",
]

solar_parameters = [
    "direct_rad:W",
    "diffuse_rad:W",
    "gloabl_rad:W",
]

startdate = dt.datetime(2023, 3, 14, tzinfo=dt.UTC)
enddate = dt.datetime(2023, 3, 17, tzinfo=dt.UTC)
interval = dt.timedelta(minutes=15)
model = "ecmwf-ifs"

# ==== Resources ====

# These should probably be specified out as a dagster resource in a seperate file
# Then called more generically in a configurable op
# But in the interest of getting it done before I go on holiday,
# here is the most basic implementation


@dg.op
def query_meteomatics_wind_api(context: dg.OpExecutionContext) -> pd.DataFrame:
    """Job to run a meteomatics download."""
    start_cutoff = dt.datetime(2019, 3, 15, tzinfo=dt.UTC)

    start: dt.datetime = context.partition_time_window.start
    end: dt.datetime = context.partition_time_window.end

    df = mmapi.query_time_series(
        wind_coordinates,
        start if start > start_cutoff else start_cutoff,
        end,
        interval,
        wind_parameters,
        username,
        password,
        model=model,
    ).reset_index(level=["lat", "lon"])

    return df


def query_meteomatics_solar_api(context: dg.OpExecutionContext) -> pd.DataFrame:
    """Job to run a meteomatics download."""
    start_cutoff = dt.datetime(2019, 3, 15, tzinfo=dt.UTC)

    start: dt.datetime = context.partition_time_window.start
    end: dt.datetime = context.partition_time_window.end

    df = mmapi.query_time_series(
        solar_coordinates,
        start if start > start_cutoff else start_cutoff,
        end,
        interval,
        solar_parameters,
        username,
        password,
        model=model,
    )

    return df


# ==== Ops ====


@dg.op
def map_df_ds(df: pd.DataFrame) -> xr.Dataset:
    df = df.reset_index(level=["lat", "lon"])
    ds = xr.Dataset.from_dataframe(df).set_coords(("lat", "lon"))
    return ds


@dg.op
def store_ds(context: dg.OpExecutionContext, ds: xr.Dataset) -> pathlib.Path:
    encoding = {}
    for var in ds.data_vars:
        encoding[var] = {"compressor": Blosc2(cname="zstd", clevel=5)}

    pdt: dt.datetime = dt.datetime.strptime(context.partition_key, "%Y-%m-%d|%H:%M").replace(
        tzinfo=dt.UTC,
    )
    path = pathlib.Path(
        f"{BASE_PATH}/{'/'.join(context.asset_key.path[:-1])}/{context.asset_key.path[-1]}_{pdt.strftime('%Y')}.zarr.zip",
    )
    with zarr.ZipStore(path.as_posix(), mode="w") as store:
        ds.to_zarr(store, encoding=encoding, mode="w")

    return path


@dg.asset(
    key=["nwp", "meteomatics", "nw_india", "wind_archive"],
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m-%d|%H:%M",
        start="2019-01-01|00:00",
        cron_schedule="0 0 1 1 *",  # Once a year
    ),
    metadata={
        "path": dg.MetadataValue.path(f"{BASE_PATH}/nwp/meteomatics/nw_india/wind_archive"),
    },
)
def meteomatics_wind_archive() -> dg.Output[str]:
    """Meteomatics wind archive asset."""
    df = query_meteomatics_wind_api()
    ds = map_df_ds(df)
    path = store_ds(ds)
    return dg.Output(
        path,
        metadata={
            "ds": dg.MetadataValue.md(str(ds)),
        },
    )


@dg.asset(
    key=["nwp", "meteomatics", "nw_india", "solar_archive"],
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m-%d|%H:%M",
        start="2019-01-01|00:00",
        cron_schedule="0 0 1 1 *",  # Once a year
    ),
    metadata={
        "path": dg.MetadataValue.path(f"{BASE_PATH}/nwp/meteomatics/nw_india/solar_archive"),
    },
)
def meteomatics_solar_archive() -> dg.Output[pathlib.Path]:
    """Meteomatics solar archive asset."""
    df = query_meteomatics_solar_api()
    ds = map_df_ds(df)
    path = store_ds(ds)
    return dg.Output(
        path,
        metadata={
            "ds": dg.MetadataValue.md(str(ds)),
        },
    )
