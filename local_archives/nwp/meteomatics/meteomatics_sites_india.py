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
from resources import MeteomaticsAPIResource

env = os.getenv("ENVIRONMENT", "local")
BASE_PATH = LOCATIONS_BY_ENVIRONMENT[env].NWP_ZARR_FOLDER

# ==== Constants ====

# The order of these coordinate lists are used to determine the station_id
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
    # Adani
    (13.995, 78.428),
    (26.483, 71.232),
    (14.225, 77.43),
    (24.12, 69.34),
]

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
    # Adani
    (26.479, 1.220),
    (23.098, 75.255),
    (23.254, 69.252),
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

# ==== Ops ====

@dg.op
def query_meteomatics_wind_api(
    context: dg.OpExecutionContext,
    meteomatics_api: MeteomaticsAPIResource,
) -> pd.DataFrame:
    """Query Meteomatics API for wind data."""
    return meteomatics_api.query_api(
        start=context.partition_time_window.start,
        end=context.partition_time_window.end,
        coords=wind_coords,
        params=wind_parameters,
    )

@dg.op
def query_meteomatics_solar_api(
    context: dg.OpExecutionContext,
    meteomatics_api: MeteomaticsAPIResource,
) -> pd.DataFrame:
    """Query Meteomatics API for solar data."""
    return meteomatics_api.query_api(
        start=context.partition_time_window.start,
        end=context.partition_time_window.end,
        coords=solar_coords,
        params=solar_parameters,
    )

@dg.op
def map_df_ds(df: pd.DataFrame) -> xr.Dataset:
    """Map DataFrame to xarray Dataset."""
    # Reset index to create columns for lat, lon, and validdate
    df = df.reset_index(level=["lat", "lon", "validdate"])
    # Create a station_id column based on the coordinates
    df["station_id"] = df.groupby(["lat", "lon"], sort=False).ngroup() + 1
    # Create a time_utc column based on the validdate
    df["time_utc"] = pd.to_datetime(df["validdate"])
    # Make a new index based on station_id and time_utc
    df = df.set_index(["station_id", "time_utc"]).drop(columns=["validdate"])
    # Create xarray dataset from dataframe
    ds = xr.Dataset.from_dataframe(df).set_coords(("lat", "lon"))
    # Ensure time_utc is a timestamp object
    ds["time_utc"] = pd.to_datetime(ds["time_utc"])
    return ds


@dg.op
def store_ds(context: dg.OpExecutionContext, ds: xr.Dataset) -> dg.Output[pathlib.Path]:
    """Store xarray Dataset to Zarr."""
    encoding = {}
    for var in ds.data_vars:
        encoding[var] = {"compressor": Blosc2(cname="zstd", clevel=5)}

    pdt = context.partition_time_window.start
    path = pathlib.Path(
        f"{BASE_PATH}/{'/'.join(context.asset_key.path[:-1])}/{context.asset_key.path[-1]}_{pdt.strftime('%Y-%m')}.zarr.zip",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with zarr.ZipStore(path.as_posix(), mode="w") as store:
        ds.to_zarr(store, encoding=encoding, mode="w")

    return dg.Output(
        path,
        metadata={
            "dataset": dg.MetadataValue.text(ds.__str__()),
            "path": dg.MetadataValue.path(path.as_posix()),
            "partition_size:kb": dg.MetadataValue.int(int(path.stat().st_size / 1024)),
        },
    )


# ==== Assets ====

@dg.graph_asset(
    key=["nwp", "meteomatics", "nw_india", "wind_archive"],
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m",
        start="2019-03",
        cron_schedule="0 0 1 * *",  # Once a month
    ),
    metadata={
        "path": dg.MetadataValue.path(f"{BASE_PATH}/nwp/meteomatics/nw_india/wind_archive"),
    },
)
def meteomatics_wind_archive() -> dg.Output[str]:
    """Meteomatics wind archive asset."""
    df = query_meteomatics_wind_api()
    ds = map_df_ds(df)
    return store_ds(ds)


@dg.graph_asset(
    key=["nwp", "meteomatics", "nw_india", "solar_archive"],
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m",
        start="2019-03",
        cron_schedule="0 0 1 * *",  # Once a month
    ),
    metadata={
        "path": dg.MetadataValue.path(f"{BASE_PATH}/nwp/meteomatics/nw_india/solar_archive"),
    },
)
def meteomatics_solar_archive() -> dg.Output[pathlib.Path]:
    """Meteomatics solar archive asset."""
    df = query_meteomatics_solar_api()
    ds = map_df_ds(df)
    return store_ds(ds)
