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
        energy_type="wind",
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
        energy_type="solar",
    )

@dg.op
def map_df_ds(df: pd.DataFrame) -> xr.Dataset:
    """Map DataFrame to xarray Dataset."""
    df = df.reset_index(level=["lat", "lon"])
    ds = xr.Dataset.from_dataframe(df).set_coords(("lat", "lon"))
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
