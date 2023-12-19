import dataclasses as dc
import datetime as dt
import os
import pathlib
import shutil

import dagster as dg
import numpy as np
import xarray as xr
from nwp_consumer.internal import IT_FOLDER_FMTSTR, FetcherInterface, FileInfoModel

from constants import LOCATIONS_BY_ENVIRONMENT


env = os.getenv("ENVIRONMENT", "local")
EPHEMERAL_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].EPHEMERAL_FOLDER

icon_partitions = dg.MultiPartitionsDefinition(
    {
        "date": dg.DailyPartitionsDefinition(start_date="2017-01-01"),
        "inittime": dg.StaticPartitionsDefinition(["00:00", "06:00", "12:00", "18:00"]),
    },
)

def map_partition_to_time(context: dg.AssetExecutionContext) -> dt.datetime:
    """Map a partition key to a datetime."""
    if type(context.partition_key) == dg.MultiPartitionKey:
        partkeys = context.partition_key.keys_by_dimension
        return dt.datetime.strptime(
            f"{partkeys['date']}|{partkeys['inittime']}",
            "%Y-%m-%d|%H:%M",
        ).replace(tzinfo=dt.UTC)
    else:
        raise ValueError(
            f"Partition key must be of type MultiPartitionKey, not {type(context.partition_key)}",
        )

@dc.dataclass
class MakeDefinitionsOptions:
    """Typesafe options for the make_asset_definitions function."""

    area: str
    fetcher: FetcherInterface
    partitions: dg.PartitionsDefinition = ecmwf_partitions

    def key_prefix(self) -> list[str]:
        """Generate an asset key prefix based on the area.

        The prefix is important as it defines the folder structure under which
        assets are stored.
        """
        return ["nwp", "icon", self.area]

@dc.dataclass
class MakeDefintitionsOutputs:
    """Typesafe outputs for the make_asset_definitions function."""

    source_asset: dg.AssetsDefinition
    raw_asset: dg.AssetsDefinition
    zarr_asset: dg.AssetsDefinition

def make_definitions(
    opts: MakeDefintionsOptions,
) -> 
