import datetime as dt
import os

import dagster as dg

from constants import LOCATIONS_BY_ENVIRONMENT
from containers.gfs import download_combine_gfs

env = os.getenv("ENVIRONMENT", "local")
ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].NWP_ZARR_FOLDER

@dg.asset(
    name="zarr_daily_archive",
    description="Daily archive of GFS global NWP data",
    key_prefix=["nwp", "gfs", "global"],
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=dg.DailyPartitionsDefinition(
        start_date="2015-01-15",
        end_offset=-2,
    ),
    metadata={
        "archive_folder": dg.MetadataValue.text(f"{ZARR_FOLDER}/nwp/gfs/global"),
        "area": dg.MetadataValue.text("global"),
        "source": dg.MetadataValue.text("gfs"),
    },
)
def zarr_archive(
    context: dg.AssetExecutionContext,
) -> dg.Output:
    start: dt.datetime.now(tz=dt.UTC)
    outfile: str = download_combine_gfs.run(
        path=ZARR_FOLDER + "/nwp/gfs/global",
        date=context.partition_time_window.start,
        config=download_combine_gfs.DEFAULT_CONFIG,
    )
    end: dt.datetime.now(tz=dt.UTC)
    return dg.Output(
        value=outfile,
        metadata={
            "archive_folder": dg.MetadataValue.text(f"{ZARR_FOLDER}/nwp/gfs/global"),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("gfs"),
            "partition_elapsed_time_minutes": dg.MetadataValue.int(
                (end - start).total_seconds() // 60,
            ),
        },
    )
