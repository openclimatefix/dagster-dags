import os
import shutil

import dagster as dg

from constants import LOCATIONS_BY_ENVIRONMENT
from containers.gfs import download_combine_gfs

env = os.getenv("ENVIRONMENT", "local")
ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].NWP_ZARR_FOLDER

@dg.asset(
    name="zarr_daily_archive",
    description="Daily archive of GFS global NWP data",
    key_prefix=["nwp", "gfs", "global"],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
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
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.MaterializeResult:
    return pipes_subprocess_client.run(
        context=context,
        command=(
            shutil.which("python"),
            dg.file_relative_path(download_combine_gfs.__file__, "download_combine_gfs.py"),
            "--date",
            context.partition_time_window.start.strftime("%Y-%m-%d"),
            "--path",
            ZARR_FOLDER + "/nwp/gfs/global",
        ),
    ).get_materialize_result()

