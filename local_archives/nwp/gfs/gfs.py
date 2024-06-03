import os

import dagster as dg
from dagster_docker import PipesDockerClient

from constants import LOCATIONS_BY_ENVIRONMENT

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
    pipes_docker_client: PipesDockerClient,
) -> dg.MaterializeResult:
    return pipes_docker_client.run(
        context=context,
        image="ghcr.io/openclimatefix/gfs-etl:main",
        command=[
            "--date",
            context.partition_time_window.start.strftime("%Y-%m-%d"),
            "--path",
            "/data",
        ],
        container_kwargs={
            "volumes": {
                f"{ZARR_FOLDER}/nwp/gfs/global": {"bind": "/data", "mode": "rw"},
            },
        },
    ).get_materialize_result()
