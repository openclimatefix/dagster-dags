import dagster as dg
import dagster_docker as dgd

from constants import LOCATIONS_BY_ENVIRONMENT

ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].NWP_ZARR_FOLDER

@dg.asset(
    name="zarr_daily_archive",
    key_prefix=["nwp", "gfs", "global"],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    partitions_def=dg.DailyPartitionsDefinition(
        start_date="2015-01-15",
        offset=-2,
    ),
    metadata={
        "archive_folder": dg.MetadataValue.text(f"{ZARR_FOLDER}/nwp/gfs/global"),
        "area": dg.MetadataValue.text("global"),
        "source": dg.MetadataValue.text("gfs"),
    },
)
def zarr_archive(
    context: dg.AssetExecutionContext,
    pipes_client: dgd.PipesDockerClient,
):
    return pipes_client.run(
        context=context,
        image="ghcr.io/openclimatefix/gfs-etl:main",
        command=[
            "--date",
            context.partition_time_window.start.strftime("%Y-%m-%d"),
            "--path",
            f"{ZARR_FOLDER}/nwp/gfs/global",
        ],
    )
