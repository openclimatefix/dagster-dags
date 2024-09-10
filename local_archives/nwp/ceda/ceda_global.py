import dagster as dg
import os
from typing import Any
import datetime as dt

from dagster_docker import PipesDockerClient
from constants import LOCATIONS_BY_ENVIRONMENT

env = os.getenv("ENVIRONMENT", "local")
ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].NWP_ZARR_FOLDER

@dg.asset(
        name="zarr_archive",
        key_prefix=["nwp", "ceda", "global"],
        metadata={
            "archive_folder": dg.MetadataValue.text(f"{ZARR_FOLDER}/nwp/ceda/global"),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("ceda"),
        },
        compute_kind="download",
        op_tags={"dagster/max_runtime": int(60 * 100)},
    partitions_def=dg.MonthlyPartitionsDefinition(
        start_date="2019-01-01",
        end_offset=-3,
    ),
)
def ceda_global(
    context: dg.AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
) -> Any:
    image: str = "ghcr.io/openclimatefix/nwp-consumer:devsjc-major-refactor"
    it: dt.datetime = context.partition_time_window.start
    return pipes_docker_client.run(
        image=image,
        command=[
            "consume",
            "-y",
            str(it.year),
            "-m",
            str(it.month),
        ],
        env={
            "NWP_CONSUMER_MODEL_REPOSITORY": "ceda-metoffice-global",
            "NWP_CONSUMER_NOTIFICATION_REPOSITORY": "dagster-pipes",
            "CEDA_FTP_USER": os.environ["CEDA_FTP_USER"],
            "CEDA_FTP_PASSWORD": os.environ["CEDA_FTP_PASSWORD"],
        },
        container_kwargs={
            "volumes": [f"{ZARR_FOLDER}/nwp/ceda/global:/work"],
        },
        context=context,
    ).get_results()
