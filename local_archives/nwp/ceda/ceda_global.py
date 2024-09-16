import datetime as dt
import os
from typing import Any

import dagster as dg
from dagster_docker import PipesDockerClient

from constants import LOCATIONS_BY_ENVIRONMENT

env = os.getenv("ENVIRONMENT", "local")
ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].NWP_ZARR_FOLDER

@dg.asset(
        name="zarr_archive",
        description="".join((
            "Zarr archive of NWP data from the Met Office's Global model. ",
            "Sourced via FTP from CEDA: ",
            "https://catalogue.ceda.ac.uk/uuid/86df725b793b4b4cb0ca0646686bd783/\n",
            "This asset is updated monthly, and surfaced as a Zarr Directory Store ",
            "for each month. It is downloaded using the nwp-consumer: ",
            "https://github.com/openclimatefix/nwp-consumer",
        )),
        key_prefix=["nwp", "ceda", "global"],
        metadata={
            "archive_folder": dg.MetadataValue.text(f"{ZARR_FOLDER}/nwp/ceda/global"),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("ceda"),
            "expected_runtime": dg.MetadataValue.text("6 hours"),
        },
        compute_kind="docker",
        automation_condition=dg.AutomationCondition.eager(),
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "ceda-ftp-consumer",
        },
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
            "archive",
            "-y",
            str(it.year),
            "-m",
            str(it.month),
        ],
        env={
            "NWP_CONSUMER_MODEL_REPOSITORY": "ceda-metoffice-global",
            "NWP_CONSUMER_NOTIFICATION_REPOSITORY": "dagster-pipes",
            "CEDA_FTP_USER": os.environ["CEDA_FTP_USER"],
            "CEDA_FTP_PASS": os.environ["CEDA_FTP_PASS"],
        },
        container_kwargs={
            "volumes": [f"{ZARR_FOLDER}/nwp/ceda/global:/work"],
        },
        context=context,
    ).get_results()
