import datetime as dt
import os
from typing import Any

import dagster as dg
from dagster_docker import PipesDockerClient

from constants import LOCATIONS_BY_ENVIRONMENT

env = os.getenv("ENVIRONMENT", "local")
ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].NWP_ZARR_FOLDER
ARCHIVE_FOLDER = f"{ZARR_FOLDER}/nwp/ecmwf-eps/india-stat"

@dg.asset(
        name="zarr_archive",
        description="".join((
            "Zarr archive of Summary NWP data from ECMWF's EPS. ",
            "Sourced via MARS API from ECMWF ",
            "(https://apps.ecmwf.int/mars-catalogue/). ",
            "This asset is updated monthly, and surfaced as a Zarr Directory Store ",
            "for each month. It is downloaded using the nwp-consumer ",
            "docker image (https://github.com/openclimatefix/nwp-consumer). ",
        )),
        key_prefix=["nwp", "ecmwf-eps", "india-stat"],
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("ecmwf-mars"),
            "expected_runtime": dg.MetadataValue.text("6 hours"),
        },
        compute_kind="docker",
        automation_condition=dg.AutomationCondition.eager(),
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "ecmwf-mars-consumer",
        },
    partitions_def=dg.MonthlyPartitionsDefinition(
        start_date="2020-01-01",
        end_offset=-3,
    ),
)
def ecmwf_eps_india_stat(
    context: dg.AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
) -> Any:
    image: str = "ghcr.io/openclimatefix/nwp-consumer:1.0.3"
    it: dt.datetime = context.partition_time_window.start
    return pipes_docker_client.run(
        image=image,
        command=[
            "archive"
            "-y",
            str(it.year),
            "-m",
            str(it.month),
        ],
        env={
            "MODEL_REPOSITORY": "ceda-metoffice-global",
            "NOTIFICATION_REPOSITORY": "dagster-pipes",
            "ECMWF_API_KEY": os.environ["ECMWF_API_KEY"],
            "ECMWF_API_EMAIL": os.environ["ECMWF_API_EMAIL"],
            "ECMWF_API_URL": os.environ["ECMWF_API_URL"],
            "ECMWF_MARS_AREA": "35/67/6/97",
        },
        container_kwargs={
            "volumes": [f"{ARCHIVE_FOLDER}:/work"],
        },
        context=context,
    ).get_results()
