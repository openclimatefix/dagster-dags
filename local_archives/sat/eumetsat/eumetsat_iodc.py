import datetime as dt
import os
from typing import Any

import dagster as dg
from dagster_docker import PipesDockerClient

from constants import LOCATIONS_BY_ENVIRONMENT

env = os.getenv("ENVIRONMENT", "local")
ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].SAT_ZARR_FOLDER

@dg.asset(
        name="zarr_archive",
        description="".join((
            "Zarr archive of satellite data from EUMETSAT's IODC satellite.",
            "Sourced via EUMDAC from EUMETSAT ",
            "(https://navigator.eumetsat.int/product/EO:EUM:DAT:MSG:OCA-IODC). ",
            "This asset is updated monthly, and surfaced as a Zarr Directory Store ",
            "for each month. It is downloaded using the sat container ",
            "(https://github.com/openclimatefix/dagster-dags/pkgs/container/sat-etl).",
        )),
        key_prefix=["sat", "eumetsat", "iodc"],
        metadata={
            "archive_folder": dg.MetadataValue.text(f"{ZARR_FOLDER}/sat/eumetsat/india"),
            "area": dg.MetadataValue.text("india"),
            "source": dg.MetadataValue.text("eumetsat"),
            "expected_runtime": dg.MetadataValue.text("TBD"),
        },
        compute_kind="docker",
        automation_condition=dg.AutomationCondition.eager(),
        tags={
            # "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "eumetsat",
        },
    partitions_def=dg.MonthlyPartitionsDefinition(
        start_date="2019-01-01",
        end_offset=-3,
    ),
)
def iodc_monthly(
    context: dg.AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
) -> Any:
    image: str = "ghcr.io/openclimatefix/sat-etl:main"
    it: dt.datetime = context.partition_time_window.start
    return pipes_docker_client.run(
        image=image,
        command=[
            "iodc",
            "-m",
            it.strftime("%Y-%m"),
            "--path",
            f"{ZARR_FOLDER}/sat/eumetsat/india",
            "--rm",
        ],
        env={
            "EUMETSAT_CONSUMER_KEY": os.environ["EUMETSAT_CONSUMER_KEY"],
            "EUMETSAT_CONSUMER_SECRET": os.environ["EUMETSAT_CONSUMER_SECRET"],
        },
        container_kwargs={
            "volumes": [f"{ZARR_FOLDER}/sat/eumetsat/india:{ZARR_FOLDER}/sat/eumetsat/india"],
        },
        context=context,
    ).get_results()

