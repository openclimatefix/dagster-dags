"""Zarr archive of NWP data from ECMWF's ENS, covering the UK.

ENS (sometimes EPS) is the ECMWF Ensemble Prediction System,
which provides 50 perturbed forecasts of upcoming atmospheric conditions.
This asset contains full accounts of this data for the UK.

Sourced via MARS API from ECMWF (https://apps.ecmwf.int/mars-catalogue).
This asset is updated monthly, and surfaced as a Zarr Directory Store for each month.
It is downloaded using the nwp-consumer docker image
(https://github.com/openclimatefix/nwp-consumer).
"""

import os
from typing import TYPE_CHECKING

import dagster as dg
from dagster_docker import PipesDockerClient

if TYPE_CHECKING:
    import datetime as dt

ARCHIVE_FOLDER = "/var/dagster-storage/nwp/ecmwf-ens-uk"
if os.getenv("ENVIRONMENT", "local") == "leo":
    ARCHIVE_FOLDER = "/mnt/storage_ssd_4tb/nwp/ecmwf-ens-uk"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.MonthlyPartitionsDefinition(
    start_date="2022-01-01",
    end_offset=-3,
)

@dg.asset(
        name="ecmwf-ens-uk",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("uk"),
            "source": dg.MetadataValue.text("ecmwf-mars"),
            "model": dg.MetadataValue.text("ens"),
            "expected_runtime": dg.MetadataValue.text("6 hours"),
        },
        compute_kind="docker",
        automation_condition=dg.AutomationCondition.on_cron(
            cron_schedule=partitions_def.get_cron_schedule(
                hour_of_day=6,
                day_of_week=1,
            ),
        ),
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "nwp-consumer",
        },
    partitions_def=partitions_def,
)
def ecmwf_ens_uk_asset(
    context: dg.AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
    ) -> dg.MaterializeResult:
    """Dagster asset downloading ECMWF ENS data for the UK."""
    it: dt.datetime = context.partition_time_window.start
    return pipes_docker_client.run(
        image="ghcr.io/openclimatefix/nwp-consumer:1.0.19",
        command=["archive", "-y", str(it.year), "-m", str(it.month)],
        env={
            "MODEL_REPOSITORY": "ecmwf-mars",
            "MODEL": "ens-uk",
            "NOTIFICATION_REPOSITORY": "dagster-pipes",
            "ECMWF_API_KEY": os.environ["ECMWF_API_KEY"],
            "ECMWF_API_EMAIL": os.environ["ECMWF_API_EMAIL"],
            "ECMWF_API_URL": os.environ["ECMWF_API_URL"],
            "CONCURRENCY": "true",
        },
        # See https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.ContainerCollection.run
        container_kwargs={
            "volumes": [f"{ARCHIVE_FOLDER}:/work"],
            "mem_limit": "8g",
            "nano_cpus": int(4e9),
        },
        context=context,
    ).get_materialize_result()

