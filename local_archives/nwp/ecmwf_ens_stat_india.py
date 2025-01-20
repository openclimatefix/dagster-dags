"""Zarr archive of Summary NWP data from ECMWF's ENS, covering India.

ENS (sometimes EPS) is the ECMWF Ensemble Prediction System,
which provides 50 perturbed forecasts of upcoming atmospheric conditions.
This asset contains summary statistics of this data (mean, standard deviation) for India.

Sourced via MARS API from ECMWF (https://apps.ecmwf.int/mars-catalogue).
This asset is updated monthly, and surfaced as a Zarr Directory Store for each month.
It is downloaded using the nwp-consumer docker image
(https://github.com/openclimatefix/nwp-consumer).
"""

import os
from typing import TYPE_CHECKING, Any

import dagster as dg
from dagster_docker import PipesDockerClient

if TYPE_CHECKING:
    import datetime as dt

ARCHIVE_FOLDER = "/var/dagster-storage/nwp/ecmwf-ens-stat-india"
if os.getenv("ENVIRONMENT", "local") == "leo":
    ARCHIVE_FOLDER = "/mnt/storage_b/nwp/ecmwf-ens-stat-india"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.MonthlyPartitionsDefinition(
    start_date="2020-01-01",
    end_offset=-3,
)

@dg.asset(
        name="ecmwf-ens-stat-india",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("india"),
            "source": dg.MetadataValue.text("ecmwf-mars"),
            "model": dg.MetadataValue.text("ens-stat"),
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
def ecmwf_ens_stat_india_asset(
    context: dg.AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
) -> Any:  # noqa: ANN401
    """Dagster asset downloading ECMWF ENS data for India."""
    it: dt.datetime = context.partition_time_window.start
    return pipes_docker_client.run(
        image="ghcr.io/openclimatefix/nwp-consumer:1.0.12",
        command=["archive", "-y", str(it.year), "-m", str(it.month)],
        env={
            "MODEL_REPOSITORY": "ecmwf-mars",
            "MODEL": "ens-stat-india",
            "NOTIFICATION_REPOSITORY": "dagster-pipes",
            "ECMWF_API_KEY": os.environ["ECMWF_API_KEY"],
            "ECMWF_API_EMAIL": os.environ["ECMWF_API_EMAIL"],
            "ECMWF_API_URL": os.environ["ECMWF_API_URL"],
            "CONCURRENCY": "false",
        },
        container_kwargs={
            "volumes": [f"{ARCHIVE_FOLDER}:/work"],
        },
        context=context,
    ).get_results()

