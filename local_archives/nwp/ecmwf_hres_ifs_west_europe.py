"""Zarr archive of NWP data from ECMWF's IFS model, covering Western Europe.

IFS is the Integrated Forecasting System, which uses a global numerical model
of earth to produce deterministic forecasts of upcoming atmospheric conditions.

Sourced via MARS API from ECMWF (https://apps.ecmwf.int/mars-catalogue).
This asset is updated monthly, and surfaced as a Zarr Directory Store for each month.
It is downloaded using the nwp-consumer docker image
(https://github.com/openclimatefix/nwp-consumer).
"""

import datetime as dt
import os
from typing import Any

import dagster as dg
from dagster_docker import PipesDockerClient

ARCHIVE_FOLDER = "/var/dagster-storage/nwp/ecmwf-hres-ifs-west-europe"
if os.getenv("ENVIRONMENT", "local") == "leo":
    ARCHIVE_FOLDER = "/mnt/storage_b/nwp/ecmwf-hres-ifs-west-europe"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.MonthlyPartitionsDefinition(
    start_date="2017-01-01",
    end_offset=-1,
)

@dg.asset(
        name="ecmwf-hres-ifs-west-europe",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("west-europe"),
            "source": dg.MetadataValue.text("ecmwf-mars"),
            "model": dg.MetadataValue.text("hres-ifs"),
            "expected_runtime": dg.MetadataValue.text("6 hours"),
        },
        compute_kind="docker",
        automation_condition=dg.AutomationCondition.on_cron(
            cron_schedule=partitions_def.get_cron_schedule(
                hour_of_day=18,
                day_of_week=0,
            ),
        ),
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "nwp-consumer",
        },
    partitions_def=partitions_def,
)
def ecmwf_hres_ifs_west_europe_asset(
    context: dg.AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
) -> Any:
    it: dt.datetime = context.partition_time_window.start
    return pipes_docker_client.run(
        image="ghcr.io/openclimatefix/nwp-consumer:1.0.12",
        command=["archive", "-y", str(it.year), "-m", str(it.month)],
        env={
            "MODEL_REPOSITORY": "ecmwf-mars",
            "MODEL": "hres-ifs-west-europe",
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

