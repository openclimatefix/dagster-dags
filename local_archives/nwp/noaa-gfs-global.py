"""Zarr archive of NWP data from NCEP's GFS model.

The National Centers for Environmental Prediction (NCEP) runs the
deterministic Global Forecast System (GFS) model
(https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast).

Sourced via S3 from NOAA (https://noaa-gfs-bdp-pds.s3.amazonaws.com/index.html).
This asset is updated monthly, and surfaced as a Zarr Directory Store for each month.
It is downloaded using the nwp-consumer docker image
(https://github.com/openclimatefix/nwp-consumer).
"""

import datetime as dt
import os
from typing import Any

import dagster as dg
from dagster_docker import PipesDockerClient

ARCHIVE_FOLDER = "/var/dagster-storage/nwp/ncep-gfs-global"
if os.getenv("ENVIRONMENT", "local") == "leo":
    ARCHIVE_FOLDER = "/mnt/storage_b/nwp/ncep-gfs-global"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.MonthlyPartitionsDefinition(
    start_date="2021-01-01",
    end_offset=-1,
)

@dg.asset(
        name="ncep-gfs-global",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("noaa-s3"),
            "model": dg.MetadataValue.text("ncep-gfs"),
            "expected_runtime": dg.MetadataValue.text("6 hours"),
        },
        compute_kind="docker",
        automation_condition=dg.AutomationCondition.on_cron(
            cron_schedule=partitions_def.get_cron_schedule(
                hour_of_day=21,
                day_of_week=1,
            ),
        ),
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "nwp-consumer",
        },
)
def ncep_gfs_global_asset(
    context: dg.AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
) -> Any:
    it: dt.datetime = context.partition_time_window.start
    return pipes_docker_client.run(
        image="ghcr.io/openclimatefix/nwp-consumer:1.0.12",
        command=["archive", "-y", str(it.year), "-m", str(it.month)],
        env={
            "MODEL_REPOSITORY": "gfs",
            "NOTIFICATION_REPOSITORY": "dagster-pipes",
            "CONCURRENCY": "false",
        },
        container_kwargs={
            "volumes": [f"{ARCHIVE_FOLDER}:/work"],
        },
        context=context,
    ).get_results()

