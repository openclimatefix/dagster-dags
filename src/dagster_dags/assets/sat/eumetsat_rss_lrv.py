"""Zarr archive of satellite image data from EUMETSAT's RSS service, low resolution.

EUMETSAT have a seviri satellite that provides images of the earth's surface.
The Rapid Scan Service (RSS) provides images at 5 minute intervals,
whilst other Severi images are at 15 minute intervals.
The images are in the MSG format, which is a compressed format that contains
multiple channels of data. The come in high resolution (HRV) and low resolution (LRV).

Sourced via eumdac from DataStore (https://navigator.eumetsat.int/product/EO:EUM:DAT:MSG:RSS).
This asset is updated monthly, and surfaced as a Zarr Directory Store for each month.
It is downloaded using the satellite-consumer.
"""

import os
from typing import TYPE_CHECKING

import dagster as dg
from dagster_docker import PipesDockerClient

if TYPE_CHECKING:
    import datetime as dt

ARCHIVE_FOLDER = "/var/dagster-storage/sat/eumetsat-rss-lrv"
if os.getenv("ENVIRONMENT", "local") == "leo":
    ARCHIVE_FOLDER = "/mnt/storage_b/archives/sat/eumetsat-rss-lrv"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.MonthlyPartitionsDefinition(
    start_date="2025-01-01",
)

@dg.asset(
        name="eumetsat-rss-lrv",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("europe"),
            "source": dg.MetadataValue.text("eumetsat"),
            "expected_runtime": dg.MetadataValue.text("6 hours"),
        },
        compute_kind="docker",
        automation_condition=dg.AutomationCondition.on_cron(
            cron_schedule=partitions_def.get_cron_schedule(
                hour_of_day=0,
                day_of_week=5,
            ),
        ),
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "eumetsat",
        },
    partitions_def=partitions_def,
)
def eumetsat_rss_lrv_asset(
    context: dg.AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
) -> dg.MaterializeResult:
    """Dagster asset for EUMETSAT's RSS service, low resolution."""
    it: dt.datetime = context.partition_time_window.start

    return pipes_docker_client.run(
        image="ghcr.io/openclimatefix/satellite-consumer:0.2.0",
        command=[],
        env={
            "EUMETSAT_CONSUMER_KEY": os.getenv("EUMETSAT_CONSUMER_KEY"),
            "EUMETSAT_CONSUMER_SECRET": os.getenv("EUMETSAT_CONSUMER_SECRET"),
            "SATCONS_COMMAND": "consume",
            "SATCONS_WINDOW_MONTHS": "1",
            "SATCONS_SATELLITE": "rss",
            "SATCONS_VALIDATE": "true",
            "SATCONS_RESCALE": "true",
            "SATCONS_TIME": it.strftime("%Y%m%dT%H%M"),
            "SATCONS_NUM_WORKERS": "4",
        },
        container_kwargs={
            "volumes": [f"{ARCHIVE_FOLDER}:/work"],
        },
        context=context,
    ).get_materialize_result()

