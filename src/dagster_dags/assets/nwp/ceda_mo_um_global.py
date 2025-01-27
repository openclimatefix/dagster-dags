"""Zarr archive of NWP data from the Met Office's Unified Model in the global configuration.

The MetOffice runs it's Unified Model (UM) in two configurations: Global, and UK.
This asset contains data from the global configuration covering the whole globe.

Sourced via FTP from CEDA (https://catalogue.ceda.ac.uk/uuid/86df725b793b4b4cb0ca0646686bd783).
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


ARCHIVE_FOLDER = "/var/dagster-storage/nwp/ceda-mo-um-global"
if os.getenv("ENVIRONMENT", "local") == "leo":
    ARCHIVE_FOLDER = "/mnt/storage_ssd_4tb/nwp/ceda-mo-um-global"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.MonthlyPartitionsDefinition(
    start_date="2019-01-01",
    end_offset=-3,
)

@dg.asset(
        name="ceda-mo-um-global",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("ceda"),
            "model": dg.MetadataValue.text("mo-um"),
            "expected_runtime": dg.MetadataValue.text("6 hours"),
        },
        compute_kind="docker",
        automation_condition=dg.AutomationCondition.on_cron(
            cron_schedule=partitions_def.get_cron_schedule(
                hour_of_day=5,
            ),
        ),
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "nwp-consumer",
        },
)
def ceda_mo_um_global_asset(
    context: dg.AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
    ) -> dg.MaterializeResult:
    """Dagster asset for MO Unified Model global NWP data from CEDA."""
    it: dt.datetime = context.partition_time_window.start
    return pipes_docker_client.run(
        image="ghcr.io/openclimatefix/nwp-consumer:1.0.12",
        command=["archive", "-y", str(it.year), "-m", str(it.month)],
        env={
            "NWP_CONSUMER_MODEL_REPOSITORY": "ceda-metoffice-global",
            "NWP_CONSUMER_NOTIFICATION_REPOSITORY": "dagster-pipes",
            "CEDA_FTP_USER": os.environ["CEDA_FTP_USER"],
            "CEDA_FTP_PASS": os.environ["CEDA_FTP_PASS"],
            "CONCURRENCY": "true",
        },
        # See https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.ContainerCollection.run
        container_kwargs={
            "volumes": [f"{ARCHIVE_FOLDER}:/work"],
            "mem_limit": "8g",
            "nano_cpus": 4e9,
        },
        context=context,
    ).get_materialize_result()

