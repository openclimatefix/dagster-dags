import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import dagster

from nwp.assets.cams import CAMSConfig, fetch_cams_eu_forecast_for_day, fetch_cams_forecast_for_day
from nwp.assets.dwd.common import IconConfig
from nwp.assets.ecmwf.mars import (
    NWPConsumerConfig,
    nwp_consumer_convert_op,
    nwp_consumer_download_op,
)

jobs: list[dagster.JobDefinition] = []
schedules: list[dagster.ScheduleDefinition] = []

# TODO: I would like to be able to use something like this at some point:
NWP_FOLDER = "/mnt/storage_b/archives/NWP"

# --- DWD ICON jobs and schedules --------------------------------------

dwd_base_path = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/DWD"

def build_config_on_runtime(model: str, run: str, delay: int = 0) -> dict:
    """Create a config dict for the DWD ICON model."""
    config = IconConfig(
        model=model,
        run=run,
        delay=delay,
        folder=f"{dwd_base_path}/{'ICON_Global' if model == 'global' else 'ICON_EU'}/{run}",
        zarr_path=f"{dwd_base_path}/{'ICON_Global' if model == 'global' else 'ICON_EU'}/{run}/{run}.zarr.zip"
    )
    config_dict = {
        "delay": config.delay,
        "folder": config.folder,
        "model": config.model,
        "run": config.run,
        "zarr_path": config.zarr_path
    }
    return config_dict

for r in ["00", "06", "12", "18"]:
    for model in ["global", "eu"]:
        for delay in [0, 1]:
            asset_job = dagster.define_asset_job(
                name=f"download_{model}_run_{r}_{'today' if delay == 0 else 'yesterday'}",
                selection=dagster.AssetSelection.all(),
                config={'ops': {
                    "download_model_files": {"config": build_config_on_runtime(model, r, delay)},
                    "process_model_files": {
                        "config": build_config_on_runtime(model, r, delay)},
                    "upload_model_files_to_hf": {
                        "config": build_config_on_runtime(model, r, delay)},
                    }},
                tags={"source": "icon"}
                )
            match (delay, r):
                case (0, "00"):
                    schedules.append(
                        dagster.ScheduleDefinition(job=asset_job, cron_schedule="30 4 * * *"))
                case (0, "06"):
                    schedules.append(
                        dagster.ScheduleDefinition(job=asset_job, cron_schedule="30 10 * * *"))
                case (0, "12"):
                    schedules.append(
                        dagster.ScheduleDefinition(job=asset_job, cron_schedule="30 16 * * *"))
                case (0, "18"):
                    schedules.append(
                        dagster.ScheduleDefinition(job=asset_job, cron_schedule="30 22 * * *"))
                case (1, "00"):
                    schedules.append(
                        dagster.ScheduleDefinition(job=asset_job, cron_schedule="1 0 * * *"))
                case (1, "06"):
                    schedules.append(
                        dagster.ScheduleDefinition(job=asset_job, cron_schedule="0 2 * * *"))
                case (1, "12"):
                    schedules.append(
                        dagster.ScheduleDefinition(job=asset_job, cron_schedule="0 6 * * *"))
                case (1, "18"):
                    schedules.append(
                        dagster.ScheduleDefinition(job=asset_job, cron_schedule="0 8 * * *"))

# --- CAMS jobs and schedules ----------------------------------------------

@dagster.daily_partitioned_config(start_date=dt.datetime(2015, 1, 1))
def CAMSDailyPartitionConfig(start: dt.datetime, _end: dt.datetime) -> dict[str, Any]:
    if start < dt.datetime.now() - dt.timedelta(days=30):
        # Only use subset for tape-based backfill
        single_level_variables = ['total_absorption_aerosol_optical_depth_400nm', 'total_absorption_aerosol_optical_depth_440nm',
    'total_absorption_aerosol_optical_depth_469nm', 'total_absorption_aerosol_optical_depth_500nm', 'total_absorption_aerosol_optical_depth_532nm',
    'total_absorption_aerosol_optical_depth_550nm', 'total_absorption_aerosol_optical_depth_645nm', 'total_absorption_aerosol_optical_depth_670nm',]
        multi_level_variables = ['aerosol_extinction_coefficient_532nm',]
        config = CAMSConfig(
            date=start.strftime("%Y-%m-%d"),
            raw_dir="/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/CAMS/raw",
            single_variables=single_level_variables,
            multi_variables=multi_level_variables,
        )
    else:
        config = CAMSConfig(
            date=start.strftime("%Y-%m-%d"),
            raw_dir="/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/CAMS/raw",
        )
    return {"ops": {"fetch_cams_forecast_for_day": {"config": json.loads(config.json())}}}


@dagster.job(
    config=CAMSDailyPartitionConfig,
    tags={"source": "cams"}
)
def cams_daily_archive() -> None:
    """Download CAMS data for a given day."""
    fetch_cams_forecast_for_day()

jobs.append(cams_daily_archive)
schedules.append(dagster.build_schedule_from_partitioned_job(cams_daily_archive, hour_of_day=16))


@dagster.daily_partitioned_config(start_date=dt.datetime(2020, 10, 27))
def CAMSEUDailyPartitionConfig(start: dt.datetime, _end: dt.datetime) -> dict[str, Any]:
    """Create a config dict for the CAMS EU model."""
    config = CAMSConfig(
        date=start.strftime("%Y-%m-%d"),
        raw_dir="/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/CAMS_EU/raw",
    )
    return {"ops": {"fetch_cams_eu_forecast_for_day": {"config": json.loads(config.json())}}}


@dagster.job(
    config=CAMSEUDailyPartitionConfig,
    tags={"source": "cams"}
)
def cams_eu_daily_archive() -> None:
    """Download CAMS data for a given day."""
    fetch_cams_eu_forecast_for_day()

jobs.append(cams_eu_daily_archive)
schedules.append(dagster.build_schedule_from_partitioned_job(cams_eu_daily_archive, hour_of_day=16))

# --- NWP Consumer jobs and schedules --------------------------------------

class NWPConsumerDagDefinition:
    """A class to define the NWPConsumerDagDefinition."""

    def __init__(
        self,
        source: str,
        folder: str,
        storage_path: str | None = None,
        env_overrides: dict[str, str] | None = None
    ) -> "NWPConsumerDagDefinition":
        """Create a NWPConsumerDagDefinition."""
        self.source = source
        self.storage_path = \
            storage_path or \
            f'/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/{folder}'
        self.env_overrides = env_overrides

# Create a dict of job names to their descriptions
nwp_consumer_jobs: dict[str, NWPConsumerDagDefinition] = {
    "ecmwf_daily_local_archive": NWPConsumerDagDefinition(
        source="ecmwf-mars",
        folder="ECMWF/uk",
        env_overrides={"ECMWF_AREA": "uk"}
    ),
    "ecmwf_india_daily_archive": NWPConsumerDagDefinition(
        source="ecmwf-mars",
        folder="ECMWF/india",
        env_overrides={"ECMWF_AREA": "nw-india", "ECMWF_HOURS": "84"}
    ),
    "ecmwf_malta_daily_archive": NWPConsumerDagDefinition(
        source="ecmwf-mars",
        folder="ECMWF/malta",
        env_overrides={"ECMWF_AREA": "malta"}
    ),
    "ceda_uk_daily_archive": NWPConsumerDagDefinition(
        source="ceda",
        folder="CEDA/uk",
    ),
}


def gen_partitioned_config_func(dagdef: NWPConsumerDagDefinition) \
        -> Callable[[str], dict[str, Any]]:
    """Create a config dict from a partition key."""

    def partitioned_config_func(partition_key: str) -> dict[str, Any]:
        time_window = partitions_def.time_window_for_partition_key(partition_key)
        consumer_config = NWPConsumerConfig(
            date_from=time_window.start.strftime("%Y-%m-%d"),
            date_to=time_window.start.strftime("%Y-%m-%d"),
            source=dagdef.source,
            env_overrides=dagdef.env_overrides or {},
            zarr_dir=f"{dagdef.storage_path}/zarr",
            raw_dir=f"{dagdef.storage_path}/raw",
        )
        return {
            "ops": {"nwp_consumer_download_op": {
                "config": json.loads(consumer_config.json()),
            }}
        }

    return partitioned_config_func

# Define the jobs and schedules from the above dict
for dagname, dagdef in nwp_consumer_jobs.items():

    partitions_def = dagster.DailyPartitionsDefinition(start_date=dt.datetime(2017, 1, 1))

    config = dagster.PartitionedConfig(
        partitions_def=partitions_def,
        run_config_for_partition_key_fn=gen_partitioned_config_func(dagdef),
    )

    @dagster.job(
        name=dagname,
        config=config,
        tags={"source": dagdef.source},
    )
    def ecmwf_daily_partitioned_archive() -> None:
        """Download and convert NWP data using the consumer according to input config."""
        nwp_consumer_convert_op(nwp_consumer_download_op())

    schedule = dagster.build_schedule_from_partitioned_job(
        job=ecmwf_daily_partitioned_archive,
        hour_of_day=20
    )

    jobs.append(ecmwf_daily_partitioned_archive)
    schedules.append(schedule)

