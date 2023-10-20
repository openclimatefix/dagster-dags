import datetime as dt
from nwp.assets.cams import (
    fetch_cams_forecast_for_day,
    CAMSConfig
)
import json
from collections.abc import Callable
from typing import Any

import dagster

from nwp.assets.dwd.common import IconConfig
from nwp.assets.ecmwf.mars import (
    NWPConsumerConfig,
    nwp_consumer_convert_op,
    nwp_consumer_download_op
)

jobs: list[dagster.JobDefinition] = []
schedules: list[dagster.ScheduleDefinition] = []

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
                    }}
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
    config = CAMSConfig(
        date=start.strftime("%Y-%m-%d"),
        raw_dir="/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/CAMS/raw",
    )
    return {"ops": {"fetch_cams_forecast_for_day": {"config": json.loads(config.json())}}}


@dagster.job(config=CAMSDailyPartitionConfig)
def cams_daily_archive() -> None:
    """Download CAMS data for a given day."""
    fetch_cams_forecast_for_day()

jobs.append(cams_daily_archive)
schedules.append(dagster.build_schedule_from_partitioned_job(cams_daily_archive, hour_of_day=16))

# --- NWP Consumer jobs and schedules --------------------------------------

class NWPConsumerDagDefinition:
    """A class to define the NWPConsumerDagDefinition."""

    def __init__(
        self, source: str, storage_path: str | None = None, env_overrides: dict[str, str] | None = None
    ) -> "NWPConsumerDagDefinition":
        """Create a NWPConsumerDagDefinition."""
        self.source = source
        area = env_overrides.get("ECMWF_AREA", "no-area")
        self.storage_path = \
            storage_path or \
            f'/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/{area}'
        self.env_overrides = env_overrides

nwp_consumer_jobs: dict[str, NWPConsumerDagDefinition] = {
    "uk": NWPConsumerDagDefinition(
        source="ecmwf-mars",
        env_overrides={"ECMWF_AREA": "uk"}
    ),
    "india": NWPConsumerDagDefinition(
        source="ecmwf-mars",
        env_overrides={"ECMWF_AREA": "nw-india", "ECMWF_HOURS": "84"}
    ),
    "malta": NWPConsumerDagDefinition(
        source="ecmwf-mars",
        env_overrides={"ECMWF_AREA": "malta"}
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
            env_overrides=dagdef.env_overrides,
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
for loc, dagdef in nwp_consumer_jobs.items():

    partitions_def = dagster.DailyPartitionsDefinition(start_date=dt.datetime(2020, 1, 1))

    config = dagster.PartitionedConfig(
        partitions_def=partitions_def,
        run_config_for_partition_key_fn=gen_partitioned_config_func(dagdef),
    )

    @dagster.job(
        name="ecmwf_daily_local_archive" if loc=="uk" else f"ecmwf_{loc}_daily_archive",
        config=config,
        tags={"area": loc},
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

