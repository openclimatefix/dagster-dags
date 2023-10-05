import datetime as dt
import json

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    daily_partitioned_config,
    define_asset_job,
    job,
)

from nwp.assets.dwd.common import IconConfig
from nwp.assets.ecmwf.mars import (
    NWPConsumerConfig,
    nwp_consumer_convert_op,
    nwp_consumer_download_op,
)

schedules = []

dwd_base_path = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/DWD"

def build_config_on_runtime(model: str, run: str, delay: int = 0) -> dict:
    """Create a config dict for the DWD ICON model."""
    config = IconConfig(model=model,
                        run=run,
                        delay=delay,
                        folder=f"{dwd_base_path}/{'ICON_Global' if model == 'global' else 'ICON_EU'}/{run}",
                        zarr_path=f"{dwd_base_path}/{'ICON_Global' if model == 'global' else 'ICON_EU'}/{run}/{run}.zarr.zip")
    config_dict = {"delay": config.delay, "folder": config.folder, "model": config.model, "run": config.run,
                   "zarr_path": config.zarr_path}
    return config_dict

schedules = []
for r in ["00", "06", "12", "18"]:
    for model in ["global", "eu"]:
        for delay in [0, 1]:
            asset_job = define_asset_job(
                name=f"download_{model}_run_{r}_{'today' if delay == 0 else 'yesterday'}",
                selection=AssetSelection.all(),
                config={'ops': {
                    "download_model_files": {"config": build_config_on_runtime(model, r, delay)},
                    "process_model_files": {"config": build_config_on_runtime(model, r, delay)},
                    "upload_model_files_to_hf": {"config": build_config_on_runtime(model, r, delay)},
                    }}
                )
            match (delay, r):
                case (0, "00"):
                    schedules.append(ScheduleDefinition(job=asset_job, cron_schedule="30 4 * * *"))
                case (0, "06"):
                    schedules.append(ScheduleDefinition(job=asset_job, cron_schedule="30 10 * * *"))
                case (0, "12"):
                    schedules.append(ScheduleDefinition(job=asset_job, cron_schedule="30 16 * * *"))
                case (0, "18"):
                    schedules.append(ScheduleDefinition(job=asset_job, cron_schedule="30 22 * * *"))
                case (1, "00"):
                    schedules.append(ScheduleDefinition(job=asset_job, cron_schedule="1 0 * * *"))
                case (1, "06"):
                    schedules.append(ScheduleDefinition(job=asset_job, cron_schedule="0 2 * * *"))
                case (1, "12"):
                    schedules.append(ScheduleDefinition(job=asset_job, cron_schedule="0 6 * * *"))
                case (1, "18"):
                    schedules.append(ScheduleDefinition(job=asset_job, cron_schedule="0 8 * * *"))


@daily_partitioned_config(start_date=dt.datetime(2020, 1, 1))
def ecmwf_uk_daily_partitioned_config(start: dt.datetime, _end: dt.datetime) -> dict:
    """Create a config dict for the nwp-consumer for uk data from ECMWF."""
    config: NWPConsumerConfig = NWPConsumerConfig(
        date_from=start.strftime("%Y-%m-%d"),
        date_to=start.strftime("%Y-%m-%d"),
        source="ecmwf-mars",
        env_overrides={"ECMWF_AREA": "uk"},
        zarr_dir='/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/uk/zarr',
        raw_dir='/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/uk/raw',
    )
    return {"ops": {
        "nwp_consumer_download_op": {"config": json.loads(config.json())},
    }}

@job(config=ecmwf_uk_daily_partitioned_config, name="ecmwf_daily_local_archive")
def ecmwf_uk_daily_local_archive() -> None:
    """Download and convert ECMWF data for the UK."""
    nwp_consumer_convert_op(nwp_consumer_download_op())

@daily_partitioned_config(start_date=dt.datetime(2020, 1, 1))
def ecmwf_india_daily_partitioned_config(start: dt.datetime, _end: dt.datetime) -> dict:
    """Create a config dict for the nwp-consumer for india data from ECMWF."""
    config: NWPConsumerConfig = NWPConsumerConfig(
        date_from=start.strftime("%Y-%m-%d"),
        date_to=start.strftime("%Y-%m-%d"),
        source="ecmwf-mars",
        env_overrides={"ECMWF_AREA": "nw-india"},
        zarr_dir='/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/india/zarr',
        raw_dir='/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/india/raw',
    )
    return {"ops": {
        "nwp_consumer_download_op": {"config": json.loads(config.json())},
    }}

@job(config=ecmwf_india_daily_partitioned_config)
def ecmwf_india_daily_local_archive() -> None:
    """Download and convert ECMWF data for India."""
    nwp_consumer_convert_op(nwp_consumer_download_op())

schedules.append(build_schedule_from_partitioned_job(ecmwf_uk_daily_local_archive, hour_of_day=13))
schedules.append(build_schedule_from_partitioned_job(ecmwf_india_daily_local_archive, hour_of_day=14))

