import pandas as pd
from dagster import AssetSelection, define_asset_job, EnvVar
import dagster
from sat.assets.eumetsat.common import EumetsatConfig
from sat.assets import download_eumetsat_iodc_data
import datetime as dt
from typing import Any
import json
import os

jobs: list[dagster.JobDefinition] = []
schedules: list[dagster.ScheduleDefinition] = []

base_path = "/mnt/storage_c/IODC/"

# --- IODC jobs and schedules ----------------------------------------------

@dagster.daily_partitioned_config(start_date=dt.datetime(2017, 1, 1))
def IODCDailyPartitionConfig(start: dt.datetime, _end: dt.datetime) -> dict[str, Any]:
    # Do one day at a time
    config = EumetsatConfig(
        date=start.strftime("%Y-%m-%d"),
        end_date=start.strftime("%Y-%m-%d"),
        data_dir=base_path,
        api_key=os.getenv("EUMETSAT_API_KEY"),
        api_secret=os.getenv("EUMETSAT_API_SECRET"),

    )
    return {"ops": {"fetch_iodc_imagery_for_day": {"config": json.loads(config.json())}}}


@dagster.job(
    config=IODCDailyPartitionConfig,
    tags={"source": "eumetsat", dagster.MAX_RUNTIME_SECONDS_TAG: 345600} # 4 days
)
def iodc_daily_archive() -> None:
    """Download CAMS data for a given day."""
    download_eumetsat_iodc_data()

jobs.append(iodc_daily_archive)
schedules.append(dagster.build_schedule_from_partitioned_job(iodc_daily_archive, hour_of_day=1))