import datetime as dt
import json
from typing import Any

import dagster
from sat.assets import (
    download_eumetsat_0_deg_data,
    download_eumetsat_iodc_data,
    download_eumetsat_rss_data,
)
from sat.assets.eumetsat.common import EumetsatConfig

jobs: list[dagster.JobDefinition] = []
schedules: list[dagster.ScheduleDefinition] = []

base_path = "/mnt/storage_c/IODC/"

# --- IODC jobs and schedules ----------------------------------------------

@dagster.daily_partitioned_config(start_date=dt.datetime(2017, 1, 1))
def IODCDailyPartitionConfig(start: dt.datetime, _end: dt.datetime) -> dict[str, Any]:
    # Do one day at a time
    config = EumetsatConfig(
        date=start.strftime("%Y-%m-%d"),
        end_date=_end.strftime("%Y-%m-%d"),
        data_dir=base_path,
        api_key="",
        api_secret="",

    )
    config = json.loads(config.json())
    config["api_key"] = {"env": "EUMETSAT_API_KEY"}
    config["api_secret"] = {"env": "EUMETSAT_API_SECRET"}
    return {"ops": {"download_eumetsat_iodc_data": {"config": config}}}


@dagster.job(
    config=IODCDailyPartitionConfig,
    tags={"source": "eumetsat", dagster.MAX_RUNTIME_SECONDS_TAG: 345600}, # 4 days
)
def iodc_daily_archive() -> None:
    """Download IODC data for a given day."""
    download_eumetsat_iodc_data()

jobs.append(iodc_daily_archive)
schedules.append(dagster.build_schedule_from_partitioned_job(iodc_daily_archive, hour_of_day=23))


@dagster.daily_partitioned_config(start_date=dt.datetime(2008, 1, 1))
def RSSDailyPartitionConfig(start: dt.datetime, _end: dt.datetime) -> dict[str, Any]:
    # Do one day at a time
    config = EumetsatConfig(
        date=start.strftime("%Y-%m-%d"),
        end_date=_end.strftime("%Y-%m-%d"),
        data_dir=base_path,
        api_key="",
        api_secret="",

    )
    config = json.loads(config.json())
    config["api_key"] = {"env": "EUMETSAT_API_KEY"}
    config["api_secret"] = {"env": "EUMETSAT_API_SECRET"}
    return {"ops": {"download_eumetsat_rss_data": {"config": config}}}


@dagster.job(
    config=RSSDailyPartitionConfig,
    tags={"source": "eumetsat", dagster.MAX_RUNTIME_SECONDS_TAG: 345600}, # 4 days
)
def rss_daily_archive() -> None:
    """Download RSS data for a given day."""
    download_eumetsat_rss_data()

jobs.append(rss_daily_archive)
schedules.append(dagster.build_schedule_from_partitioned_job(rss_daily_archive, hour_of_day=23))

@dagster.daily_partitioned_config(start_date=dt.datetime(2008, 1, 1))
def ZeroDegDailyPartitionConfig(start: dt.datetime, _end: dt.datetime) -> dict[str, Any]:
    # Do one day at a time
    config = EumetsatConfig(
        date=start.strftime("%Y-%m-%d"),
        end_date=_end.strftime("%Y-%m-%d"),
        data_dir=base_path,
        api_key="",
        api_secret="",

    )
    config = json.loads(config.json())
    config["api_key"] = {"env": "EUMETSAT_API_KEY"}
    config["api_secret"] = {"env": "EUMETSAT_API_SECRET"}
    return {"ops": {"download_eumetsat_0_deg_data": {"config": config}}}


@dagster.job(
    config=ZeroDegDailyPartitionConfig,
    tags={"source": "eumetsat", dagster.MAX_RUNTIME_SECONDS_TAG: 345600}, # 4 days
)
def zero_deg_daily_archive() -> None:
    """Download RSS data for a given day."""
    download_eumetsat_0_deg_data()

jobs.append(zero_deg_daily_archive)
schedules.append(dagster.build_schedule_from_partitioned_job(zero_deg_daily_archive, hour_of_day=23))
