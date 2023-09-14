import datetime as dt

from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    build_schedule_from_partitioned_job,
    job,
    partitioned_config,
)

from nwp.assets.dwd.archive_to_hf import (
    download_model_files,
    process_model_files,
    upload_model_files_to_hf,
)
from nwp.assets.dwd.common import IconConfig
from nwp.assets.dwd.utils import build_config
from nwp.assets.ecmwf.mars import nwp_consumer_docker_op

schedules = []

@partitioned_config(partitions_def=MultiPartitionsDefinition({
    "date": DailyPartitionsDefinition(start_date=dt.datetime(2023, 9, 10)),
    "run": StaticPartitionsDefinition(["00", "06", "12", "18"]),
}))
def icon_global_dailyrun_partitioned_config(partition_key):
    return {"ops": {
        "download_model_files": {"config": build_config("global", partition_key["run"])},
        "process_model_files": {"config": build_config("global", partition_key["run"])},
        "upload_model_files_to_hf": {"config": build_config("global", partition_key["run"])},
    }}

@job(config=icon_global_dailyrun_partitioned_config)
def icon_global_hf_archive():
    download_model_files()
    process_model_files()
    upload_model_files_to_hf()

schedules.append(build_schedule_from_partitioned_job(icon_global_hf_archive))

@partitioned_config(partitions_def=MultiPartitionsDefinition({
    "date": DailyPartitionsDefinition(start_date=dt.datetime(2023, 9, 10)),
    "run": StaticPartitionsDefinition(["00", "06", "12", "18"]),
}))
def icon_eu_dailyrun_partitioned_config(partition_key):
    return {"ops": {
        "download_model_files": {"config": build_config("eu", partition_key["run"])},
        "process_model_files": {"config": build_config("eu", partition_key["run"])},
        "upload_model_files_to_hf": {"config": build_config("eu", partition_key["run"])},
}}

@job(config=icon_eu_dailyrun_partitioned_config)
def icon_eu_hf_archive():
    download_model_files()
    process_model_files()
    upload_model_files_to_hf()

schedules.append(build_schedule_from_partitioned_job(icon_eu_hf_archive))

@partitioned_config(partitions_def=DailyPartitionsDefinition(start_date=dt.datetime(2021, 1, 1)))
def ecmwf_daily_partitioned_config(start: dt.datetime, _end: dt.datetime):
    return {"ops": {"nwp_consumer_docker_op": {"config": {
        "date_from": start.strftime("%Y-%m-%d"),
        "date_to": start.strftime("%Y-%m-%d"),
        "source": "ecmwf-mars",
        "env_vars": ["ECMWF_API_URL", "ECMWF_API_KEY", "ECMWF_API_EMAIL"],
        "docker_volumes": ['}/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF:/tmp']
    }}}}

@job(config=ecmwf_daily_partitioned_config)
def ecmwf_daily_local_archive():
    nwp_consumer_docker_op()

schedules.append(build_schedule_from_partitioned_job(ecmwf_daily_local_archive, hour_of_day=13))
