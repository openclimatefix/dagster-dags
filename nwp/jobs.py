from dagster import AssetSelection, ScheduleDefinition, define_asset_job, schedule, job, RunConfig, ScheduleEvaluationContext

from nwp.assets.dwd.common import IconConfig
from nwp.assets.ecmwf.mars import nwp_consumer_docker_op, NWPConsumerConfig

import datetime as dt


base_path = "/mnt/storage_b/data/ocf/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/DWD"


def build_config_on_runtime(model, run, delay=0):
    config = IconConfig(model=model,
                        run=run,
                        delay=delay,
                        folder=f"{base_path}/ICON_Global/{run}",
                        zarr_path=f"{base_path}/ICON_Global/{run}/{run}.zarr.zip")
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
            
@job
def nwp_consumer_docker_job():
    nwp_consumer_docker_op()

@schedule(job=nwp_consumer_docker_job, cron_schedule="0 13 * * *")
def ecmwf_daily_schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        run_config={
            "ops": {"nwp_consumer_docker_op": NWPConsumerConfig(
                date_from=scheduled_date,
                date_to=scheduled_date,
                source="ecmwf-mars",
                docker_volumes=[
                    '/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/raw:/tmp/raw',
                    '/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/zarr:/tmp/zarr',
                    '/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/tmp:/tmp/nwpc'
                ]
                )}
            },
        tags={"date": scheduled_date},
    )

schedules.append(ecmwf_daily_schedule)

