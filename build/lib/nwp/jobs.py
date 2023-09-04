from dagster import job, schedule, ScheduleEvaluationContext, RunRequest, RunConfig, materialize, ScheduleDefinition, op
from nwp.assets.dwd.common import IconConfig
from dagster import AssetSelection, define_asset_job

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


asset_jobs = []
schedule_jobs = []
for r in ["00", "06", "12", "18"]:
    for model in ["global", "eu"]:
        for delay in [0, 1]:
            asset_job = define_asset_job(f"download_{model}_run_{r}_{'today' if delay == 0 else 'yesterday'}", AssetSelection.all(),
                                         config={
                                             'ops': {"download_model_files": {"config": build_config_on_runtime(model, r, delay)},
                                                     "process_model_files": {"config": build_config_on_runtime(model, r, delay)},
                                                     "upload_model_files_to_hf": {
                                                         "config": build_config_on_runtime(model, r, delay)}, }})
            if delay == 0:
                if r == "00":
                    schedule = ScheduleDefinition(job=asset_job, cron_schedule="30 4 * * *")
                elif r == "06":
                    schedule = ScheduleDefinition(job=asset_job, cron_schedule="30 10 * * *")
                elif r == "12":
                    schedule = ScheduleDefinition(job=asset_job, cron_schedule="30 16 * * *")
                elif r == "18":
                    schedule = ScheduleDefinition(job=asset_job, cron_schedule="30 22 * * *")
            elif delay == 1:
                if r == "00":
                    schedule = ScheduleDefinition(job=asset_job, cron_schedule="1 0 * * *")
                elif r == "06":
                    schedule = ScheduleDefinition(job=asset_job, cron_schedule="0 2 * * *")
                elif r == "12":
                    schedule = ScheduleDefinition(job=asset_job, cron_schedule="0 6 * * *")
                elif r == "18":
                    schedule = ScheduleDefinition(job=asset_job, cron_schedule="0 8 * * *")

            asset_jobs.append(asset_job)
            schedule_jobs.append(schedule)

