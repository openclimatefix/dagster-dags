import dagster as dg

from .nwp import icon

defs = dg.Definitions(
    assets=[*icon.all_assets],
    jobs=[*icon.all_jobs],
    schedules=[
        dg.build_schedule_from_partitioned_job(job) for job in icon.all_jobs
    ]
)
