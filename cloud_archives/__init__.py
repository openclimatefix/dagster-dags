import os

import dagster as dg

from .nwp import icon
from .pv import passiv

defs = dg.Definitions(
    assets=[*icon.all_assets, *passiv.all_assets],
    jobs=[*icon.all_jobs], # TODO all jobs?
    schedules=[dg.build_schedule_from_partitioned_job(job) for job in icon.all_jobs],
)
