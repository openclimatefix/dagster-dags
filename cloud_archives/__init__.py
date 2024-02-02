import dagster as dg

from .nwp import icon

defs = dg.Definitions(
    jobs=[*icon.all_jobs]
)
