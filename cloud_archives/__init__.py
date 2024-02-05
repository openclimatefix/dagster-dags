import dagster as dg

from .nwp import icon

defs = dg.Definitions(
    assets=[*icon.all_assets],
    jobs=[*icon.all_jobs],
)
