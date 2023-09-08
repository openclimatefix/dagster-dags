from dagster import Definitions, load_assets_from_modules

from nwp import assets, jobs


all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    schedules=jobs.schedules,
)
