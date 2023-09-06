from dagster import Definitions, load_assets_from_modules 
from nwp import assets, jobs

def test_compiles():
    all_assets = load_assets_from_modules([assets])
    defs = Definitions(
            assets=all_assets,
            jobs=jobs.asset_jobs,
            schedules=jobs.schedule_jobs
    )
