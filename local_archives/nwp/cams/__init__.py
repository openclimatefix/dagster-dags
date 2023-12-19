import dagster as dg

from . import cams_eu, cams_global

eu_assets = dg.load_assets_from_modules(
    modules=[cams_eu],
    group_name="cams_eu",
)
eu_jobs = [
    cams_eu.scan_cams_eu_raw_archive
]

global_assets = dg.load_assets_from_modules(
    modules=[cams_global],
    group_name="cams_global",
)
global_jobs = [
    cams_global.scan_cams_global_raw_archive
]

all_assets: list[dg.AssetsDefinition] = [*eu_assets, *global_assets]
all_jobs: list[dg.JobDefinition] = [*eu_jobs, *global_jobs]
