import dagster as dg

from . import icon_eu, icon_global

global_assets = dg.load_assets_from_modules(
    modules=[icon_global],
    group_name="icon_global",
)

eu_assets = dg.load_assets_from_modules(
    modules=[icon_eu],
    group_name="icon_eu",
)

all_assets: list[dg.AssetsDefinition] = [*global_assets, *eu_assets]

all_jobs: list[dg.JobDefinition] = [
    icon_global.archive_icon_global_sl_job,
    icon_global.archive_icon_global_ml_job,
    icon_eu.archive_icon_europe_sl_job,
    icon_eu.archive_icon_europe_ml_job,
]
