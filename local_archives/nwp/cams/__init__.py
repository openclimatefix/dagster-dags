import dagster as dg

from . import cams_eu, cams_global

eu_assets = dg.load_assets_from_modules(
    modules=[cams_eu],
    group_name="cams_eu",
)

global_assets = dg.load_assets_from_modules(
    modules=[cams_global],
    group_name="cams_global",
)

all_assets: list[dg.AssetsDefinition] = [*eu_assets, *global_assets]
