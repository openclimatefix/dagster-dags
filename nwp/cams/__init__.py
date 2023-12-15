from dagster import load_assets_from_modules
from . import cams_eu, cams_global

eu_assets = load_assets_from_modules(
    modules=[cams_eu],
    group_name="cams_eu",
)

global_assets = load_assets_from_modules(
    modules=[cams_global],
    group_name="cams_global",
)

all_assets = [*eu_assets, *global_assets]
