from dagster import load_assets_from_modules
from . import cams_eu

eu_assets = load_assets_from_modules(
    modules=[cams_eu],
    group_name="cams_eu",
)

all_assets = [*eu_assets]
