from dagster import load_assets_from_modules
from . import ecmwf_uk, ecmwf_nw_india

uk_assets = load_assets_from_modules(
    modules=[ecmwf_uk],
    group_name="ecmwf_uk",
)

nw_india_assets = load_assets_from_modules(
    modules=[ecmwf_nw_india],
    group_name="ecmwf_nw_india",
)

all_assets = [*uk_assets, *nw_india_assets]

