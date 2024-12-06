import dagster as dg

from . import (
    ecmwf_malta,
    ecmwf_nw_india,
    ecmwf_uk,
    ecmwf_india,
    ecmwf_ens_stat_india,
)

uk_assets = dg.load_assets_from_modules(
    modules=[ecmwf_uk],
    group_name="ecmwf_uk",
)


nw_india_assets = dg.load_assets_from_modules(
    modules=[ecmwf_nw_india],
    group_name="ecmwf_nw_india",
)

malta_assets = dg.load_assets_from_modules(
    modules=[ecmwf_malta],
    group_name="ecmwf_malta",
)

india_assets = dg.load_assets_from_modules(
    modules=[ecmwf_india],
    group_name="ecmwf_india",
)

india_stat_assets = dg.load_assets_from_modules(
    modules=[ecmwf_ens_stat_india],
    group_name="ecmwf-ens_india-stat",
)

all_assets: list[dg.AssetsDefinition] = [
    *uk_assets,
    *nw_india_assets,
    *malta_assets,
    *india_assets,
    *india_stat_assets,
]
