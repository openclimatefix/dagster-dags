import dagster as dg

from . import ecmwf_malta, ecmwf_nw_india, ecmwf_uk, ecmwf_india

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

all_assets: list[dg.AssetsDefinition] = [
    *uk_assets,
    *nw_india_assets,
    *malta_assets,
    *india_assets,
]
