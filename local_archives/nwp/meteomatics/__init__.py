import dagster as dg

from . import meteomatics_sites_india

india_site_assets = dg.load_assets_from_modules(
    modules=[meteomatics_sites_india],
    group_name="meteomatics_sites_india",
)

all_assets: list[dg.AssetsDefinition] = [
    *india_site_assets,
]
