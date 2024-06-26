import dagster as dg

from . import passiv_monthly, passiv_year

global_assets = dg.load_assets_from_modules(
    modules=[passiv_monthly, passiv_year],
    group_name="pv_passiv",
)

all_assets: list[dg.AssetsDefinition] = [*global_assets]

# TODO do we need to define jobs for these assets?

