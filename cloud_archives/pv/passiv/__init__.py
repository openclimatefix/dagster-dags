import dagster as dg

from . import passiv_daily

global_assets = dg.load_assets_from_modules(
    modules=[passiv_daily],
    group_name="pv_passiv_daily",
)

all_assets: list[dg.AssetsDefinition] = [*global_assets]

# TODO do we need to define jobs for these assets?

