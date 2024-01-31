import dagster as dg

from . import icon_global

global_assets = dg.load_assets_from_modules(
    modules=[icon_global],
    group_name="icon_global",
)

all_assets: list[dg.AssetsDefinition] = [*global_assets]