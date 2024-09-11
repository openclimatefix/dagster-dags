import dagster as dg

from . import ceda_uk, ceda_global

uk_assets = dg.load_assets_from_modules(
    modules=[ceda_uk],
    group_name="ceda_uk",
)

global_assets = dg.load_assets_from_modules(
    modules=[ceda_global],
    group_name="ceda_global",
)

all_assets: list[dg.AssetsDefinition] = [*uk_assets, *global_assets]
