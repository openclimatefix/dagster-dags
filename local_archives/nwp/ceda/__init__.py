import dagster as dg

from . import ceda_uk

uk_assets = dg.load_assets_from_modules(
    modules=[ceda_uk],
    group_name="ceda_uk",
)

all_assets: list[dg.AssetsDefinition] = [*uk_assets]
