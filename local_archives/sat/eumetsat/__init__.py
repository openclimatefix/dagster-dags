import dagster as dg

from . import eumetsat_iodc


iodc_assets = dg.load_assets_from_modules(
    modules=[eumetsat_iodc],
    group_name="eumetsat_iodc",
)

all_assets: list[dg.AssetsDefinition] = [*iodc_assets]

