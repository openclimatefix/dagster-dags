import dagster as dg

from . import gfs

all_assets: list[dg.AssetsDefinition] = dg.load_assets_from_modules(
    modules=[gfs],
    group_name="gfs_global",
)
