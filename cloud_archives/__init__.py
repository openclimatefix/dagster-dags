import dagster as dg

from . import nwp

all_assets = [
    *nwp.all_assets,
]

defs = dg.Definitions(
    assets=all_assets,
)
