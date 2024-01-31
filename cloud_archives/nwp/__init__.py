import dagster as dg

from . import (
    icon,
)

all_assets: list[dg.AssetsDefinition] = [
    *icon.all_assets,
]
