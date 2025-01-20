import os

import dagster as dg

from . import pv

pv_assets = dg.load_assets_from_package_module(
    package_module=pv,
    group_name="pv",
    key_prefix="pv",
)

defs = dg.Definitions(
    assets=[*pv_assets],
    jobs=[],
    schedules=[],
)
