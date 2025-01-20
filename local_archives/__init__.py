import os

import dagster as dg
from dagster_docker import PipesDockerClient

from . import nwp, sat

nwp_assets = dg.load_assets_from_package_module(
    package_module=nwp,
    group_name="nwp",
    key_prefix="nwp",
)

sat_assets = dg.load_assets_from_package_module(
    package_module=sat,
    group_name="sat",
    key_prefix="sat",
)

defs = dg.Definitions(
    assets=[*nwp_assets, *sat_assets],
    resources={
        "pipes_subprocess_client": dg.PipesSubprocessClient(),
        "pipes_docker_client": PipesDockerClient(),
    },
    jobs=[],
    schedules=[],
)
