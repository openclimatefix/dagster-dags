"""All dagster definitions to be surfaced in this code location."""

import dagster as dg
from dagster_docker import PipesDockerClient

from .assets import nwp, pv, sat

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

pv_assets = dg.load_assets_from_package_module(
    package_module=pv,
    group_name="pv",
    key_prefix="pv",
)

defs = dg.Definitions(
    assets=[*nwp_assets, *sat_assets, *pv_assets],
    resources={
        "pipes_subprocess_client": dg.PipesSubprocessClient(),
        "pipes_docker_client": PipesDockerClient(),
    },
    jobs=[],
    schedules=[],
)

