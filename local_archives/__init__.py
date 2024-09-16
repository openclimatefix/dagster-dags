import os

import dagster as dg
from dagster_docker import PipesDockerClient

import managers
import resources
from constants import LOCATIONS_BY_ENVIRONMENT

from . import nwp, sat

resources_by_env = {
    "leo": {
        "nwp_xr_zarr_io": managers.LocalFilesystemXarrayZarrManager(
            base_path=LOCATIONS_BY_ENVIRONMENT["leo"].NWP_ZARR_FOLDER,
        ),
        "meteomatics_api": resources.MeteomaticsAPIResource(
            username=dg.EnvVar("METEOMATICS_USERNAME"),
            password=dg.EnvVar("METEOMATICS_PASSWORD"),
        ),
        "pipes_subprocess_client": dg.PipesSubprocessClient(),
        "pipes_docker_client": PipesDockerClient(),
    },
    "local": {
        "nwp_xr_zarr_io": managers.LocalFilesystemXarrayZarrManager(
            base_path=LOCATIONS_BY_ENVIRONMENT["local"].NWP_ZARR_FOLDER,
        ),
        "meteomatics_api": resources.MeteomaticsAPIResource(
            username=dg.EnvVar("METEOMATICS_USERNAME"),
            password=dg.EnvVar("METEOMATICS_PASSWORD"),
        ),
        "pipes_subprocess_client": dg.PipesSubprocessClient(),
        "pipes_docker_client": PipesDockerClient(),
    },
}

all_assets: list[dg.AssetsDefinition] = [
    *nwp.all_assets,
    *sat.all_assets,
]

all_jobs: list[dg.JobDefinition] = [
    *nwp.all_jobs,
    *sat.all_jobs,
]

all_schedules: list[dg.ScheduleDefinition] = [
    *nwp.all_schedules,
    *sat.all_schedules,
]

defs = dg.Definitions(
    assets=all_assets,
    resources=resources_by_env[os.getenv("ENVIRONMENT", "local")],
    jobs=all_jobs,
    schedules=all_schedules,
)
