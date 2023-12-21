import os

import dagster as dg

import managers
from constants import LOCATIONS_BY_ENVIRONMENT

from . import nwp

resources_by_env = {
    "leo": {
        "nwp_xr_zarr_io": managers.LocalFilesystemXarrayZarrManager(
            base_path=LOCATIONS_BY_ENVIRONMENT["leo"].NWP_ZARR_FOLDER,
        ),
    },
    "local": {
        "nwp_xr_zarr_io": managers.LocalFilesystemXarrayZarrManager(
            base_path=LOCATIONS_BY_ENVIRONMENT["local"].NWP_ZARR_FOLDER,
        ),
    },
}

all_assets: list[dg.AssetsDefinition] = [
    *nwp.all_assets,
]

all_jobs: list[dg.JobDefinition] = [
    *nwp.all_jobs,
]

defs = dg.Definitions(
    assets=all_assets,
    resources=resources_by_env[os.getenv("ENVIRONMENT", "local")],
    jobs=all_jobs,
)
