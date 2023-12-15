import os

import dagster as dg

import managers
from constants import LOCATIONS_BY_ENVIRONMENT

from .nwp import cams, ecmwf

resources_by_env = {
    "leo": {
        "xr_zarr_io": managers.LocalFilesystemXarrayZarrManager(
            base_path=LOCATIONS_BY_ENVIRONMENT["leo"].PROCESSED_FOLDER,
        ),
    },
    "local": {
        "xr_zarr_io": managers.LocalFilesystemXarrayZarrManager(
            base_path=LOCATIONS_BY_ENVIRONMENT["local"].PROCESSED_FOLDER,
        ),
    },
}

all_assets: list[dg.AssetsDefinition] = [*ecmwf.all_assets, *cams.all_assets]

defs = dg.Definitions(
    assets=all_assets,
    resources=resources_by_env[os.getenv("ENVIRONMENT", "local")],
    jobs=[*ecmwf.all_jobs, *cams.all_jobs],
)
