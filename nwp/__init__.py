from dagster import Definitions, load_assets_from_modules
import os

import managers
from .ecmwf import all_assets as ecmwf_assets
from .cams import all_assets as cams_assets

from constants import LOCATIONS_BY_ENVIRONMENT

resources_by_env = {
    "leo": {
        "xr_zarr_io": managers.LocalFilesystemXarrayZarrManager(
            base_path=LOCATIONS_BY_ENVIRONMENT["leo"].RAW_FOLDER
        )
    },
    "local": {
        "xr_zarr_io": managers.LocalFilesystemXarrayZarrManager(
            base_path=LOCATIONS_BY_ENVIRONMENT["local"].RAW_FOLDER
        )
    }
}

defs = Definitions(
    assets=[*ecmwf_assets, *cams_assets],
    resources=resources_by_env[os.getenv("ENVIRONMENT", "local")]
)
