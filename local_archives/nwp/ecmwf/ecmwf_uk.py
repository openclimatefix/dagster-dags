"""ECMWF UK data pipeline."""


import os

import dagster as dg
from nwp_consumer.internal.inputs.ecmwf import mars

from constants import LOCATIONS_BY_ENVIRONMENT

from ._factories import MakeAssetDefinitionsOptions, make_definitions

env = os.getenv("ENVIRONMENT", "local")
RAW_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].RAW_FOLDER
ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].PROCESSED_FOLDER

fetcher = mars.Client(
    area="uk",
    param_group="basic",
)

defs = make_definitions(
    opts=MakeAssetDefinitionsOptions(
        area="uk",
        fetcher=fetcher,
    ),
)

ecmwf_uk_source_archive = defs.source_asset
ecmwf_uk_raw_archive = defs.raw_asset
ecmwf_uk_zarr_archive = defs.zarr_asset
scan_ecmwf_uk_raw_archive = defs.raw_job
scan_ecmwf_uk_zarr_archive = defs.zarr_job

