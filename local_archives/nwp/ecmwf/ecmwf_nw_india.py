"""ECMWF NW India data pipeline."""

import os

import dagster as dg
from nwp_consumer.internal.inputs.ecmwf import mars

from constants import LOCATIONS_BY_ENVIRONMENT

from ._factories import MakeAssetDefinitionsOptions, make_definitions

env = os.getenv("ENVIRONMENT", "local")
RAW_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].RAW_FOLDER
ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].PROCESSED_FOLDER

fetcher = mars.Client(
    area="nw-india",
    param_group="basic",
)

defs = make_definitions(
    opts=MakeAssetDefinitionsOptions(
        area="nw_india",
        fetcher=fetcher,
    ),
)

ecmwf_nw_india_source_archive = defs.source_asset
ecmwf_nw_india_raw_archive = defs.raw_asset
ecmwf_nw_india_zarr_archive = defs.zarr_asset
scan_ecmwf_nw_india_raw_archive = defs.raw_job
scan_ecmwf_nw_india_zarr_archive = defs.zarr_job
