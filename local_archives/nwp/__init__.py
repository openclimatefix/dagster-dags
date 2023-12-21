"""Definitions for the NWP dagster code location."""

import dagster as dg

from . import cams, ceda, ecmwf, jobs

all_assets: list[dg.AssetsDefinition] = [
    *ceda.all_assets,
    *ecmwf.all_assets,
    *cams.all_assets,
]

all_jobs: list[dg.JobDefinition] = [
    jobs.scan_nwp_raw_archive,
    jobs.scan_nwp_zarr_archive,
]
