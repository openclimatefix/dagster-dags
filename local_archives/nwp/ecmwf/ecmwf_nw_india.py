"""ECMWF NW India data pipeline."""

from nwp_consumer.internal.inputs.ecmwf import mars

from ._factories import MakeDefinitionsOptions, make_definitions

fetcher = mars.Client(
    area="nw-india",
    param_group="basic",
)

defs = make_definitions(
    opts=MakeDefinitionsOptions(
        area="nw_india",
        fetcher=fetcher,
    ),
)

ecmwf_nw_india_source_archive = defs.source_asset
ecmwf_nw_india_raw_archive = defs.raw_asset
ecmwf_nw_india_zarr_archive = defs.zarr_asset
scan_ecmwf_nw_india_raw_archive = defs.raw_job
scan_ecmwf_nw_india_zarr_archive = defs.zarr_job
