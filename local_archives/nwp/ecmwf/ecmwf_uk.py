"""ECMWF UK data pipeline."""

from nwp_consumer.internal.inputs.ecmwf import mars

from ._factories import MakeDefinitionsOptions, make_definitions

fetcher = mars.Client(
    area="uk",
    param_group="basic",
)

defs = make_definitions(
    opts=MakeDefinitionsOptions(
        area="uk",
        fetcher=fetcher,
    ),
)

ecmwf_uk_source_archive = defs.source_asset
ecmwf_uk_raw_archive = defs.raw_asset
ecmwf_uk_zarr_archive = defs.zarr_asset
scan_ecmwf_uk_raw_archive = defs.raw_job
scan_ecmwf_uk_zarr_archive = defs.zarr_job
