"""ECMWF Malta data pipeline."""

from nwp_consumer.internal.inputs.ecmwf import mars

from ._factories import MakeDefinitionsOptions, make_definitions

fetcher = mars.Client(
    area="malta",
    param_group="basic",
)

defs = make_definitions(
    opts=MakeDefinitionsOptions(
        area="malta",
        fetcher=fetcher,
    ),
)

ecmwf_malta_source_archive = defs.source_asset
ecmwf_malta_raw_archive = defs.raw_asset
ecmwf_malta_zarr_archive = defs.zarr_asset
scan_ecmwf_malta_raw_archive = defs.raw_job
scan_ecmwf_malta_zarr_archive = defs.zarr_job
