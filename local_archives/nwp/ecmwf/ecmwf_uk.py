"""ECMWF UK data pipeline."""

from nwp_consumer.internal.inputs.ecmwf import mars
from local_archives.partitions import InitTimePartitionsDefinition

from local_archives.nwp._generic_definitions_factory import (
    MakeDefinitionsOptions,
    make_definitions,
)

fetcher = mars.Client(
    area="uk",
    param_group="basic",
)

partitions = InitTimePartitionsDefinition(
    start="2017-01-01",
    init_times=["00:00", "12:00"],
)

defs = make_definitions(
    opts=MakeDefinitionsOptions(
        area="uk",
        source="ecmwf",
        partitions=partitions,
        fetcher=fetcher,
    ),
)

ecmwf_uk_source_archive = defs.source_asset
ecmwf_uk_raw_archive = defs.raw_asset
ecmwf_uk_zarr_archive = defs.zarr_asset
scan_ecmwf_uk_raw_archive = defs.raw_job
scan_ecmwf_uk_zarr_archive = defs.zarr_job
