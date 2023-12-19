"""ECMWF NW India data pipeline."""

from nwp_consumer.internal.inputs.ecmwf import mars
from partitions import InitTimePartitionsDefinition

from local_archives.nwp._generic_definitions_factory import (
    MakeDefinitionsOptions,
    make_definitions,
)

fetcher = mars.Client(
    area="nw-india",
    hours=84,
)

partitions = InitTimePartitionsDefinition(
    start="2017-01-01",
    init_times=["00:00", "12:00"],
)

defs = make_definitions(
    opts=MakeDefinitionsOptions(
        area="nw_india",
        source="ecmwf",
        partitions=partitions,
        fetcher=fetcher,
    ),
)

ecmwf_nw_india_source_archive = defs.source_asset
ecmwf_nw_india_raw_archive = defs.raw_asset
ecmwf_nw_india_zarr_archive = defs.zarr_asset
scan_ecmwf_nw_india_raw_archive = defs.raw_job
scan_ecmwf_nw_india_zarr_archive = defs.zarr_job
