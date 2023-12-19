"""ECMWF Malta data pipeline."""

from nwp_consumer.internal.inputs.ecmwf import mars
from local_archives.partitions import InitTimePartitionsDefinition
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nwp_consumer.internal import FetcherInterface
    from local_archives.nwp._generic_definitions_factory import MakeDefinitionsOutputs

from local_archives.nwp._generic_definitions_factory import (
    MakeDefinitionsOptions,
    make_definitions,
)

fetcher: FetcherInterface  = mars.Client(
    area="malta",
)

partitions: InitTimePartitionsDefinition = InitTimePartitionsDefinition(
    start="2017-01-01",
    init_times=["00:00", "12:00"],
)

defs: MakeDefinitionsOutputs = make_definitions(
    opts=MakeDefinitionsOptions(
        area="malta",
        source="ecmwf",
        partitions=partitions,
        fetcher=fetcher,
    ),
)

ecmwf_malta_source_archive = defs.source_asset
ecmwf_malta_raw_archive = defs.raw_asset
ecmwf_malta_zarr_archive = defs.zarr_asset
scan_ecmwf_malta_raw_archive = defs.raw_job
scan_ecmwf_malta_zarr_archive = defs.zarr_job
