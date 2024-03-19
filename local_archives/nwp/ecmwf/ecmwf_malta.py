"""ECMWF Malta data pipeline."""
import dagster as dg
from nwp_consumer.internal import FetcherInterface
from nwp_consumer.internal.inputs.ecmwf import mars

from local_archives.nwp._generic_definitions_factory import (
    MakeDefinitionsOptions,
    MakeDefinitionsOutputs,
    make_definitions,
)
from local_archives.partitions import InitTimePartitionsDefinition

fetcher: FetcherInterface = mars.MARSClient(
    area="malta",
)

partitions: InitTimePartitionsDefinition = InitTimePartitionsDefinition(
    start="2017-01-01",
    init_times=["00:00", "12:00"],
    end_offset=-2,
)

defs: MakeDefinitionsOutputs = make_definitions(
    opts=MakeDefinitionsOptions(
        area="malta",
        source="ecmwf",
        partitions=partitions,
        fetcher=fetcher,
    ),
)

ecmwf_malta_raw_archive = defs.raw_asset
ecmwf_malta_zarr_archive = defs.zarr_asset
