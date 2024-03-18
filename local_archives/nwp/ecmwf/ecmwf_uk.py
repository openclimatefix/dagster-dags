"""ECMWF UK data pipeline."""

from nwp_consumer.internal import FetcherInterface
from nwp_consumer.internal.inputs.ecmwf import mars

from local_archives.nwp._generic_definitions_factory import (
    MakeDefinitionsOptions,
    MakeDefinitionsOutputs,
    make_definitions,
)
from local_archives.partitions import InitTimePartitionsDefinition

fetcher: FetcherInterface = mars.MARSClient(
    area="uk",
    hours=84,
)

partitions: InitTimePartitionsDefinition = InitTimePartitionsDefinition(
    start="2017-01-01",
    init_times=["00:00", "12:00"],
    end_offset=-1,
)

defs: MakeDefinitionsOutputs = make_definitions(
    opts=MakeDefinitionsOptions(
        area="uk",
        source="ecmwf",
        partitions=partitions,
        fetcher=fetcher,
    ),
)

ecmwf_uk_raw_archive = defs.raw_asset
ecmwf_uk_zarr_archive = defs.zarr_asset
