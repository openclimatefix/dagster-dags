"""ECMWF Malta data pipeline."""
import dagster as dg
from nwp_consumer.internal import FetcherInterface
from nwp_consumer.internal.inputs.ecmwf import mars

from local_archives.nwp._generic_definitions_factory import (
    MakeDefinitionsOptions,
    MakeDefinitionsOutputs,
    make_definitions,
)

fetcher: FetcherInterface = mars.MARSClient(
    area="malta",
    hours=84,
)

partitions: dg.TimeWindowPartitionsDefinition = dg.TimeWindowPartitionsDefinition(
    start="2017-01-01T00:00",
    cron_schedule="0 0,12 * * *",  # 00:00 and 12:00
    fmt="%Y-%m-%dT%H:%M",
    end_offset=-(2 * 2),  # ECMWF only available 2 days back (2 partitions per day)
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
