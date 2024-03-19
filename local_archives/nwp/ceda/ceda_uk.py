"""CEDA UK data pipeline."""
import os

import dagster as dg
from nwp_consumer.internal import FetcherInterface
from nwp_consumer.internal.inputs import ceda

from local_archives.nwp._generic_definitions_factory import (
    MakeDefinitionsOptions,
    MakeDefinitionsOutputs,
    make_definitions,
)

fetcher: FetcherInterface = ceda.Client(
    ftpUsername=os.getenv("CEDA_FTP_USER", "not-set"),
    ftpPassword=os.getenv("CEDA_FTP_PASS", "not-set"),
)

partitions: dg.TimeWindowPartitionsDefinition = dg.TimeWindowPartitionsDefinition(
    start="2017-01-01T00:00",
    cron_schedule="0 0/3 * * *",  # Every 3 hours
    fmt="%Y-%m-%dT%H:%M",
    end_offset=-(8 * 8),  # CEDA only available 8 days back (8 partitions per day)
)

defs: MakeDefinitionsOutputs = make_definitions(
    opts=MakeDefinitionsOptions(
        area="uk",
        source="ceda",
        fetcher=fetcher,
        partitions=partitions,
    ),
)

ceda_uk_raw_archive = defs.raw_asset
ceda_uk_zarr_archive = defs.zarr_asset
