"""CEDA UK data pipeline."""

import os

from nwp_consumer.internal import FetcherInterface
from nwp_consumer.internal.inputs import ceda

from local_archives.nwp._generic_definitions_factory import (
    MakeDefinitionsOptions,
    MakeDefinitionsOutputs,
    make_definitions,
)
from local_archives.partitions import InitTimePartitionsDefinition

fetcher: FetcherInterface = ceda.Client(
    ftpUsername=os.getenv("CEDA_FTP_USER", "not-set"),
    ftpPassword=os.getenv("CEDA_FTP_PASS", "not-set"),
)

partitions: InitTimePartitionsDefinition = InitTimePartitionsDefinition(
    start="2017-01-01",
    init_times=["00:00", "03:00", "06:00", "09:00", "12:00", "15:00", "18:00", "21:00"],
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
