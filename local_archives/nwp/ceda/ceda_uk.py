"""CEDA UK data pipeline."""

import os

from nwp_consumer.internal.inputs import ceda
from local_archives.partitions import InitTimePartitionsDefinition
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nwp_consumer.internal import FetcherInterface
    from local_archives.nwp._generic_definitions_factory import MakeDefinitionsOutputs

from local_archives.nwp._generic_definitions_factory import (
    MakeDefinitionsOptions,
    make_definitions,
)

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

ceda_uk_source_archive = defs.source_asset
ceda_uk_raw_archive = defs.raw_asset
ceda_uk_zarr_archive = defs.zarr_asset
scan_ceda_uk_raw_archive = defs.raw_job
scan_ceda_uk_zarr_archive = defs.zarr_job
