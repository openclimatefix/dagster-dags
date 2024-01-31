"""Pipeline for the processing of global ICON data."""

import dagster as dg

from cloud_archives.nwp.icon._factories import MakeDefinitionsOptions, MakeDefinitionsOutputs, make_definitions

opts: MakeDefinitionsOptions = MakeDefinitionsOptions(
    area="global",
    hf_repo_id="sol-ocf/test-dwd-global",
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m-%d|%H:%M",
        start="2020-01-01|00:00",
        cron_schedule="0 0/12 * * *",
    ),
)

defs: MakeDefinitionsOutputs = make_definitions(opts)

icon_global_zarr_asset = defs.zarr_asset
