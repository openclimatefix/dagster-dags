"""Pipeline for the processing of global ICON data."""
import os

import dagster as dg

from cloud_archives.ops.huggingface import (
    HFFileConfig,
)
from cloud_archives.ops.kbatch import (
    NWPConsumerConfig,
)

from ._ops import (
    AssetMaterializationConfig,
    create_icon_kbatch_huggingface_graph_config,
    icon_kbatch_huggingface_graph,
)

archive_icon_job = icon_kbatch_huggingface_graph.to_job(
    name="archive_icon_global_job",
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m-%d|%H:%M",
        start="2024-01-31|00:00",
        cron_schedule="0 0/12 * * *",
    ),
    config=create_icon_kbatch_huggingface_graph_config(
        nwp_config=NWPConsumerConfig(
            source="icon",
            sink="huggingface",
            docker_tag="0.3.1",
            env={
                "ICON_MODEL": "global",
                "ICON_PARAMETER_GROUP": "full",
                "HUGGINGFACE_TOKEN": os.environ["HUGGINGFACE_TOKEN"],
                "HUGGINGFACE_REPO_ID": "sol-ocf/test-dwd-global",
            },
        ),
        hf_config=HFFileConfig(hf_repo_id="sol-ocf/test-dwd-global"),
        am_config=AssetMaterializationConfig(
            asset_key=["nwp", "icon", "global", "zarr_archive"],
            asset_description="Global ICON Zarr Archive stored in huggingface.",
        ),
    ),
)

icon_global_zarr_archive = dg.external_asset_from_spec(
    dg.AssetSpec(["nwp", "icon", "global", "zarr_archive"]),
)
