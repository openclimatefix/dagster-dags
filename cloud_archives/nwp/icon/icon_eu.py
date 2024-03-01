"""Pipeline for the processing of eu ICON data."""
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
    create_kbatch_huggingface_graph_config,
    kbatch_huggingface_graph,
)

# Define the ICON europe zarr archive as a source asset
icon_europe_zarr_archive = dg.SourceAsset(
    key=["nwp", "icon", "europe", "zarr_archive"],
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m-%d|%H:%M",
        start="2024-01-31|00:00",
        cron_schedule="0 0/6 * * *",
    ),
)

# Define the job to materialize the ICON europe zarr archive
archive_icon_europe_sl_job = kbatch_huggingface_graph.to_job(
    name="archive_icon_europe_sl_job",
    partitions_def=icon_europe_zarr_archive.partitions_def,
    config=create_kbatch_huggingface_graph_config(
        nwp_config=NWPConsumerConfig(
            source="icon",
            sink="huggingface",
            docker_tag="refactor-service-loop",
            zdir="single-level/data",
            env={
                "ICON_MODEL": "europe",
                "ICON_PARAMETER_GROUP": "single-level",
                "HUGGINGFACE_TOKEN": os.getenv("HUGGINGFACE_TOKEN", default="not-set"),
                "HUGGINGFACE_REPO_ID": "sol-ocf/test-dwd-europe",
            },
        ),
        hf_config=HFFileConfig(hf_repo_id="sol-ocf/test-dwd-europe"),
        am_config=AssetMaterializationConfig(
            asset_key=list(icon_europe_zarr_archive.key.path),
            asset_description="Europe ICON Zarr Archive stored in huggingface.",
        ),
    ),
)


archive_icon_europe_ml_job = kbatch_huggingface_graph.to_job(
    name="archive_icon_europe_ml_job",
    partitions_def=icon_europe_zarr_archive.partitions_def,
    config=create_kbatch_huggingface_graph_config(
        nwp_config=NWPConsumerConfig(
            source="icon",
            sink="huggingface",
            docker_tag="main",
            zdir="multi-level/data",
            env={
                "ICON_MODEL": "europe",
                "ICON_PARAMETER_GROUP": "multi-level",
                "HUGGINGFACE_TOKEN": os.getenv("HUGGINGFACE_TOKEN", default="not-set"),
                "HUGGINGFACE_REPO_ID": "sol-ocf/test-dwd-europe",
            },
        ),
        hf_config=HFFileConfig(hf_repo_id="sol-ocf/test-dwd-europe"),
        am_config=AssetMaterializationConfig(
            asset_key=list(icon_europe_zarr_archive.key.path),
            asset_description="Europe ICON Zarr Archive stored in huggingface.",
        ),
    ),
)



