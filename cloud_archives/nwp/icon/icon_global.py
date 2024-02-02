"""Pipeline for the processing of global ICON data."""
import os

from cloud_archives.kbatch_ops import (
    define_kbatch_consumer_job,
    kbatch_consumer_graph,
    NWPConsumerConfig,
    kbatch_job_failure_hook,
)
from cloud_archives.huggingface_ops import (
    get_hf_zarr_file_metadata,
    HFFileConfig,
)
from ._ops import (
    icon_kbatch_huggingface_graph,
    log_asset_materialization,
    AssetMaterializationConfig,
)

import dagster as dg


icon_global_archive_job = icon_kbatch_huggingface_graph.to_job(
    name="icon_global_hf_archive_job",
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m-%d|%H:%M",
        start="2024-01-31|00:00",
        cron_schedule="0 0/12 * * *",
    ),
    config=dg.RunConfig(
        ops={
            kbatch_consumer_graph.name: {
                "ops": {
                    define_kbatch_consumer_job.name: NWPConsumerConfig(
                        source="icon",
                        sink="huggingface",
                        docker_tag="0.2.0",
                        env={
                            "ICON_MODEL": "global",
                            "ICON_PARAMETER_GROUP": "basic",  # TODO: change to "full"
                            "ICON_HOURS": "3",  # TODO: remove
                            "HUGGINGFACE_TOKEN": os.environ["HUGGINGFACE_TOKEN"],
                            "HUGGINGFACE_REPO_ID": "sol-ocf/test-dwd-data",
                        }
                    ),
                }
            },
            get_hf_zarr_file_metadata.name: HFFileConfig(
                hf_repo_id="sol-ocf/test-dwd-global",
            ),
            get_hf_zarr_file_metadata.name + "_2": HFFileConfig(
                hf_repo_id="sol-ocf/test-dwd-global",
            ),
            log_asset_materialization.name: AssetMaterializationConfig(
                asset_key=["nwp", "icon", "global", "zarr_archive"],
                asset_description="Global ICON Zarr Archive stored in huggingface.",
            ),
            log_asset_materialization.name + "_2": AssetMaterializationConfig(
                asset_key=["nwp", "icon", "global", "zarr_archive"],
                asset_description="Global ICON Zarr Archive stored in huggingface.",
            ),
        },
    )
)
