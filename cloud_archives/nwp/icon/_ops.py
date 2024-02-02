import os

from cloud_archives.kbatch_ops import (
    kbatch_consumer_graph,
)
from cloud_archives.huggingface_ops import (
    get_hf_zarr_file_metadata_for_partition,
)
import dagster as dg

from huggingface_hub.hf_api import RepoFile


@dg.graph
def icon_kbatch_huggingface_graph() -> RepoFile:
    """Op graph for icon archiving to huggingface using kbatch.

    Note: Some of the ops within the graphs require configuration.

    Returns:
        The file metadata for the zarr file that was archived.
    """
    job_name: str = kbatch_consumer_graph()
    file_metadata: RepoFile = get_hf_zarr_file_metadata_for_partition(job_name)

    return file_metadata