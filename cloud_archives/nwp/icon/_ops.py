import dagster as dg

from cloud_archives.ops.generic import (
    AssetMaterializationConfig,
    log_asset_materialization,
    raise_exception,
)
from cloud_archives.ops.huggingface import (
    HFFileConfig,
    get_hf_zarr_file_metadata,
)
from cloud_archives.ops.kbatch import (
    NWPConsumerConfig,
    define_kbatch_consumer_job,
    kbatch_consumer_graph,
)


def create_kbatch_huggingface_graph_config(
    nwp_config: NWPConsumerConfig,
    hf_config: HFFileConfig,
    am_config: AssetMaterializationConfig,
) -> dg.RunConfig:
    """Mapping from Config to RunConfig for the corresponding graph.

    Args:
        nwp_config: Configuration for the nwp consumer.
        hf_config: Configuration for huggingface.
        am_config: Configuration for asset materialisation.

    Returns:
        The RunConfig for the graph.
    """
    return dg.RunConfig(
        ops={
            kbatch_consumer_graph.name: {
                "ops": {define_kbatch_consumer_job.name: nwp_config},
            },
            get_hf_zarr_file_metadata.name: hf_config,
            get_hf_zarr_file_metadata.name + "_2": hf_config,
            log_asset_materialization.name: am_config,
            log_asset_materialization.name + "_2": am_config,
        },
    )


@dg.graph
def kbatch_huggingface_graph() -> dict[str, dg.MetadataValue]:
    """Op graph for archiving to huggingface using nwp-consumer in kbatch.

    Note: Some of the ops within the graphs require the defining of
    run configuration.

    Returns:
        The file metadata for the zarr file that was archived.
    """
    # First check to see if the file in question already exists
    file_metadata, no_file_at_start = get_hf_zarr_file_metadata()
    # If the file exists, log the materialization
    log_asset_materialization(file_metadata)
    # If the file does not exist, create a kbatch job to archive it
    job_name = kbatch_consumer_graph(no_file_at_start)
    file_metadata, no_file_after_job = get_hf_zarr_file_metadata(job_name)
    # Now the file should exist, so log the materialization
    log_asset_materialization(file_metadata)
    # Raise an exception if it doesn't exist at this point
    raise_exception(no_file_after_job)

    return file_metadata
