from typing import Any

from cloud_archives.kbatch_ops import (
    kbatch_consumer_graph,
)
from cloud_archives.huggingface_ops import (
    get_hf_zarr_file_metadata,
)
import dagster as dg
from pydantic import Field

from huggingface_hub.hf_api import RepoFile


class AssetMaterializationConfig(dg.Config):
    """Configuration for asset materialisation.

    Builds upon the dagster Config type, allowing for the configuration to be
    passed to an Op in a dagster pipeline. Default values of an ellipsis (...)
    are used to indicate that the value must be provided.
    """

    asset_key: list[str] = Field(
        description="The key of the asset to materialise.",
        default=...,
    )
    asset_description: str | None = Field(
        description="A description of the asset.",
        default=None,
    )

@dg.op
def log_asset_materialization(
        context: dg.OpExecutionContext,
        config: AssetMaterializationConfig,
        metadata: dict[str, Any],
) -> None:
    """Materialises an asset according to the config."""

    context.log_event(
        dg.AssetMaterialization(
            asset_key=config.asset_key,
            description=config.asset_description,
            partition=context.partition_key if context.has_partition_key else None,
            metadata=metadata,
        )
    )

@dg.op(
    ins={"depends_on": dg.In(dg.Nothing)},
)
def raise_exception() -> None:
    """Dagster Op that raises an exception.

    This Op is used to mark a branch in a graph as being undesirable.
    Defines a "Nothing" input to allow for the op to have upstream dependencies
    in a graph without the passing of data.
    """
    raise Exception("Reached exception Op.")


@dg.graph
def icon_kbatch_huggingface_graph() -> RepoFile:
    """Op graph for icon archiving to huggingface using kbatch.

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
    job_name: str = kbatch_consumer_graph(no_file_at_start)
    file_metadata, no_file_after_job = get_hf_zarr_file_metadata(job_name)
    # Now the file should exist, so log the materialization
    log_asset_materialization(file_metadata)
    # Raise an exception if it doesn't exist at this point
    raise_exception(no_file_after_job)

    return file_metadata