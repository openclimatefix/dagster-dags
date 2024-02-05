import dagster as dg
import datetime as dt
import os

from pydantic import Field
from typing import Tuple

from huggingface_hub.hf_api import RepoFile, HfApi
from huggingface_hub import hf_hub_url


class HFFileConfig(dg.Config):
    """Configuration for huggingface.

    Builds upon the dagster Config type, allowing for the configuration to be
    passed to an Op in a dagster pipeline.

    Default values of an ellipsis (...) are used to indicate that the value
    must be provided when the configuration object is instantiated.
    """

    hf_repo_id: str = Field(
        description="The id of the huggingface repo to archive to.",
        default=...,
    )
    file_init_time: str = Field(
        description="The initialisation time of the data of interest.",
        default=dt.datetime.now(dt.UTC).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).strftime("%Y-%m-%d|%H:%M"),
    )


@dg.op(
    ins={"depends_on": dg.In(dg.Nothing)},
    out={
        "file_metadata": dg.Out(RepoFile, is_required=False),
        "no_such_file": dg.Out(dg.Nothing, is_required=False),
    },
)
def get_hf_zarr_file_metadata(
        context: dg.OpExecutionContext,
        config: HFFileConfig,
) -> Tuple[dict[str, dg.MetadataValue], dg.Nothing]:
    """Dagster op to get metadata for a zarr file in a huggingface dataset.

    Assumes the zarr files are stored in a folder structure of the form:
        data/{year}/{month}/{day}
    and that the names of the zarr files contain the initialisation time:
        {year}{month}{day}T{hour}{minute}.zarr.zip
    where the time parts correspond to the initialisation time of the file.

    Defines a "Nothing" input to allow for the op to have upstream dependencies
    in a graph without the passing of data, as well as two outputs,
        - file_metadata: The metadata for the zarr file that was found.
        - no_such_file: A signal that no file was found for the given init time.
    This is done instead of simply raising an error when no files are found,
    as it allows for a branching configuration: downstream Ops can decide
    how to handle the case where either one or none files are found.

    Args:
        context: The dagster context.
        config: Configuration for where to look on huggingface.
    Returns:
        Either the metadata for the zarr file that was found, or a signal that
        no file was found for the given init time.
    """
    # Get the init time from the config or the partition key
    itstring: str = config.file_init_time
    if context.has_partition_key:
        itstring = context.partition_key
        it: dt.datetime = dt.datetime.strptime(itstring, "%Y-%m-%d|%H:%M").replace(tzinfo=dt.UTC)

    api = HfApi(token=os.environ["HUGGINGFACE_TOKEN"])
    # Check if there is an init time folder
    if len(api.get_paths_info(
            repo_id=config.hf_repo_id,
            repo_type="dataset",
            paths=f"data/{it.strftime('%Y/%m/%d')}",
    )) == 0:
        files: list[RepoFile] = []
    else:
        # List all files in the repo folder for the given init time's date
        # and filter for zarr files named according to the init time
        files: list[RepoFile] = [
            p
            for p in api.list_repo_tree(
                repo_id=config.hf_repo_id,
                repo_type="dataset",
                path_in_repo=f"data/{it.strftime('%Y/%m/%d')}",
            )
            if isinstance(p, RepoFile)
               and p.path.endswith(".zarr.zip")
               and f"{it.strftime('%Y%m%dT%H%M')}" in p.path
        ]

    if len(files) == 0:
        context.log.info("No files found in the repo for the given init time.")
        yield dg.Output(dg.Nothing(), "no_such_file")
    else:
        rf: RepoFile = next(iter(files))
        context.log.info(f"Found file {rf} in repo {config.hf_repo_id}.")
        # Map RepoFile object to a dagster metadata dict
        metadata: dict[str, dg.MetadataValue] = {
            "file": dg.MetadataValue.path(rf.path),
            "url": dg.MetadataValue.url(
                hf_hub_url(repo_id=config.hf_repo_id, repo_type="dataset", filename=rf.path)
            ),
            "size (bytes)": dg.MetadataValue.int(rf.size),
            "blob ID": dg.MetadataValue.text(rf.blob_id),
        }
        yield dg.Output(metadata, "file_metadata")
