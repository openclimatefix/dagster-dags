import dagster as dg
import datetime as dt
import os

from huggingface_hub.hf_api import RepoFile, HfApi


class HFConfig(dg.Config):
    """Configuration for huggingface."""

    hf_repo_id: str


@dg.op(
    ins={"requires": dg.In(dg.Nothing)},
)
def get_hf_zarr_file_metadata_for_partition(
        context: dg.OpExecutionContext,
        config: HFConfig,
) -> RepoFile:
    """Get the metadata for the zarr file for the given partition key.

    Args:
        context: The dagster context.
        config: The configuration for the job.
    Returns:
        A dictionary of file paths to file sizes.
    """
    inittime: dt.datetime = dt.datetime.strptime(
        context.partition_key,
        "%Y-%m-%d|%H:%M"
    ).replace(tzinfo=dt.UTC)

    # List zarr files for the day of the inittime
    api = HfApi(token=os.environ["HUGGINGFACE_TOKEN"])
    files: list[RepoFile] = [
        p
        for p in api.list_repo_tree(
            repo_id=config.hf_repo_id,
            repo_type="dataset",
            path_in_repo=f"data/{inittime.strftime('%Y/%m/%d')}",
        )
        if isinstance(p, RepoFile)
    ]

    if len(files) == 0:
        raise Exception(
            f"No files found repo {config.hf_repo_id} for inittime {inittime.strftime('%Y-%m-%d|%H:%M')}."
        )

    # Get the file that corresponds to the given init time
    inittime_fileinfos: list[RepoFile] = [
        rf for rf in files if (f"{inittime.strftime('%Y%m%dT%H%M')}" in rf.path)
    ]
    if len(inittime_fileinfos) == 0:
        raise Exception("No files found in the repo for the given init time.")

    return next(iter(inittime_fileinfos))
