"""Defines creation functions for icon assets."""
import dataclasses as dc
import datetime as dt
import os
import time

import dagster as dg
import huggingface_hub as hfh
import kbatch._core as kbc
from huggingface_hub.hf_api import (
    RepoFile,
)
from kbatch._types import Job


@dc.dataclass
class MakeDefinitionsOptions:
    """Typesafe options for the _make_defintions function."""

    area: str
    hf_repo_id: str
    partitions_def: dg.PartitionsDefinition

    def key_prefix(self) -> list[str]:
        """Generate an asset key prefix based on the area."""
        return ["nwp", "icon", self.area]


@dc.dataclass
class MakeDefinitionsOutputs:
    """Typesafe outputs for the _make_definitions function."""

    zarr_asset: dg.AssetsDefinition


def make_definitions(opts: MakeDefinitionsOptions) -> MakeDefinitionsOutputs:
    @dg.asset(
        name="zarr_archive",
        key_prefix=opts.key_prefix(),
        partitions_def=opts.partitions_def,
        auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
        compute_kind="external",
        op_tags={
            "MAX_RUNTIME_SECONDS_TAG": 20 * 60 * 60,
        },
    )
    def zarr_asset(context: dg.AssetExecutionContext) -> dg.AssetsDefinition:
        """A cloud-stored zarr archive of ICON data."""
        execution_start = dt.datetime.now(tz=dt.UTC)

        # Get the inittime as a datetime object from the partition key
        inittime = dt.datetime.strptime(context.partition_key, "%Y-%m-%d|%H:%M").replace(
            tzinfo=dt.UTC,
        )

        context.log.info(f"Creating zarr archive for {inittime.strftime('%Y-%m-%d|%H:%M')}")

        # Set the kbatch url and token arguments to none in all calls to kbatch
        # * Don't ask me why, but setting them their values using the environment
        #   variables doesn't work, so let kbatch do that instead
        kbatch_dict = {
            "kbatch_url": None,
            "token": None,
        }

        # Create a kbatch job to consume the data.
        # * This spins up a single pod in a job (deployment) on the planetary computers cluster.
        job = Job(
            name=f"icon-backfill",
            image="gchr.io/openclimatefix/nwp-consumer:0.2.0",
            args=[
                "consume",
                "--source=icon",
                "--sink=huggingface",
                "--rdir=raw",
                "--zdir=data",
                f"--from={inittime.strftime('%Y-%m-%dT%H%M')}",
            ],
            env={
                "ICON_MODEL": opts.area,
                "ICON_PARAMETER_GROUP": "basic",  # TODO: change to "full"
                "ICON_HOURS": "3",  # TODO: remove
                "HUGGINGFACE_TOKEN": os.environ["HUGGINGFACE_TOKEN"],
                "HUGGINGFACE_REPO_ID": opts.hf_repo_id,
            },
        )
        context.log.info("Submitting job to kbatch.")
        result = kbc.submit_job(job=job, **kbatch_dict)

        # Extract the job and pod names from the result
        job_name: str = result["metadata"]["name"]
        pod_name: str = kbc.list_pods(job_name=job_name, **kbatch_dict)["items"][0]["metadata"]["name"]

        context.log.info(f"Kbatch job {job_name} created running with pod {pod_name}.")

        # Wait for the job to complete
        timeout: int = 60 * 60 * 4  # 4 hours
        time_spent: int = 0
        result = {}
        while time_spent < timeout:
            result = kbc.show_job(resource_name=job_name, **kbatch_dict)
            active = result["status"]["active"]
            if active is None:
                break
            time.sleep(10)
            time_spent += 10
            if time_spent % 5 * 60 * 60 == 0:
                context.log.info(f"Kbatch job {job_name} still running after {int(time_spent / 60)} minutes.")

        # Write out the logs to stdout
        for log in kbc._logs(pod_name=pod_name, stream=True, read_timeout=60, **kbatch_dict):
            print(log)

        # Delete the job
        kbc.delete_job(resource_name=job_name, **kbatch_dict)

        # Check what has been created
        if result["status"]["failed"] is not None:
            raise Exception("Job failed, see logs.")

        if result["status"]["succeeded"] is not None:
            api = hfh.HfApi(token=os.environ["HUGGINGFACE_TOKEN"])
            files: list[RepoFile] = [
                p
                for p in api.list_repo_tree(
                    repo_id=opts.hf_repo_id,
                    path_in_repo=f"data/{inittime.strftime('%Y/%m/%d')}",
                )
                if isinstance(p, RepoFile)
            ]

            if len(files) == 0:
                raise Exception("No files found in the repo.")

            # Get the file that corresponds to the given init time
            inittime_fileinfos: list[RepoFile] = [
                rf for rf in files if (f"{inittime.strftime('%Y%m%dT%H%M')}" in rf.path)
            ]
            if len(inittime_fileinfos) == 0:
                raise Exception("No files found in the repo for the given init time.")

            fileinfo: RepoFile = next(iter(inittime_fileinfos))

            elapsed_time = dt.datetime.now(tz=dt.UTC) - execution_start

            yield dg.Output(
                value=fileinfo.path,
                metadata={
                    "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
                    "file_path": dg.MetadataValue.text(fileinfo.path),
                    "partition_size": dg.MetadataValue.int(fileinfo.size),
                    "area": dg.MetadataValue.text(opts.area),
                    "elapsed_time_mins": dg.MetadataValue.float(
                        elapsed_time / dt.timedelta(minutes=1),
                    ),
                },
            )

    return MakeDefinitionsOutputs(
        zarr_asset=zarr_asset,
    )
