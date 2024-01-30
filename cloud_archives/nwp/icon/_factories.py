"""Defines creation functions for icon assets."""
import dataclasses as dc
import datetime as dt
import json
import os

import dagster as dg
from kbatch._types import Job
from kbatch import V1Job
import kbatch._core as kbc


@dc.dataclass
class MakeDefinitionsOptions:
    """Typesafe options for the _make_defintions function."""

    area: str
    hf_repo_id: str
    partitions: dg.PartitionsDefinition

    def key_prefix(self) -> list[str]:
        """Generate an asset key prefix based on the area."""
        return ["nwp", "icon", self.area]


@dc.dataclass
class MakeDefinitionsOutputs:
    """Typesafe outputs for the _make_definitions function."""

    zarr_asset: dg.AssetDefinition


def _make_definitions(opts: MakeDefinitionsOptions) -> MakeDefinitionsOutputs:
    @dg.asset(
        name="zarr_archive",
        key_prefix=opts.key_prefix(),
        partitions_def=opts.partitions,
        auto_materialize_policy=dg.AutoMaterializationPolicy.eager(),
        compute_kind="external",
        op_tags={
            "MAX_RUNTIME_SECONDS_TAG": 20 * 60 * 60,
        },
    )
    def zarr_asset(context: dg.AssetExecutionContext) -> dg.AssetsDefinition:
        """A cloud-stored zarr archive of ICON data."""
        execution_start = dt.datetime.now(tz=dt.UTC)

        # Create a kbatch job to consume the data
        job = Job(
            name=f"icon-backfill-{context.partition_key}",
            image="gchr.io/openclimatefix/nwp-consumer:0.2.0",
            args=[
                "consume",
                "--source=icon",
                "--sink=huggingface",
                "--rdir=raw",
                "--zdir=data",
                f"--from={context.partition_key}",
            ],
            env={
                "ICON_MODEL": opts.area,
                "ICON_PARAMETER_GROUP": "full",
                "HUGGINGFACE_TOKEN": os.environ["HUGGINGFACE_TOKEN"],
                "HUGGINGFACE_REPO_ID": opts.hf_repo_id,
            },
        )

        result = kbc._submit_job(
            job,
            model=V1Job,
        )
        job_name: str = result["metadata"]["name"]
        pod_name: str = kbc.list_pods(job_name=job_name)["items"][0]["metadata"]["name"]
        logs: list[str] = kbc._logs(pod_name=pod_name, stream=True, read_timeout=60 * 60)

    return MakeDefinitionsOutputs(
        zarr_asset=zarr_asset,
    )
