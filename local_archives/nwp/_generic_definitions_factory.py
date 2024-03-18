"""Defines a factory for creating nwp-consumer-backed assets and jobs."""

import dataclasses as dc
import datetime as dt
import os
import pathlib
import shutil
from typing import Literal

import dagster as dg
import numpy as np
import xarray as xr
from nwp_consumer.internal import IT_FOLDER_STRUCTURE_RAW, FetcherInterface, FileInfoModel

from constants import LOCATIONS_BY_ENVIRONMENT
from local_archives.partitions import InitTimePartitionsDefinition

env = os.getenv("ENVIRONMENT", "local")
RAW_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].RAW_FOLDER
ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].NWP_ZARR_FOLDER


@dc.dataclass
class MakeDefinitionsOptions:
    """Typesafe options for the make_asset_definitions function."""

    area: str
    fetcher: FetcherInterface
    source: Literal["ecmwf", "ceda", "cams"]
    partitions: InitTimePartitionsDefinition

    def key_prefix(self) -> list[str]:
        """Generate an asset key prefix based on the area.

        The prefix is important as it defines the folder structure under which
        assets are stored.
        """
        return ["nwp", self.source, self.area]


@dc.dataclass
class MakeDefinitionsOutputs:
    """Typesafe outputs for the make_definitions function."""

    raw_asset: dg.AssetsDefinition
    zarr_asset: dg.AssetsDefinition


def make_definitions(
    opts: MakeDefinitionsOptions,
) -> MakeDefinitionsOutputs:
    """Generates assets and associated jobs for NWP-consumer data."""

    # The Raw Archive asset has the following properties:
    # * Key Prefix: nwp/{source}/{area} - defines part of the storage folder structure
    # * Auto Materialize Policy: Eagerly materialize the asset when the raw archive is updated
    # ** This is checked on a cron schedule every tuesday and saturday at midnight, and up
    # ** to 10 materializations are allowed per check.
    # * Partitions: Defines the partitioning scheme for the asset
    # * Check Specs: Defines the checks that should be performed on the asset
    @dg.asset(
        name="raw_archive",
        key_prefix=opts.key_prefix(),
        auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
        partitions_def=opts.partitions,
        check_specs=[
            dg.AssetCheckSpec(
                name="num_local_is_num_remote",
                asset=[*opts.key_prefix(), "raw_archive"],
            ),
            dg.AssetCheckSpec(name="nonzero_local_size", asset=[*opts.key_prefix(), "raw_archive"]),
        ],
        metadata={
            "archive_folder": dg.MetadataValue.text(f"{RAW_FOLDER}/{'/'.join(opts.key_prefix())}"),
            "area": dg.MetadataValue.text(opts.area),
            "source": dg.MetadataValue.text(opts.source),
        },
        compute_kind="download",
        op_tags={"MAX_RUNTIME_SECONDS_TAG": int(60 * 100)},
    )
    def _raw_archive(
        context: dg.AssetExecutionContext,
    ) -> dg.Output[list[pathlib.Path]]:
        """Locally stored archive of raw data."""
        execution_start = dt.datetime.now(tz=dt.UTC)

        # List all available source files for this partition
        it = opts.partitions.parse_key(key=context.partition_key)
        context.log.info(
            f"Listing files for init time {it.strftime('%Y-%m-%d %H:%M')} from {opts.source}.",
        )
        fileinfos: list[FileInfoModel] = opts.fetcher.listRawFilesForInitTime(it=it)

        if len(fileinfos) == 0:
            raise ValueError("No files found for this partition. See error logs.")

        context.log.info(f"Found {len(fileinfos)} files for this partition.")

        # For each file in the remote archive, download and store it
        stored_paths: list[pathlib.Path] = []
        sizes: list[int] = []

        # Store the file based on the asset key prefix and the init time of the file
        loc = "/".join(context.asset_key.path[:-1])
        for fi in fileinfos:
            dst = pathlib.Path(
                f"{RAW_FOLDER}/{loc}/{fi.it().strftime(IT_FOLDER_STRUCTURE_RAW)}/{fi.filename()}",
            )

            # If the file already exists, don't re download it
            if dst.exists() and dst.stat().st_size > 0:
                context.log.info(
                    f"File {fi.filename()} already exists at {dst.as_posix()}. Skipping download.",
                )
                stored_paths.append(dst)
                sizes.append(dst.stat().st_size)
                continue

            # Otherwise, download it and store it
            if dst.exists() and dst.stat().st_size == 0:
                dst.unlink()
            context.log.info(
                f"Downloading file {fi.filename()} to {dst.as_posix()}",
            )
            # Download to temp fails soft, so we need to check the src
            # to see if it is an empty path.
            fi, src = opts.fetcher.downloadToTemp(fi=fi)
            if src is None or src == pathlib.Path():
                raise ValueError(
                    f"Error downloading file {fi.filename()}. See stdout logs for details.",
                )
            context.log.info(f"Moving file {src.as_posix()} to {dst.as_posix()}")
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(src=src, dst=dst)

            stored_paths.append(dst)
            sizes.append(dst.stat().st_size)

        elapsed_time = dt.datetime.now(tz=dt.UTC) - execution_start

        yield dg.Output(
            stored_paths,
            metadata={
                "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
                "partition_num_files": dg.MetadataValue.int(len(stored_paths)),
                "file_paths": dg.MetadataValue.text(str([f.as_posix() for f in stored_paths])),
                "partition_size": dg.MetadataValue.int(sum(sizes)),
                "area": dg.MetadataValue.text(opts.area),
                "elapsed_time_mins": dg.MetadataValue.float(elapsed_time / dt.timedelta(minutes=1)),
            },
        )

        # Perform the checks defined in the check_specs above
        yield dg.AssetCheckResult(
            check_name="num_local_is_num_remote",
            passed=bool(len(stored_paths) == len(fileinfos)),
        )
        yield dg.AssetCheckResult(
            check_name="nonzero_local_size",
            passed=bool(np.all(sizes)),
        )

    # The Zarr Archive asset has the following properties:
    # * Key Prefix: nwp/{source}/{area} - defines part of the storage folder structure
    # * Auto Materialize Policy: Eagerly materialize the asset when the raw archive is updated
    # * Partitions: Defines the partitioning scheme for the asset
    @dg.asset(
        name="zarr_archive",
        key_prefix=opts.key_prefix(),
        partitions_def=opts.partitions,
        auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
        ins={"raw_paths": dg.AssetIn(key=_raw_archive.key)},
        io_manager_key="nwp_xr_zarr_io",
        compute_kind="process",
        metadata={
            "archive_folder": dg.MetadataValue.text(f"{ZARR_FOLDER}/{'/'.join(opts.key_prefix())}"),
            "area": dg.MetadataValue.text(opts.area),
            "source": dg.MetadataValue.text(opts.source),
        },
        op_tags={"MAX_RUNTIME_SECONDS_TAG": 60 * 5},
    )
    def _zarr_archive(
        context: dg.AssetExecutionContext,
        raw_paths: list[pathlib.Path],
    ) -> dg.Output[xr.Dataset]:
        """Locally stored archive of zarr-formatted xarray data."""
        execution_start = dt.datetime.now(tz=dt.UTC)
        # Convert each file to an xarray dataset and merge
        datasets: list[xr.Dataset] = []
        for path in raw_paths:
            context.log.info(f"Converting raw file at {path.as_posix()} to xarray dataset.")
            datasets.append(opts.fetcher.mapTemp(p=path))
        context.log.info(f"Merging {len(datasets)} datasets into one.")
        ds = xr.merge(datasets, combine_attrs="drop_conflicts")

        elapsed_time = dt.datetime.now(tz=dt.UTC) - execution_start

        return dg.Output(
            ds,
            metadata={
                "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
                "dataset": dg.MetadataValue.md(str(ds)),
                "elapsed_time_mins": dg.MetadataValue.float(elapsed_time / dt.timedelta(minutes=1)),
            },
        )

    return MakeDefinitionsOutputs(
        raw_asset=_raw_archive,
        zarr_asset=_zarr_archive,
    )
