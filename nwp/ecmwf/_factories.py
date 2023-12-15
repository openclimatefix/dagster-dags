import dataclasses as dc
import datetime as dt
import pathlib
import shutil

import dagster as dg
import numpy as np
import xarray as xr
from nwp_consumer.internal import IT_FOLDER_FMTSTR, FetcherInterface, FileInfoModel

from constants import LOCATIONS_BY_ENVIRONMENT

RAW_FOLDER = LOCATIONS_BY_ENVIRONMENT["local"].RAW_FOLDER

ecmwf_partitions = dg.MultiPartitionsDefinition(
    {
        "date": dg.DailyPartitionsDefinition(start_date="2017-01-01"),
        "inittime": dg.StaticPartitionsDefinition(["00:00", "12:00"]),
    },
)

def map_partition_to_time(context: dg.AssetExecutionContext) -> dt.datetime:
    """Map a partition key to a datetime."""
    if type(context.partition_key) == dg.MultiPartitionKey:
        partkeys = context.partition_key.keys_by_dimension
        return dt.datetime.strptime(
            f"{partkeys['date']}|{partkeys['inittime']}",
            "%Y-%m-%d|%H:%M",
        ).replace(tzinfo=dt.UTC)
    else:
        raise ValueError(
            f"Partition key must be of type MultiPartitionKey, not {type(context.partition_key)}"
        )


@dc.dataclass
class MakeAssetDefinitionsOptions:
    """Typesafe options for the make_asset_definitions function."""

    area: str
    fetcher: FetcherInterface
    partitions: dg.PartitionsDefinition = ecmwf_partitions

    def key_prefix(self) -> list[str]:
        """Generate an asset key prefix based on the area.

        The prefix is important as it defines the folder structure under which
        assets are stored.
        """
        return ["nwp", "ecmwf", self.area]


def make_asset_definitions(
    opts: MakeAssetDefinitionsOptions,
) -> tuple[dg.AssetsDefinition, dg.AssetsDefinition, dg.AssetsDefinition]:
    """Generate the assets for an ECMWF dataset."""


    @dg.asset(
        name="source_archive",
        key_prefix=opts.key_prefix(),
        partitions_def=opts.partitions,
        compute_kind="network_request",
        op_tags={"MAX_RUNTIME_SECONDS_TAG": 1000},
    )
    def _ecmwf_source_archive(
        context: dg.AssetExecutionContext,
    ) -> dg.Output[list[FileInfoModel]]:
        """Asset detailing all wanted remote files from ECMWF."""
        execution_start = dt.datetime.now(tz=dt.UTC)

        # List all files for this partition
        it = map_partition_to_time(context=context)
        fileinfos = opts.fetcher.listRawFilesForInitTime(it=it)

        elapsed_time = dt.datetime.now(tz=dt.UTC) - execution_start

        if len(fileinfos) == 0:
            raise ValueError("No files found for this partition. See error logs.")

        return dg.Output(
            fileinfos,
            metadata={
                "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
                "num_files": dg.MetadataValue.int(len(fileinfos)),
                "file_names": dg.MetadataValue.text(str([f.filename() for f in fileinfos])),
                "elapsed_time_mins": dg.MetadataValue.float(elapsed_time / dt.timedelta(minutes=1)),
            },
        )


    @dg.asset(
        name="raw_archive",
        key_prefix=opts.key_prefix(),
        partitions_def=opts.partitions,
        ins={"fis": dg.AssetIn(key=_ecmwf_source_archive.key)},
        check_specs=[
            dg.AssetCheckSpec(
                name="num_local_is_num_remote",
                asset=[*opts.key_prefix(), "raw_archive"]
            ),
            dg.AssetCheckSpec(
                name="nonzero_local_size",
                asset=[*opts.key_prefix(), "raw_archive"]
            ),
        ],
        metadata={
            "archive_folder": dg.MetadataValue.text(f"{RAW_FOLDER}/{'/'.join(opts.key_prefix())}"),
            "area": dg.MetadataValue.text(opts.area),
        },
        compute_kind="download",
        op_tags={"MAX_RUNTIME_SECONDS_TAG": 1000},
    )
    def _ecmwf_raw_archive(
        context: dg.AssetExecutionContext,
        fis: list[FileInfoModel],
    ) -> dg.Output[list[pathlib.Path]]:
        """Locally stored archive of raw data from ECMWF."""
        execution_start = dt.datetime.now(tz=dt.UTC)

        # For each file in the remote archive, download and store it
        stored_paths: list[pathlib.Path] = []
        sizes: list[int] = []
        # Store the file based on the asset key prefix and the init time of the file
        loc = "/".join(context.asset_key.path[:-1])
        for fi in fis:
            dst = pathlib.Path(
                f"{RAW_FOLDER}/{loc}/{fi.it().strftime(IT_FOLDER_FMTSTR)}/{fi.filename()}",
            )
            # If the file already exists, delete it
            if dst.exists():
                dst.unlink()
            # Otherwise, download it and store it
            fi, src = opts.fetcher.downloadToTemp(fi=fi)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(src=src, dst=dst)
            src.unlink(missing_ok=True)

            stored_paths.append(dst)
            sizes.append(dst.stat().st_size)

        elapsed_time = dt.datetime.now(tz=dt.UTC) - execution_start

        yield dg.Output(
            stored_paths,
            metadata={
                "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
                "num_files": dg.MetadataValue.int(len(stored_paths)),
                "file_paths": dg.MetadataValue.text(str([f.as_posix() for f in stored_paths])),
                "partition_size": dg.MetadataValue.int(sum(sizes)),
                "area": dg.MetadataValue.text(opts.area),
                "elapsed_time_mins": dg.MetadataValue.float(elapsed_time / dt.timedelta(minutes=1)),
            },
        )

        # Perform the checks defined in the check_specs above
        yield dg.AssetCheckResult(
            check_name="num_local_is_num_remote",
            passed=bool(len(stored_paths) == len(fis)),
        )
        yield dg.AssetCheckResult(
            check_name="nonzero_local_size",
            passed=bool(np.all(sizes)),
        )

    @dg.asset(
        name="zarr_archive",
        key_prefix=opts.key_prefix(),
        partitions_def=opts.partitions,
        ins={"raw_paths": dg.AssetIn(key=_ecmwf_raw_archive.key)},
        io_manager_key="xr_zarr_io",
        compute_kind="process",
        op_tags={"MAX_RUNTIME_SECONDS_TAG": 1000},
    )
    def _ecmwf_zarr_archive(
        context: dg.AssetExecutionContext,
        raw_paths: list[pathlib.Path],
    ) -> dg.Output[xr.Dataset]:
        """Locally stored archive of zarr-formatted xarray data from ECMWF."""
        execution_start = dt.datetime.now(tz=dt.UTC)
        # Convert each file to an xarray dataset and merge
        datasets: list[xr.Dataset] = []
        for path in raw_paths:
            datasets.append(opts.fetcher.mapTemp(p=path))
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

    return (_ecmwf_source_archive, _ecmwf_raw_archive, _ecmwf_zarr_archive)
