import dagster as dg
import datetime as dt
from nwp_consumer.internal import FileInfoModel, IT_FOLDER_FMTSTR, FetcherInterface
import pathlib
import numpy as np
import shutil
import dataclasses as dc

from constants import LOCATIONS_BY_ENVIRONMENT


RAW_FOLDER = LOCATIONS_BY_ENVIRONMENT["local"].RAW_FOLDER

ecmwf_partitions = dg.MultiPartitionsDefinition({
    "date": dg.DailyPartitionsDefinition(start_date="2017-01-01"),
    "inittime": dg.StaticPartitionsDefinition(["00:00", "12:00"]),
})

def map_partition_to_time(context: dg.AssetExecutionContext) -> dt.datetime:
    """Map a partition key to a datetime."""
    partkeys = context.partition_key.keys_by_dimension
    return dt.datetime.strptime(
        f"{partkeys['date']}|{partkeys['inittime']}", "%Y-%m-%d|%H:%M"
    ).replace(tzinfo=dt.UTC)


@ dc.dataclass
class MakeAssetDefinitionsOptions:
    """Typesafe options for the make_asset_definitions function."""

    area: str
    fetcher: FetcherInterface
    partitions: dg.PartitionsDefinition = ecmwf_partitions
    key_prefix: list[str] = ["nwp", "ecmwf", area]


def make_asset_definitions(opts: MakeAssetDefinitionsOptions) \
        -> tuple[dg.AssetsDefinition, dg.AssetsDefinition, dg.AssetsDefinition]:
    """Generate the assets for an ECMWF dataset."""

    @dg.asset(
        name="source_archive",
        key_prefix=opts.key_prefix,
        partitions_def=opts.partitions,
        check_specs=[
            dg.AssetCheckSpec(
                name="nonzero_num_files",
                asset=key_prefix.append('source_archive')
            )
        ],
        compute_kind="network_request",
        op_tags={"MAX_RUNTIME_SECONDS_TAG": 1000}
    )
    def _ecmwf_source_archive(
            context: dg.AssetExecutionContext
    ) -> dg.Output[list[FileInfoModel]]:
        """Asset detailing all wanted remote files from ECMWF."""
        # List all files for this partition
        it = map_partition_to_time(context=context)
        fileinfos = opts.fetcher.listRawFilesForInitTime(it=it)

        yield dg.Output(fileinfos, metadata={
            "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
            "num_files": dg.MetadataValue.int(len(fileinfos)),
            "file_names": dg.MetadataValue.text(str([f.filename() for f in fileinfos])),
        })

        yield dg.AssetCheckResult(
            check_name="nonzero_num_files",
            passed=bool(len(fileinfos) > 0),
            metadata={"num_files": dg.MetadataValue.int(len(fileinfos))},
        )

    @dg.asset(
        name="raw_archive",
        key_prefix=opts.key_prefix,
        partitions_def=opts.partitions,
        check_specs=[
            dg.AssetCheckSpec(
                name="num_local_is_num_remote",
                asset=key_prefix.append('raw_archive')
            ),
            dg.AssetCheckSpec(
                name="nonzero_local_size",
                asset=key_prefix.append('raw_archive')
            ),
        ],
        metadata={
            "archive_folder": dg.MetadataValue.text(f"{RAW_FOLDER}/nwp/ecmwf/{opts.area}"),
            "area": dg.MetadataValue.text(opts.area),
        },
        compute_kind="download",
        op_tags={"MAX_RUNTIME_SECONDS_TAG": 1000}
    )
    def _ecmwf_raw_archive(
        context: dg.AssetExecutionContext,
        source_archive: list[FileInfoModel]
    ) -> dg.Output[list[pathlib.Path]]:
        """Locally stored archive of raw data from ECMWF."""
        # For each file in the remote archive, download and store it
        stored_paths: list[pathlib.Path] = []
        sizes: list[int] = []
        # Store the file based on the asset key prefix and the init time of the file
        loc = "/".join(context.asset_key.path[:-1])
        for fi in source_archive:
            dst = pathlib.Path(f"{RAW_FOLDER}/{loc}/{fi.it().strftime(IT_FOLDER_FMTSTR)}/{fi.filename()}")
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

        yield dg.Output(stored_paths, metadata={
            "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
            "num_files": dg.MetadataValue.int(len(stored_paths)),
            "file_paths": dg.MetadataValue.text(str([f.as_posix() for f in stored_paths])),
            "partition_size": dg.MetadataValue.int(sum(sizes)),
            "area": dg.MetadataValue.text(opts.area),
        })

        # Perform the checks defined in the check_specs above
        yield dg.AssetCheckResult(
            check_name="num_local_is_num_remote",
            passed=bool(len(stored_paths) == len(source_archive)),
        )
        yield dg.AssetCheckResult(
            check_name="nonzero_local_size",
            passed=bool(np.all(sizes)),
        )

    @dg.asset(
        name="zarr_archive",
        key_prefix=opts.key_prefix,
        partitions_def=opts.partitions,
        io_manager_key="xr_zarr_io",
        compute_kind="process",
        op_tags={"MAX_RUNTIME_SECONDS_TAG": 1000}
    )
    def _ecmwf_zarr_archive(
        context: dg.AssetExecutionContext,
        raw_archive: list[pathlib.Path],
    ) -> dg.Output[xr.Dataset]:
        """Local zarr archive asset."""
        # Convert each file to an xarray dataset and merge
        datasets: list[xr.dataset] = []
        for path in raw_archive:
            datasets.append(opts.fetcher.mapTemp(p=path))
        ds = xr.merge(datasets, combine_attrs="drop_conflicts")

        yield dg.Output(ds, metadata={
            "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
            "dataset": dg.MetadataValue.md(str(ds)),
        })

    return [_ecmwf_source_archive, _ecmwf_raw_archive, _ecmwf_zarr_archive]

