"""Defines the jobs for the ECMWF data pipeline."""

import datetime as dt
import pathlib

import dagster as dg
from nwp_consumer.internal import (
    IT_FOLDER_FMTSTR,
    IT_FOLDER_GLOBSTR,
)


class ValidateExistingFilesConfig(dg.Config):
    """Config schema for the validate_existing_files job."""

    base_path: str
    asset_key: list[str]


# TODO: This is the same as the one in ecmwf - consolidate somewhere sensible
@dg.op
def validate_existing_raw_init_files(
    context: dg.OpExecutionContext,
    config: ValidateExistingFilesConfig,
) -> None:
    """Checks for existing raw files."""
    loc: str = "/".join(config.asset_key[:-1])
    base_path: pathlib.Path = pathlib.Path(config.base_path) / loc

    total_archive_size_bytes: int = 0
    for it_folder in [f for f in base_path.glob(IT_FOLDER_GLOBSTR) if f.suffix == ""]:
        # Parse the folder as an inittime:
        try:
            it = dt.datetime.strptime(
                it_folder.relative_to(base_path).as_posix(),
                IT_FOLDER_FMTSTR,
            ).replace(tzinfo=dt.UTC)
        except ValueError:
            continue

        # For every file in the inittime folder with the correct extension,
        # create an AssetObservation for the relevant partition
        sizes: list[int] = []
        it_filepaths: list[pathlib.Path] = []
        for file in list(it_folder.glob("*.grib")) + list(it_folder.glob("*.nc")):
            it_filepaths.append(file)
            sizes.append(file.stat().st_size)

        total_archive_size_bytes += sum(sizes)

        if len(it_filepaths) > 0:
            context.log_event(
                dg.AssetObservation(
                    asset_key=config.asset_key,
                    partition=it.strftime("%Y-%m-%d|%H:%M"),
                    metadata={
                        "inittime": dg.MetadataValue.text(
                            it.strftime("%Y-%m-%d|%H:%M"),
                        ),
                        "num_files": dg.MetadataValue.int(
                            len(it_filepaths),
                        ),
                        "file_paths": dg.MetadataValue.text(
                            str([f.as_posix() for f in it_filepaths]),
                        ),
                        "partition_size": dg.MetadataValue.int(
                            sum(sizes),
                        ),
                        "area": dg.MetadataValue.text(config.asset_key[-2]),
                        "last_checked": dg.MetadataValue.text(
                            dt.datetime.now(tz=dt.UTC).isoformat(),
                        ),
                    },
                ),
            )

    context.log_event(
        dg.AssetObservation(
            asset_key=config.asset_key,
            metadata={
                "archive_folder": dg.MetadataValue.text(base_path.as_posix()),
                "area": dg.MetadataValue.text(config.asset_key[-2]),
                "total_archive_size_gb": dg.MetadataValue.float(total_archive_size_bytes / 1e9),
                "last_scan": dg.MetadataValue.text(dt.datetime.now(tz=dt.UTC).isoformat()),
            },
        ),
    )
