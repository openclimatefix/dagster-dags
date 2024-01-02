"""Defines the jobs for the ECMWF data pipeline."""
import datetime as dt
import os
import pathlib

import dagster as dg
import ocf_blosc2  # noqa
import xarray as xr
from nwp_consumer.internal import (
    IT_FOLDER_FMTSTR,
    IT_FOLDER_GLOBSTR,
)

from constants import LOCATIONS_BY_ENVIRONMENT

env = os.getenv("ENVIRONMENT", "local")
RAW_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].RAW_FOLDER
ZARR_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].NWP_ZARR_FOLDER


class ValidateExistingFilesConfig(dg.Config):
    """Config schema for the validate_existing_files job."""

    base_path: str
    source: str
    area: str
    asset_name: str

    def check(self) -> None:
        """Check that the source and area are valid."""
        if self.area not in ["global", "eu", "uk", "nw_india", "malta"]:
            raise ValueError(f"Area {self.area} not recognised.")

        if self.source not in ["ecmwf", "icon", "ceda", "cams"]:
            raise ValueError(f"Source {self.source} not recognised.")

        if self.archive_path().exists() is False:
            raise FileNotFoundError(
                f"Could not find archive folder {self.archive_path().as_posix()}",
            )

    def archive_path(self) -> pathlib.Path:
        """Return the base path of the archive."""
        return pathlib.Path(self.base_path) / "nwp" / self.source / self.area


@dg.op
def validate_existing_raw_files(
    context: dg.OpExecutionContext,
    config: ValidateExistingFilesConfig,
) -> None:
    """Checks for existing raw files.

    The folder structure of the raw files is assumed to follw the convention
    from the nwp-consumer library. That is to say, the files are stored in
    folders named after the inittime, which are in turn stored in folders
    named after the area and source. See README.md for more details.
    """
    config.check()

    total_archive_size_bytes: int = 0
    for it_folder in [f for f in config.archive_path().glob(IT_FOLDER_GLOBSTR) if f.suffix == ""]:
        # Parse the folder as an inittime:
        try:
            it = dt.datetime.strptime(
                it_folder.relative_to(config.archive_path()).as_posix(),
                IT_FOLDER_FMTSTR,
            ).replace(tzinfo=dt.UTC)
        except ValueError:
            continue

        # For every file in the inittime folder with the correct extension,
        # create an AssetObservation for the relevant partition
        sizes: list[int] = []
        it_filepaths: list[pathlib.Path] = []
        for file in list(it_folder.glob("*.nc")) + list(it_folder.glob("*.grib")):
            it_filepaths.append(file)
            sizes.append(file.stat().st_size)

        total_archive_size_bytes += sum(sizes)

        if len(it_filepaths) > 0:
            context.log_event(
                dg.AssetObservation(
                    asset_key=["nwp", config.source, config.area, config.asset_name],
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
                        "area": dg.MetadataValue.text(config.area),
                        "last_checked": dg.MetadataValue.text(
                            dt.datetime.now(tz=dt.UTC).isoformat(),
                        ),
                    },
                ),
            )

    context.log_event(
        dg.AssetObservation(
            asset_key=["nwp", config.source, config.area, config.asset_name],
            metadata={
                "archive_folder": dg.MetadataValue.text(config.archive_path().as_posix()),
                "area": dg.MetadataValue.text(config.area),
                "total_archive_size_gb": dg.MetadataValue.float(total_archive_size_bytes / 1e9),
                "last_scan": dg.MetadataValue.text(dt.datetime.now(tz=dt.UTC).isoformat()),
            },
        ),
    )


@dg.op
def validate_existing_zarr_files(
    context: dg.OpExecutionContext,
    config: ValidateExistingFilesConfig,
) -> None:
    """Checks for existing zarr files."""
    config.check()

    total_archive_size_bytes: int = 0
    for file in config.archive_path().glob("*.zarr.zip"):
        # Try to parse the init time from the filename
        try:
            it = dt.datetime.strptime(
                file.name,
                "%Y%m%dT%H%M.zarr.zip",
            ).replace(tzinfo=dt.UTC)
        except ValueError:
            continue

        total_archive_size_bytes += file.stat().st_size

        ds = xr.open_zarr("zip::" + file.as_posix())

        # Create an AssetObservation for the relevant partition
        context.log_event(
            dg.AssetObservation(
                asset_key=config.asset_key,
                partition=it.strftime("%Y-%m-%d|%H:%M"),
                metadata={
                    "inittime": dg.MetadataValue.text(it.strftime("%Y-%m-%d|%H:%M")),
                    "dataset": dg.MetadataValue.md(str(ds)),
                },
            ),
        )

    context.log_event(
        dg.AssetObservation(
            asset_key=["nwp", config.source, config.area, config.asset_name],
            metadata={
                "archive_folder": dg.MetadataValue.text(config.archive_path().as_posix()),
                "area": dg.MetadataValue.text(config.area),
                "total_archive_size_gb": dg.MetadataValue.float(total_archive_size_bytes / 1e9),
                "last_scan": dg.MetadataValue.text(dt.datetime.now(tz=dt.UTC).isoformat()),
            },
        ),
    )

    return None


@dg.job(
    name="scan_nwp_raw_archive",
    config=dg.RunConfig(
        ops={
            validate_existing_raw_files.__name__: ValidateExistingFilesConfig(
                base_path=RAW_FOLDER,
                source="ecmwf",
                area="uk",
                asset_name="raw_archive",
            ),
        },
    ),
)
def scan_nwp_raw_archive() -> None:
    """Scan the raw NWP archive for existing files.

    This assumes a folder structure as follows:
    >>> {base_path}/nwp/{source}/{area}/{YYYY}/{MM}/{DD}/{HHMM}/{file}

    where the time values pertain to the init time.
    The values `nwp`, `source``` and `area`
    are taken from the asset key.
    """
    validate_existing_raw_files()


@dg.job(
    name="scan_nwp_zarr_archive",
    config=dg.RunConfig(
        ops={
            validate_existing_zarr_files.__name__: ValidateExistingFilesConfig(
                base_path=ZARR_FOLDER,
                source="ecmwf",
                area="uk",
                asset_name="zarr_archive",
            ),
        },
    ),
)
def scan_nwp_zarr_archive() -> None:
    """Scan the zarr NWP archive for existing files.

    This assumes a folder structure as follows:
    >>> {base_path}/nwp/{source}/{area}/{YYYYMMDD}T{HHMM}.zarr.zip

    where the time values pertain to the init time.
    """
    validate_existing_zarr_files()


def gen_run_config(asset_key: dg.AssetKey) -> dg.RunConfig:
    """Generate a Run config for the validate_existing_files job."""
    vc: ValidateExistingFilesConfig = ValidateExistingFilesConfig(
        base_path=RAW_FOLDER,
        source=asset_key.path[1],
        area=asset_key.path[2],
        asset_name=asset_key.path[3],
    )

    if asset_key.path[-1] == "raw_archive":
        return dg.RunConfig(
            ops={
                validate_existing_raw_files.__name__: vc,
            },
        )
    elif asset_key.path[-1] == "zarr_archive":
        return dg.RunConfig(
            ops={
                validate_existing_zarr_files.__name__: vc,
            },
        )
