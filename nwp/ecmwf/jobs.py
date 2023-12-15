import dagster as dg
import datetime as dt
import pathlib

from .ecmwf_uk import (
    ecmwf_uk_raw_archive,
    ecmwf_uk_zarr_archive,
)


class ValidateExistingFilesConfig(dg.Config):
    """Config schema for the validate_existing_files job."""
    base_path: str

@dg.op
def validate_existing_raw_ecmwf_files(config: ValidateExistingFilesConfig) -> None:
    """Checks for existing raw ECMWF files."""

    
    base_path: pathlib.Path = pathlib.Path(config.raw_archive_folder)
    glob_str: str = "/".join(["*"] * len(config.it_folder_fmtstr.split("/")))

    inittimes: set[dt.datetime] = set()
    for folder in [f for f in base_path.glob(glob_str) if f.suffix == ""]:
        # Parse the folder as an inittime:
        try:
            it = dt.datetime.strptime(
                folder.relative_to(base_path).as_posix(),
                config.it_folder_fmtstr
            )
        except ValueError:
            continue

        # For every file in the folder with the correct extension,
        # create an AssetMaterialization for the relevant partition
        sizes: list[int] = []
        it_paths: list[pathlib.Path] = []
        for file in folder.glob(f"*{config.file_extension}"):
            it_paths.append(file)
            sizes.append(file.stat().st_size)

        if len(it_paths) > 0:
            yield dg.AssetMaterialization(
                asset_key=ecmwf_uk_raw_archive.key,
                partition=it.strftime("%Y-%m-%d|%H:%M"),
                metadata={
                    "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
                    "num_files": dg.MetadataValue.int(len(stored_paths)),
                    "file_paths": dg.MetadataValue.text(str([f.as_posix() for f in stored_paths])),
                    "partition_size": dg.MetadataValue.int(sum(sizes)),
                    "elapsed_time_mins": dg.MetadataValue.float(elapsed_time / dt.timedelta(minutes=1)),
                }
            

    

    
