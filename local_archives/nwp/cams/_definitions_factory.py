import dataclasses as dc
import datetime as dt
import os
import pathlib
from typing import Any, Literal

import cdsapi
import dagster as dg
from nwp_consumer.internal import IT_FOLDER_FMTSTR

from constants import LOCATIONS_BY_ENVIRONMENT
from local_archives.partitions import InitTimePartitionsDefinition

from ._ops import ValidateExistingFilesConfig, validate_existing_raw_init_files

env = os.getenv("ENVIRONMENT", "local")
RAW_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].RAW_FOLDER


@dc.dataclass
class VariableSelection:
    """Defines the variables to request from CAMS."""

    slow: list[str] = dc.field(default_factory=list)
    fast: list[str] = dc.field(default_factory=list)
    hours: list[str] = dc.field(default_factory=list)


@dc.dataclass
class MakeDefinitionsOptions:
    """Typesafe options for the make_asset_definitions function."""

    area: str
    file_format: Literal["grib", "netcdf"]
    partitions: InitTimePartitionsDefinition
    client: cdsapi.Client
    multilevel_vars: VariableSelection | None = None
    multilevel_levels: list[str] | None = None
    singlelevel_vars: VariableSelection | None = None

    def key_prefix(self) -> list[str]:
        """Generate an asset key prefix based on the area.

        The prefix is important as it defines the folder structure under which
        assets are stored.
        """
        return ["nwp", "cams", self.area]

    def dataset_name(self) -> str:
        """Generate a dataset name based on the area."""
        match self.area:
            case "eu":
                return "cams-europe-air-quality-forecasts"
            case "global":
                return "cams-global-atmospheric-composition-forecasts"
            case _:
                raise ValueError(f"Area {self.area} not supported")


@dc.dataclass
class CamsFileInfo:
    """Information about a remote file from the CAMS CDS API.

    Mirrors the structure of the cdsapi.api.Result.toJSON() method:
    https://github.com/ecmwf/cdsapi/blob/master/cdsapi/api.py
    Also adds in a field to hold the variable name.
    """

    resultType: str
    contentType: str
    contentLength: int
    location: str
    var: str
    inittime: dt.datetime


@dc.dataclass
class MakeDefinitionsOutputs:
    """Outputs from the make_asset_definitions function."""

    source_asset: dg.AssetsDefinition
    raw_asset: dg.AssetsDefinition
    raw_job: dg.JobDefinition


def make_definitions(
    opts: MakeDefinitionsOptions,
) -> MakeDefinitionsOutputs:
    """Generate the assets for a CAMS datset."""

    @dg.asset(
        name="source_archive",
        key_prefix=opts.key_prefix(),
        partitions_def=opts.partitions,
        compute_kind="network_request",
        op_tags={
            "expected_runtime": "20min",
            "MAX_RUNTIME_SECONDS_TAG": 20 * 60,
        },
    )
    def _cams_source_archive(context: dg.AssetExecutionContext) -> dg.Output[list[CamsFileInfo]]:
        """Asset detailing all wanted remote files from CAMS."""
        execution_start = dt.datetime.now(tz=dt.UTC)

        it = opts.partitions.parse_key(key=context.partition_key)
        # Check if partition is targeting a time more than 30 days old
        # * CAMS data older than 30 days is only available from tape
        # * These variables are slower to collect
        use_slow: bool = False
        if (dt.datetime.now(tz=dt.UTC) - it) > dt.timedelta(days=30):
            use_slow = True

        fis: list[CamsFileInfo] = []

        # First handle single level variables
        if opts.singlelevel_vars is not None:
            for var in opts.singlelevel_vars.slow if use_slow else opts.singlelevel_vars.fast:
                # Build the request
                sl_var_request: dict[str, Any] = {
                    "date": it.strftime("%Y-%m-%d/%Y-%m-%d"),
                    "type": "forecast",
                    "format": opts.file_format,
                    "variable": var,
                    "leadtime_hour": opts.singlelevel_vars.hours,
                    "time": it.strftime("%H:%M"),
                }
                if opts.area == "eu":
                    sl_var_request["model"] = "ensemble"

                result = opts.client.retrieve(
                    name=opts.dataset_name(),
                    request=sl_var_request,
                )
                fis.append(CamsFileInfo(**result.toJSON(), var=var, inittime=it))

        # Then handle multilevel variables
        if opts.multilevel_vars is not None:
            for var in opts.multilevel_vars.slow if use_slow else opts.multilevel_vars.fast:
                # Build the request
                ml_var_request: dict[str, Any] = {
                    "date": it.strftime("%Y-%m-%d/%Y-%m-%d"),
                    "type": "forecast",
                    "format": opts.file_format,
                    "variable": var,
                    "leadtime_hour": opts.multilevel_vars.hours,
                    "time": it.strftime("%H:%M"),
                    "level": opts.multilevel_levels,
                }
                if opts.area == "eu":
                    ml_var_request["model"] = "ensemble"

                result = opts.client.retrieve(
                    name=opts.dataset_name(),
                    request=ml_var_request,
                )
                fis.append(CamsFileInfo(**result.toJSON(), var=var))

        if len(fis) == 0:
            raise Exception(
                "No remote files found for this partition key. See logs for more details.",
            )

        elapsed_time: dt.timedelta = dt.datetime.now(tz=dt.UTC) - execution_start

        return dg.Output(
            fis,
            metadata={
                "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
                "num_files": dg.MetadataValue.int(len(fis)),
                "partition_size": dg.MetadataValue.int(sum([fi.contentLength for fi in fis])),
                "elapsed_time_mins": dg.MetadataValue.float(elapsed_time / dt.timedelta(minutes=1)),
            },
        )

    @dg.asset(
        name="raw_archive",
        key_prefix=opts.key_prefix(),
        partitions_def=opts.partitions,
        ins={"fis": dg.AssetIn(_cams_source_archive.key)},
        compute_kind="download",
        op_tags={
            "expected_runtime": "20min",
            "MAX_RUNTIME_SECONDS_TAG": 20 * 60,
        },
        metadata={
            "archive_folder": dg.MetadataValue.text(f"{RAW_FOLDER}/{'/'.join(opts.key_prefix())}"),
            "area": dg.MetadataValue.text(opts.area),
        },
    )
    def _cams_raw_archive(
        context: dg.AssetExecutionContext,
        fis: list[CamsFileInfo],
    ) -> dg.Output[list[pathlib.Path]]:
        """Locally stored archive of raw data from CAMS."""
        execution_start = dt.datetime.now(tz=dt.UTC)

        stored_paths: list[pathlib.Path] = []
        sizes: list[int] = []
        # Iterate over the variables and their associated result objects from the cdsapi
        for fi in fis:
            # Store the file based on the asset key prefix and the init time of the file
            loc = "/".join(context.asset_key.path[:-1])
            ext = ".grib" if opts.file_format == "grib" else ".nc"
            dst = pathlib.Path(
                f"{RAW_FOLDER}/{loc}/{fi.inittime.strftime(IT_FOLDER_FMTSTR)}/" \
                + f"{fi.inittime.strftime('%Y%m%d%H')}_{fi.var}{ext}"
            )
            # If the file already exists, don't redownload it
            if dst.exists():
                stored_paths.append(dst)
                sizes.append(dst.stat().st_size)
                continue

            # Download the file using the CDS api. Target must be a list even though it
            # is a single file - see the _download method on the Client class
            # https://github.com/ecmwf/cdsapi/blob/master/cdsapi/api.py
            dst.unlink(missing_ok=True)
            stored_path = opts.client._download(fi.__dict__, target=[dst.as_posix()])
            stored_paths.append(pathlib.Path(stored_path))

        if len(stored_paths) == 0:
            raise Exception("No raw files found for this partition key. See logs for more details.")

        elapsed_time: dt.timedelta = dt.datetime.now(tz=dt.UTC) - execution_start

        return dg.Output(
            stored_paths,
            metadata={
                "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
                "num_files": dg.MetadataValue.int(len(stored_paths)),
                "file_locs": dg.MetadataValue.text(str([r.as_posix() for r in stored_paths])),
                "elapsed_time_mins": dg.MetadataValue.float(elapsed_time / dt.timedelta(minutes=1)),
            },
        )

    @dg.job(
        name=f"scan_cams_{opts.area}_raw_archive",
        config=dg.RunConfig(
            ops={
                "validate_existing_raw_init_files": ValidateExistingFilesConfig(
                    base_path=RAW_FOLDER,
                    asset_key=list(_cams_raw_archive.key.path),
                ),
            },
        ),
    )
    def _scan_cams_raw_archive() -> None:
        """Scan the raw archive for existing files."""
        validate_existing_raw_init_files()

    return MakeDefinitionsOutputs(
        source_asset=_cams_source_archive,
        raw_asset=_cams_raw_archive,
        raw_job=_scan_cams_raw_archive,
    )
