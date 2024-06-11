import dataclasses as dc
import datetime as dt
import os
import pathlib
from typing import Any, Literal

import cdsapi
import dagster as dg

from constants import LOCATIONS_BY_ENVIRONMENT

env = os.getenv("ENVIRONMENT", "local")
RAW_FOLDER = LOCATIONS_BY_ENVIRONMENT[env].RAW_FOLDER
IT_FOLDER_FMTSTR = "%Y/%m/%d/%H%M"

@dc.dataclass
class VariableSelection:
    """Defines the variables to request from CAMS."""

    # The slow variables are those that are only available from tape
    # The dictionary maps the variable group name to a list of variables
    # to pull within that groups' request. This can be one to [one].
    slow: dict[str, list[str]] = dc.field(default_factory=dict)
    # The fast variables are those that are available from disk
    # The dictionary maps the variable group name to a list of variables
    # to pull within that groups' request. This can be one to [one].
    fast: dict[str, list[str]] = dc.field(default_factory=dict)
    # The hours to pull
    hours: list[str] = dc.field(default_factory=list)

@dc.dataclass
class MakeDefinitionsOptions:
    """Typesafe options for the make_asset_definitions function."""

    area: str
    file_format: Literal["grib", "netcdf"]
    partitions: dg.TimeWindowPartitionsDefinition
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
    Also adds in a field to hold the variable name and inittime.
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

    raw_asset: dg.AssetsDefinition


def make_definitions(
    opts: MakeDefinitionsOptions,
) -> MakeDefinitionsOutputs:
    """Generate the assets for a CAMS datset."""

    @dg.asset(
        name="raw_archive",
        key_prefix=opts.key_prefix(),
        partitions_def=opts.partitions,
        compute_kind="download",
        op_tags={
            "expected_runtime": "5hrs",
            "MAX_RUNTIME_SECONDS_TAG": 20 * 60 * 60,
        },
    )
    def _cams_raw_archive(context: dg.AssetExecutionContext) -> dg.Output[list[pathlib.Path]]:
        """Asset detailing all wanted remote files from CAMS."""
        execution_start = dt.datetime.now(tz=dt.UTC)

        stored_files: list[pathlib.Path] = []
        sizes: list[int] = []

        # Check if partition is targeting a time more than 30 days old
        # * CAMS data older than 30 days is only available from tape
        # * These variables are slower to collect
        it = context.partition_time_window.start
        use_slow: bool = False
        if (dt.datetime.now(tz=dt.UTC) - it) > dt.timedelta(days=30):
            context.log.info(
                f"Partition {context.partition_key} is targeting a time more than 30 days old. "
                + "Pulling variables from tape, this may take a while.",
            )
            use_slow = True

        # First handle single level variables
        if opts.singlelevel_vars is not None:
            for name, varlist in (
                opts.singlelevel_vars.slow if use_slow else opts.singlelevel_vars.fast
            ).items():

                # Create the target file path for the current set of vars
                loc = "/".join(context.asset_key.path[:-1])
                ext = ".grib" if opts.file_format == "grib" else ".nc"
                dst = pathlib.Path(
                    f"{RAW_FOLDER}/{loc}/{it.strftime(IT_FOLDER_FMTSTR)}/"
                    + f"{it.strftime('%Y%m%d%H')}_{name}{ext}",
                )
                # If the file already exists, don't redownload it
                if dst.exists():
                    stored_files.append(dst)
                    sizes.append(dst.stat().st_size)
                    context.log.info(f"File {dst.as_posix()} already exists, skipping", extra={
                        "file": dst.as_posix(),
                        "size": dst.stat().st_size,
                    })
                    continue

                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.touch()

                # Othrwise, build the request
                sl_var_request: dict[str, Any] = {
                    "date": it.strftime("%Y-%m-%d/%Y-%m-%d"),
                    "type": "forecast",
                    "format": opts.file_format,
                    "variable": varlist,
                    "leadtime_hour": opts.singlelevel_vars.hours,
                    "time": it.strftime("%H:%M"),
                }
                if opts.area == "eu":
                    sl_var_request["model"] = "ensemble"

                # Request the file and download it to the target
                context.log.info(f"Reqesting file {dst.as_posix()} from CDS API", extra={
                    "request": sl_var_request,
                    "target": dst.as_posix(),
                })
                result = opts.client.retrieve(
                    name=opts.dataset_name(),
                    request=sl_var_request,
                    target=dst.as_posix(),
                )
                stored_files.append(dst)
                sizes.append(dst.stat().st_size)
                context.log.info(f"File {dst.as_posix()} downloaded from CDS API", extra={
                    "file": dst.as_posix(),
                    "size": dst.stat().st_size,
                })

                # TODO: Split up multi-variables stored files into a single file per variable
                # using grib_filter

        # Then handle multilevel variables
        if opts.multilevel_vars is not None:
            for name, varlist in (
                opts.multilevel_vars.slow if use_slow else opts.multilevel_vars.fast
            ).items():

                # Create the target file path for the current set of vars
                loc = "/".join(context.asset_key.path[:-1])
                ext = ".grib" if opts.file_format == "grib" else ".nc"
                dst = pathlib.Path(
                    f"{RAW_FOLDER}/{loc}/{it.strftime(IT_FOLDER_FMTSTR)}/"
                    + f"{it.strftime('%Y%m%d%H')}_{name}{ext}",
                )

                # If the file already exists, don't redownload it
                if dst.exists():
                    stored_files.append(dst)
                    sizes.append(dst.stat().st_size)
                    context.log.info(f"File {dst.as_posix()} already exists, skipping", extra={
                        "file": dst.as_posix(),
                        "size": dst.stat().st_size,
                    })
                    continue

                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.touch()

                # Othrwise, build the request
                ml_var_request: dict[str, Any] = {
                    "date": it.strftime("%Y-%m-%d/%Y-%m-%d"),
                    "type": "forecast",
                    "format": opts.file_format,
                    "variable": varlist,
                    "leadtime_hour": opts.multilevel_vars.hours,
                    "time": it.strftime("%H:%M"),
                }
                if opts.area == "eu":
                    ml_var_request["level"] = opts.multilevel_levels
                    ml_var_request["model"] = "ensemble"
                else:
                    ml_var_request["pressure_level"] = opts.multilevel_levels

                # Request the file and download it to the target
                context.log.info(f"Reqesting file {dst.as_posix()} from CDS API", extra={
                    "request": ml_var_request,
                    "target": dst.as_posix(),
                })
                result = opts.client.retrieve(
                    name=opts.dataset_name(),
                    request=ml_var_request,
                    target=dst.as_posix(),
                )
                stored_files.append(dst)
                sizes.append(dst.stat().st_size)
                context.log.info(f"File {dst.as_posix()} downloaded from CDS API", extra={
                    "file": dst.as_posix(),
                    "size": dst.stat().st_size,
                })


        if len(stored_files) == 0:
            raise Exception(
                "No remote files found for this partition key. See logs for more details.",
            )

        elapsed_time: dt.timedelta = dt.datetime.now(tz=dt.UTC) - execution_start

        return dg.Output(
            stored_files,
            metadata={
                "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
                "num_files": dg.MetadataValue.int(len(stored_files)),
                "partition_size": dg.MetadataValue.int(sum(sizes)),
                "elapsed_time_mins": dg.MetadataValue.float(elapsed_time / dt.timedelta(minutes=1)),
            },
        )

    return MakeDefinitionsOutputs(
        raw_asset=_cams_raw_archive,
    )
