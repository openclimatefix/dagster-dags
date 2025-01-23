"""NetCDF archive of Atmospheric Quality data from CAMS, covering Europe.

CAMS is Copernicus' Atmospheric Monitoring Service, which provides
forecasts of atmospheric quality.

Sourced via CDS API from Copernicus ADS (https://ads.atmosphere.copernicus.eu).
This asset is updated weekly, and surfaced as a zipped NetCDF file for each week
per variable. It is downloaded using the cdsapi Python package
(https://github.com/ecmwf/cdsapi).
"""

import datetime as dt
import os
import pathlib
from typing import Any

import cdsapi
import dagster as dg

ARCHIVE_FOLDER = "/var/dagster-storage/air/cams-europe"
if os.getenv("ENVIRONMENT", "local") == "leo":
    ARCHIVE_FOLDER = "/mnt/storage_ssd_4tb/air/cams-europe"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.WeeklyPartitionsDefinition(
    start_date="2020-02-08",
    end_offset=-2,
)

@dg.asset(
        name="cams-europe",
        description=__doc__,
        key_prefix=["air"],
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("europe"),
            "source": dg.MetadataValue.text("copernicus-ads"),
            "model": dg.MetadataValue.text("cams"),
            "format": dg.MetadataValue.text("netcdf"),
            "expected_runtime": dg.MetadataValue.text("6 hours"),
        },
        compute_kind="python",
        automation_condition=dg.AutomationCondition.on_cron(
            cron_schedule=partitions_def.get_cron_schedule(
                hour_of_day=7,
            ),
        ),
        tags={
            "dagster/max_runtime": str(60 * 60 * 24 * 4), # Should take about 2 days
            "dagster/priority": "1",
            "dagster/concurrency_key": "copernicus-ads",
        },
    partitions_def=partitions_def,
)
def cams_eu_raw_asset(context: dg.AssetExecutionContext) -> dg.Output[list[pathlib.Path]]:
    """Downloads CAMS Europe air quality forecast data from Copernicus ADS."""
    it_start: dt.datetime = context.partition_time_window.start
    it_end: dt.datetime = context.partition_time_window.end
    execution_start = dt.datetime.now(tz=dt.UTC)
    stored_files: list[pathlib.Path] = []

    variables: list[str] = [
        "alder_pollen",
        "ammonia",
        "birch_pollen",
        "carbon_monoxide",
        "dust",
        "grass_pollen",
        "nitrogen_dioxide",
        "nitrogen_monoxide",
        "non_methane_vocs",
        "olive_pollen",
        "ozone",
        "particulate_matter_10um",
        "particulate_matter_2.5um",
        "peroxyacyl_nitrates",
        "pm10_wildfires",
        "ragweed_pollen",
        "secondary_inorganic_aerosol",
        "sulphur_dioxide",
    ]

    for var in variables:
        dst: pathlib.Path = pathlib.Path(ARCHIVE_FOLDER) \
            / "raw" / f"{it_start:%Y%m%d}-{it_end:%Y%m%d}_{var}.nc.zip"
        dst.parent.mkdir(parents=True, exist_ok=True)

        if dst.exists():
            context.log.info("File already exists, skipping download", extra={
                "file": dst.as_posix(),
            })
            stored_files.append(dst)
            continue

        request: dict[str, Any] = {
            "date": [f"{it_start:%Y-%m-%d}/{it_end:%Y-%m-%d}"],
            "type": ["forecast"],
            "time": ["00:00"],
            "model": ["ensemble"],
            "leadtime_hour": [str(x) for x in range(0, 97)],
            "data_format": ["netcdf_zip"],
            "level": ["0", "50", "250", "500", "1000", "3000", "5000"],
            "variable":  [var],
        }

        context.log.info(
            "Reqesting file from Copernicus ADS via CDS API",
            extra={
                "request": request,
                "target": dst.as_posix(),
            },
        )
        client = cdsapi.Client()
        client.retrieve(
            name="cams-europe-air-quality-forecast",
            request=request,
            target=dst.as_posix(),
        )
        context.log.info(
            f"Downloaded file {dst.as_posix()} from Copernicus ADS via CDS API",
            extra={
                "file": dst.as_posix(),
                "size": dst.stat().st_size,
            },
        )
        stored_files.append(dst)

    if len(stored_files) == 0:
        raise Exception(
            "No remote files found for this partition key. See logs for more details.",
        )

    elapsed_time: dt.timedelta = dt.datetime.now(tz=dt.UTC) - execution_start

    return dg.Output(
        value=stored_files,
        metadata={
            "files": dg.MetadataValue.text(", ".join([f.as_posix() for f in stored_files])),
            "partition_size": dg.MetadataValue.int(sum([f.stat().st_size for f in stored_files])),
            "elapsed_time_hours": dg.MetadataValue.float(elapsed_time / dt.timedelta(hours=1)),
        },
    )

