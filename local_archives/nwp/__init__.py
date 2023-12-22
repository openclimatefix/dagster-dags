"""Definitions for the NWP dagster code location."""

import dagster as dg

from . import cams, ceda, ecmwf, jobs

all_assets: list[dg.AssetsDefinition] = [
    *ceda.all_assets,
    *ecmwf.all_assets,
    *cams.all_assets,
]

all_jobs: list[dg.JobDefinition] = [
    jobs.scan_nwp_raw_archive,
    jobs.scan_nwp_zarr_archive,
]

@dg.schedule(
    job=jobs.scan_nwp_raw_archive,
    cron_schedule="0 3 * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def scan_nwp_raw_archives_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """Scan the raw archives.

    Yields a RunRequest for the scan_nwp_raw_archive job for each raw archive.
    """
    raw_assets: list[dg.AssetsDefinition] = [
        a for a in all_assets if "raw_archive" in a.key.path
    ]
    for a in raw_assets:
        yield dg.RunRequest(
            run_key=f"scan_nwp_{a.key.path[1]}_{a.key.path[2]}_{a.key.path[3]}",
            run_config=jobs.gen_run_config(a.key)
        )

@dg.schedule(
    job=jobs.scan_nwp_zarr_archive,
    cron_schedule="15 3 * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def scan_nwp_zarr_archives_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """Scan the zarr archives.

    Yields a RunRequest for the scan_nwp_zarr_archive job for each zarr archive.
    """
    zarr_assets: list[dg.AssetsDefinition] = [
        a for a in all_assets if "zarr_archive" in a.key.path
    ]
    for a in zarr_assets:
        yield dg.RunRequest(
            run_key=f"scan_nwp_{a.key.path[1]}_{a.key.path[2]}_{a.key.path[3]}",
            run_config=jobs.gen_run_config(a.key)
        )

all_schedules: list[dg.ScheduleDefinition] = [
    scan_nwp_raw_archives_schedule,
    scan_nwp_zarr_archives_schedule,
]

