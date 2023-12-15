import dagster as dg

from . import ecmwf_nw_india, ecmwf_uk

uk_assets = dg.load_assets_from_modules(
    modules=[ecmwf_uk],
    group_name="ecmwf_uk",
)
uk_jobs = [
    ecmwf_uk.scan_ecmwf_uk_raw_archive,
    ecmwf_uk.scan_ecmwf_uk_zarr_archive,
]


nw_india_assets = dg.load_assets_from_modules(
    modules=[ecmwf_nw_india],
    group_name="ecmwf_nw_india",
)
nw_india_jobs = [
    ecmwf_nw_india.scan_ecmwf_nw_india_raw_archive,
    ecmwf_nw_india.scan_ecmwf_nw_india_zarr_archive,
]

all_assets: list[dg.AssetsDefinition] = [*uk_assets, *nw_india_assets]
all_jobs: list[dg.JobDefinition] = [*uk_jobs, *nw_india_jobs]
