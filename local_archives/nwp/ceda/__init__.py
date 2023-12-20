import dagster as dg

from . import ceda_uk

uk_assets = dg.load_assets_from_modules(
    modules=[ceda_uk],
    group_name="ceda_uk",
)
uk_jobs = [
    ceda_uk.scan_ceda_uk_raw_archive,
    ceda_uk.scan_ceda_uk_zarr_archive,
]

all_assets: list[dg.AssetsDefinition] = [*uk_assets]
all_jobs: list[dg.JobDefinition] = [*uk_jobs]
