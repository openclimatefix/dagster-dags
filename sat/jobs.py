import pandas as pd
from dagster import AssetSelection, define_asset_job, EnvVar

from sat.assets.eumetsat.common import EumetsatConfig

base_path = "/mnt/storage_c/IODC/"



asset_jobs = []
asset_job = define_asset_job(f"download_iodc_raw_files", AssetSelection.all(),
                             config={
                                 'ops': {"download_eumetsat_iodc_data": {"config": EumetsatConfig(api_key=EnvVar("EUMETSAT_API_KEY"),
                                                                                                  api_secret=EnvVar("EUMETSAT_API_SECRET"),
                                                                                                  data_dir=base_path,
                                                                                                  start_date="2017-02-01",
                                                                                                  end_date=str(pd.Timestamp().utcnow())).to_fields_dict()},},})

asset_jobs.append(asset_job)
