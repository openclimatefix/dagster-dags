import pandas as pd
from dagster import AssetSelection, define_asset_job, EnvVar

from sat.assets.eumetsat.common import EumetsatConfig
from sat.assets import download_eumetsat_iodc_data

base_path = "/mnt/storage_c/IODC/"

config = EumetsatConfig(api_key=EnvVar("EUMETSAT_API_KEY"),
                        api_secret=EnvVar("EUMETSAT_API_SECRET"),
                        data_dir=base_path,
                        start_date="2017-02-01",
                        end_date=pd.Timestamp.utcnow().strftime('%Y-%m-%d'))

asset_jobs = []
asset_job = define_asset_job(f"download_iodc_raw_files", AssetSelection.assets(download_eumetsat_iodc_data),
                             config={
                                 'ops': {"download_eumetsat_iodc_data": {
                                     "config": {"api_key": config.api_key, "api_secret": config.api_secret, "data_dir": config.data_dir,
                                                "start_date": config.start_date, "end_date": config.end_date}}, }, })

asset_jobs.append(asset_job)
