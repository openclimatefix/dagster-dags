import pandas as pd
from dagster import asset  # import the `dagster` library

from . import EumetsatConfig, download_product_range


@asset
def download_eumetsat_iodc_data(config: EumetsatConfig) -> None:
    download_product_range(api_key=config.api_key,
                           api_secret=config.api_secret,
                           data_dir=config.data_dir,
                           product_id="EO:EUM:DAT:MSG:HRSEVIRI-IODC",
                           start_date=pd.Timestamp(config.start_date),
                           end_date=pd.Timestamp(config.end_date))
