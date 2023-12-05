from dagster import asset  # import the `dagster` library
from . import download_product_range, EumetsatConfig
import pandas as pd

@asset
def download_eumetsat_0_deg_data(config: EumetsatConfig) -> None:
    download_product_range(api_key=config.api_key,
                           api_secret=config.api_secret,
                           data_dir=config.data_dir,
                           product_id="EO:EUM:DAT:MSG:HRSEVIRI",
                           start_date=pd.Timestamp(config.start_date),
                           end_date=pd.Timestamp(config.end_date))
