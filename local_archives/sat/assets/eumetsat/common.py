"""
EO:EUM:DAT:MSG:HRSEVIRI-IODC
"""
from satip.eumetsat import DownloadManager, eumetsat_filename_to_datetime
from satip.utils import filter_dataset_ids_on_current_files
import pandas as pd
import os

from dagster import Config


class EumetsatConfig(Config):
    api_key: str
    api_secret: str
    data_dir: str
    start_date: str
    end_date: str

def download_product_range(api_key: str, api_secret: str, data_dir: str, product_id: str, start_date: pd.Timestamp, end_date: pd.Timestamp):
    download_manager = DownloadManager(user_key=api_key, user_secret=api_secret, data_dir=data_dir)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    date_range = pd.date_range(start=start_str,
                               end=end_str,
                               freq="30min")
    filenames_downloaded = []
    for filename in os.listdir(data_dir):
        filenames_downloaded.append(filename.split("/")[-1])
    for date in date_range:
        start_date = pd.Timestamp(date) - pd.Timedelta("1min")
        end_date = pd.Timestamp(date) + pd.Timedelta("1min")
        datasets = download_manager.identify_available_datasets(
            start_date=start_date.tz_localize(None).strftime("%Y-%m-%d-%H-%M-%S"),
            end_date=end_date.tz_localize(None).strftime("%Y-%m-%d-%H-%M-%S"),
        )
        filtered_datasets = []
        for dataset in datasets:
            if dataset["id"] not in filenames_downloaded:
                filtered_datasets.append(dataset)
        datasets = filtered_datasets
        download_manager.download_datasets(datasets, product_id=product_id)
