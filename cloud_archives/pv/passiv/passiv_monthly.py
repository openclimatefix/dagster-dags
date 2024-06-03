""" Get passiv daily data and save to Hugging Face"""

import datetime
import io, os, pytz
import pandas as pd
from huggingface_hub.hf_api import HfApi
import dagster as dg
from huggingface_hub import HfFileSystem

from .filenames import get_daily_hf_file_name, get_monthly_hf_file_name


def get_monthly_passiv_data(start_date: datetime, upload_to_hf: bool = True, overwrite: bool = False):
    """ Get monthly passiv data and save to Hugging Face"""

    # set up HF and check if we have data for that day already
    huggingface_file = get_monthly_hf_file_name(date=start_date)
    fs = HfFileSystem()
    if not overwrite:
        if fs.exists(f'datasets/openclimatefix/uk_pv/{huggingface_file}'):
            print(f"Data already exists for {start_date.date()}")
            return

    # start of the month from datetime
    start_date = start_date.replace(day=1)

    # set end date, as next month
    end_date = (start_date + datetime.timedelta(days=31)).replace(day=1) - datetime.timedelta(days=1)

    # list of dates in that month
    dates = pd.date_range(start_date, end_date, freq="D")

    data_df = []
    for date in dates:

        # load file from hugging face
        huggingface_load_file = get_daily_hf_file_name(date=date)

        # load data
        print(f"Loading data from {huggingface_load_file}")
        with fs.open(f'datasets/openclimatefix/uk_pv/{huggingface_load_file}') as f:
            data = f.read()
        pq_file = io.BytesIO(data)
        generation_data = pd.read_parquet(pq_file)

        data_df.append(generation_data)

    # join together data
    generation_data = pd.concat(data_df)

    # save to parquet file
    local_file = f"passiv_5min_monthly_{start_date.date()}.parquet"
    generation_data.to_parquet(local_file)

    # upload to hugging face
    if upload_to_hf:
        api = HfApi()
        api.token = os.getenv("HUGGINGFACE_TOKEN")
        api.upload_file(
            path_or_fileobj=local_file,
            path_in_repo=huggingface_file,
            repo_id="openclimatefix/uk_pv",
            repo_type="dataset",
        )


@dg.asset(
    key=["pv", "passiv", "monthly"],
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m",
        start="2023-01",
        cron_schedule="0 12 2 * *",  # 2nd day of the month, at 12 oclock,
    ),
    # metadata={
    #     "path": dg.MetadataValue.path(f"{BASE_PATH}/nwp/meteomatics/nw_india/solar_archive"),
    # },
)
def pv_passiv_monthly(context: dg.AssetExecutionContext):
    """PV Passiv archive asset."""

    partition_date_str = context.partition_key
    start_date = datetime.datetime.strptime(partition_date_str, "%Y-%m")
    start_date = pytz.utc.localize(start_date)

    get_monthly_passiv_data(start_date)




