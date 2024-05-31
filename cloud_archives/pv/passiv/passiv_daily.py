""" Get passiv daily data and save to Hugging Face"""

import datetime
import os, pytz
import pandas as pd
from huggingface_hub.hf_api import HfApi
import dagster as dg

from .ss_rawdata_api import SSRawDataAPI


def get_daily_passiv_data(start_date: datetime, upload_to_hf: bool = True):
    """ Get dail passiv data and save to Hugging Face"""

    # set end date
    end_date = start_date + datetime.timedelta(days=1)

    # setup class
    ss_rawdata_api = SSRawDataAPI(
        user_id=os.getenv("SS_USER_ID"),
        api_key=os.getenv("SS_API_KEY")
    )

    # only get passiv systems
    system_metadata = ss_rawdata_api.metadata
    system_metadata = system_metadata[system_metadata["owner_name"] == "Passiv"]

    # get generation data
    generation_data = ss_rawdata_api.download(
        start=start_date,
        end=end_date,
        period=5,
        n_processes=0
    )

    # filter out only passiv systems
    generation_data = generation_data[generation_data["ss_id"].isin(system_metadata["ss_id"])]

    # format datetime_GMT as datetime and timezone
    generation_data["datetime_GMT"] = pd.to_datetime(generation_data["datetime_GMT"])
    generation_data["datetime_GMT"] = generation_data["datetime_GMT"].dt.tz_localize("UTC")

    # dont include the last end date
    generation_data = generation_data[generation_data.datetime_GMT < end_date.replace(tzinfo=pytz.utc)]

    # save to parquet file
    file = f"passiv_5min_{start_date.date()}.parquet"
    generation_data.to_parquet(file)

    # upload to hugging face
    if upload_to_hf:
        huggingface_dir = f"data/{start_date.strftime('%Y/%m/%d')}/5min.parquet"
        api = HfApi()
        api.token = os.getenv("HUGGINGFACE_TOKEN")
        api.upload_file(
            path_or_fileobj=file,
            path_in_repo=huggingface_dir,
            repo_id="openclimatefix/uk_pv",
            repo_type="dataset",
        )


@dg.asset(
    key=["pv", "passiv", "daily"],
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m-%d",
        start="2023-01-01",
        cron_schedule="12 1 * * *",  # Once a day, at 12 oclock
    ),
    # metadata={
    #     "path": dg.MetadataValue.path(f"{BASE_PATH}/nwp/meteomatics/nw_india/solar_archive"),
    # },
)
def pv_passiv_daily(context: dg.AssetExecutionContext):
    """PV Passiv archive asset."""

    partition_date_str = context.partition_key
    start_date = datetime.datetime.strptime(partition_date_str, "%Y-%m-%d")
    start_date = pytz.utc.localize(start_date)

    get_daily_passiv_data(start_date)




