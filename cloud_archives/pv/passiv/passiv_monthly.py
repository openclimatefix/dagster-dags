""" Get passiv daily data and save to Hugging Face"""

import datetime
import os, pytz
import logging
import pandas as pd
from huggingface_hub.hf_api import HfApi
import dagster as dg

from .ss_rawdata_api import SSRawDataAPI
from .filenames import get_monthly_hf_file_name
from huggingface_hub import HfFileSystem

logger = logging.getLogger(__name__)


def get_monthly_passiv_data(start_date: datetime, upload_to_hf: bool = True, overwrite: bool = False, period:int=5):
    """ Get monthly passiv data and save to Hugging Face"""

    logger.info(f"Getting data for {start_date}")

    # check if we have data for that day already
    huggingface_file = get_monthly_hf_file_name(date=start_date, period=period)
    if not overwrite:
        token = os.getenv("HUGGINGFACE_TOKEN")
        fs = HfFileSystem(token=token)
        if fs.exists(f'datasets/openclimatefix/uk_pv/{huggingface_file}'):
            print(f"Data already exists for {start_date.date()}")
            return

    # set end date
    end_date = (start_date + datetime.timedelta(days=31)).replace(day=1)

    # setup class
    ss_rawdata_api = SSRawDataAPI(
        user_id=os.getenv("SS_USER_ID"),
        api_key=os.getenv("SS_API_KEY")
    )

    # only get passiv systems
    system_metadata = ss_rawdata_api.metadata
    system_metadata = system_metadata[system_metadata["owner_name"] == "Passiv"]

    # get generation data
    logger.debug(f"Downloading data from {start_date} to {end_date}")
    generation_data = ss_rawdata_api.download(
        start=start_date,
        end=end_date,
        period=period,
    )

    # filter out only passiv systems
    generation_data = generation_data[generation_data["ss_id"].isin(system_metadata["ss_id"])]

    # format datetime_GMT as datetime and timezone
    generation_data["datetime_GMT"] = pd.to_datetime(generation_data["datetime_GMT"])
    if generation_data["datetime_GMT"].iloc[0].tzinfo is None:
        generation_data["datetime_GMT"] = generation_data["datetime_GMT"].dt.tz_localize("UTC")

    # dont include the last end date
    generation_data = generation_data[generation_data.datetime_GMT < end_date.replace(tzinfo=pytz.utc)]

    # save to parquet file
    file = f"passiv_5min_{start_date.date()}.parquet"
    generation_data.to_parquet(file)

    # upload to hugging face
    if upload_to_hf:
        logger.info("Uploading to Hugging Face")
        api = HfApi()
        api.token = os.getenv("HUGGINGFACE_TOKEN")
        api.upload_file(
            path_or_fileobj=file,
            path_in_repo=huggingface_file,
            repo_id="openclimatefix/uk_pv",
            repo_type="dataset",
        )
        
    # remove local file
    logger.info("Removing local file")
    os.remove(file)


@dg.asset(
    key=["pv", "passiv", "monthly_30min"],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m",
        start="2010-01",
        cron_schedule="0 12 1 * *",  # 1st day of the month, at 12 oclock
    ),
)
def pv_passiv_monthly_30min(context: dg.AssetExecutionContext):
    """PV Passiv archive monthlyasset."""

    partition_date_str = context.partition_key
    start_date = datetime.datetime.strptime(partition_date_str, "%Y-%m")
    start_date = pytz.utc.localize(start_date)

    get_monthly_passiv_data(start_date, period=30)




@dg.asset(
    key=["pv", "passiv", "monthly_5min"],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y-%m",
        start="2018-01",
        cron_schedule="0 12 1 * *",  # 1st day of the month, at 12 oclock
    ),
)
def pv_passiv_monthly_5min(context: dg.AssetExecutionContext):
    """PV Passiv archive monthlyasset."""

    partition_date_str = context.partition_key
    start_date = datetime.datetime.strptime(partition_date_str, "%Y-%m")
    start_date = pytz.utc.localize(start_date)

    get_monthly_passiv_data(start_date, period=5)




