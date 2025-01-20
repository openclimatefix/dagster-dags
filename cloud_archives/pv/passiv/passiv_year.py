"""Get passiv daily data and save to Hugging Face."""

import datetime as dt
import io
import logging
import os
from typing import Literal

import dagster as dg
import pandas as pd
import pytz
from huggingface_hub import HfFileSystem
from huggingface_hub.hf_api import HfApi

from .filenames import get_monthly_hf_file_name, get_yearly_hf_file_name

logger = logging.getLogger(__name__)

def get_yearly_passiv_data(
        start_date: dt.datetime,
        upload_to_hf: bool = True,
        overwrite: bool = False,
        period: Literal[5, 30] = 5,
    ) -> None:
    """Get yearly passiv data and save to Hugging Face."""
    # set up HF and check if we have data for that day already
    huggingface_file = get_yearly_hf_file_name(date=start_date, period=period)
    token = os.getenv("HUGGINGFACE_TOKEN")
    fs = HfFileSystem(token=token)
    if not overwrite and fs.exists(f"datasets/openclimatefix/uk_pv/{huggingface_file}"):
        logger.info(f"Data already exists for {start_date.date()}")
        return

    # start of the month from datetime
    start_date = start_date.replace(day=1)
    end_date = start_date + dt.timedelta(days=365)

    data_df = []
    date = start_date
    while date < end_date:

        # load file from hugging face
        huggingface_load_file = get_monthly_hf_file_name(date=date, period=period)

        # load data
        logger.info(f"Loading data from {huggingface_load_file}")
        with fs.open(f"datasets/openclimatefix/uk_pv/{huggingface_load_file}") as f:
            data = f.read()
        pq_file = io.BytesIO(data)
        generation_data = pd.read_parquet(pq_file)

        data_df.append(generation_data)

        date = date + dt.timedelta(days=31)
        date = date.replace(day=1)

    # join together data
    generation_data = pd.concat(data_df)

    # save to parquet file
    local_file = f"passiv_{period}min_yearly_{start_date.date()}.parquet"
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
    key=["pv", "passiv", "yearly_5min"],
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y",
        start="2018",
        cron_schedule="0 12 2 1 *",  # 2nd day of January, at 12 oclock,
    ),
)
def pv_passiv_yearly_5min(context: dg.AssetExecutionContext) -> None:
    """PV Passiv archive yearly data."""
    partition_date_str = context.partition_key
    start_date = dt.datetime.strptime(partition_date_str, "%Y").replace(tzinfo=dt.UTC)
    start_date = pytz.utc.localize(start_date)

    get_yearly_passiv_data(start_date, period=5)


@dg.asset(
    key=["pv", "passiv", "yearly_30min"],
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=dg.TimeWindowPartitionsDefinition(
        fmt="%Y",
        start="2010",
        cron_schedule="0 12 2 1 *",  # 2nd day of January, at 12 oclock,
    ),
)
def pv_passiv_yearly_30min(context: dg.AssetExecutionContext) -> None:
    """PV Passiv archive yearly data."""
    partition_date_str = context.partition_key
    start_date = dt.datetime.strptime(partition_date_str, "%Y").replace(tzinfo=dt.UTC)
    start_date = pytz.utc.localize(start_date)

    get_yearly_passiv_data(start_date, period=30)

