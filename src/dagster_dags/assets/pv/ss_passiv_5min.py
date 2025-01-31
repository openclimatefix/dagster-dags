"""Parquet archive of PV data from Passiv meters in the UK.

Sheffield Solar collect PV data from a number of sources, including Passiv
systems. This timeseries data is recorded at two resolutions: 5-minutely
and 30-minutely. This asset pulls the 5-minutely data.

Sourced from Sheffield Solar's API (https://api.solar.sheffield.ac.uk/pvlive/docs).
This asset is updated monthly, and surfaced as a parquet file per month.
The data is then uploaded to Hugging Face for public access.
"""


import dagster as dg
import pandas as pd

from dagster_dags.resources import (
    SheffieldSolarAPIResource,
    SheffieldSolarMetadataRequest,
    SheffieldSolarRawdataRequest,
)

monthly_partitions_def: dg.MonthlyPartitionsDefinition = dg.MonthlyPartitionsDefinition(
    start_date="2018-01-01",
    end_offset=-1,
    hour_offset=12,
)

@dg.asset(
    name="passiv_5min_monthly",
    description=__doc__,
    metadata={
        "source": dg.MetadataValue.text("sheffield-solar"),
        "area": dg.MetadataValue.text("uk"),
        "expected_runtime": dg.MetadataValue.text("1 hour"),
    },
    partitions_def=monthly_partitions_def,
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule=monthly_partitions_def.get_cron_schedule(
            day_of_week=1,
            hour_of_day=20,
        ),
    ),
    io_manager_key="parquet_io_manager",
)
def pv_passiv_5min_monthly_asset(
    context: dg.AssetExecutionContext,
    ss_api_client: SheffieldSolarAPIResource,
) -> pd.DataFrame:
    """Dagster asset downloading 5-minutely PV data from Passiv systems."""
    # Don't redownload existing data
    try:
        existing_df: pd.DataFrame = context.resources.original_resource_dict["parquet_io_manager"]\
            .manual_load(
                asset_key=context.asset_key,
                partition_time_window=context.partition_time_window,
            )
        return existing_df
    except Exception as e:
        context.log.debug(f"No existing data found: {e}.")

    passiv_metadata_df: pd.DataFrame = (
        ss_api_client.request(SheffieldSolarMetadataRequest())
        .loc[lambda df: df["owner_name"] == "Passiv"]
    )

    context.log.info(f"Fetching data for {context.partition_time_window.start:%Y-%m}")
    request: SheffieldSolarRawdataRequest = SheffieldSolarRawdataRequest(
        start=context.partition_time_window.start,
        end=context.partition_time_window.end,
        period_mins=5,
    )
    passive_df: pd.DataFrame = (
        ss_api_client.request(request=request)
        .loc[lambda df: df["ss_id"].isin(passiv_metadata_df["ss_id"])]
        .assign(datetime_GMT=lambda df: pd.to_datetime(df["date"]).dt.tz_localize("UTC"))
    )
    return passive_df


yearly_partitions_def: dg.TimeWindowPartitionsDefinition = dg.TimeWindowPartitionsDefinition(
    fmt="%Y",
    start="2010",
    cron_schedule="0 12 2 1 *",
)

@dg.asset(
    name="passiv_5min_yearly",
    description="Yearly summary of 5-minutely PV data from Passiv systems.",
    metadata={
        "source": dg.MetadataValue.text("sheffield-solar"),
        "area": dg.MetadataValue.text("uk"),
        "expected_runtime": dg.MetadataValue.text("1 hour"),
    },
    partitions_def=yearly_partitions_def,
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule=yearly_partitions_def.get_cron_schedule(),
    ),
    ins={"passiv_5min_monthly": dg.AssetIn(partition_mapping=dg.TimeWindowPartitionMapping())},
    io_manager_key="parquet_io_manager",
)
def pv_passiv_5min_yearly_asset(
    context: dg.AssetExecutionContext,
    passiv_5min_monthly: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Dagster asset summarising yearly 5-minutely PV data from Passiv systems."""
    context.log.info("Summarizing monthly data for {context.partition_time_windoiw.start:%Y}")
    return pd.concat(passiv_5min_monthly.values())

