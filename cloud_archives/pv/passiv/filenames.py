from datetime import datetime


def get_daily_hf_file_name(date: datetime):
    return f"data/{date.strftime('%Y/%m/%d')}/5min.parquet"


def get_monthly_hf_file_name(date: datetime):
    return f"data/{date.strftime('%Y/%m')}/5min.parquet"


def get_yearly_hf_file_name(date: datetime):
    return f"data/{date.strftime('%Y')}/5min.parquet"
