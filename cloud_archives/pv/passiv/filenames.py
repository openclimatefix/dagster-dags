from datetime import datetime


def get_monthly_hf_file_name(date: datetime, period: int = 5):
    return f"data/{date.strftime('%Y/%m')}/{period}min.parquet"


def get_yearly_hf_file_name(date: datetime, period: int = 5):
    return f"data/{date.strftime('%Y')}/{period}min.parquet"
