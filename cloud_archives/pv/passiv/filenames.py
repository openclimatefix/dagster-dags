from datetime import datetime


def get_monthly_hf_file_name(date: datetime, period: int = 5):
    return f"data/{date.strftime('%Y/%m')}/{date.strftime('%Y%m')}_{period}min.parquet"


def get_yearly_hf_file_name(date: datetime, period: int = 5):
    return f"data/{date.strftime('%Y')}/{date.strftime('%Y')}_{period}min.parquet"
