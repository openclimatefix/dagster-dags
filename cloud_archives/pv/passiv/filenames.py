"""Functions for naming files for passiv datasets."""

from datetime import datetime


def get_monthly_hf_file_name(date: datetime, period: int = 5) -> str:
    """Format a datetime as a huggingface-appropriate monthly filename."""
    return f"data/{date.strftime('%Y/%m')}/{date.strftime('%Y%m')}_{period}min.parquet"


def get_yearly_hf_file_name(date: datetime, period: int = 5) -> str:
    """Format a datetime as a huggingface-appropriate yearly filename."""
    return f"data/{date.strftime('%Y')}/{date.strftime('%Y')}_{period}min.parquet"
