from datetime import timedelta

from dagster import Config


class IconConfig(Config):
    model: str
    run: str
    delay: int = 0
    folder: str
    zarr_path: str


def get_run(current_datetime, delay=0):
    now = current_datetime + timedelta(hours=delay)
    date_string = now.strftime("%Y%m%d")
    hour = now.strftime("%H")
    return date_string, hour
