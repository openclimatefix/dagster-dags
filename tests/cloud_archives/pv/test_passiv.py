from cloud_archives.pv.passiv.passiv_daily import get_daily_passiv_data

from datetime import datetime, timezone


def test_get_daily_passiv_data():
    start_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
    get_daily_passiv_data(start_date, upload_to_hf=False)