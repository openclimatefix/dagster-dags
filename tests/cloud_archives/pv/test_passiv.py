from datetime import UTC, datetime

from cloud_archives.pv.passiv.passiv_monthly import get_monthly_passiv_data


def test_get_daily_passiv_data():
    start_date = datetime(2024, 12, 5, tzinfo=UTC)
    get_monthly_passiv_data(start_date, upload_to_hf=False, overwrite=True)
