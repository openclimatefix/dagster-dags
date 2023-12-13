import datetime as dt
import os

import cdsapi
import dagster
from dagster import AssetObservation

from nwp.assets.cams.utils import CAMSConfig

INIT_TIMES: list[str] = [
    '00:00',
]

VARIABLES = [
    'alder_pollen', 'ammonia', 'birch_pollen',
    'carbon_monoxide', 'dust', 'grass_pollen',
    'nitrogen_dioxide', 'nitrogen_monoxide', 'non_methane_vocs',
    'olive_pollen', 'ozone', 'particulate_matter_10um',
    'particulate_matter_2.5um', 'peroxyacyl_nitrates', 'pm10_wildfires',
    'ragweed_pollen', 'secondary_inorganic_aerosol', 'sulphur_dioxide',
]


@dagster.op
def fetch_cams_eu_forecast_for_day(context: dagster.OpExecutionContext, config: CAMSConfig):
    """Fetch CAMS forecast for a given day."""
    c = cdsapi.Client()

    date: dt.datetime = dt.datetime.strptime(config.date, "%Y-%m-%d")
    variables_to_use = config.dict().get("variables", VARIABLES)

    if date < dt.datetime.utcnow() - dt.timedelta(days=1095):
        raise ValueError('CAMS data is only available from 3 years ago onwards.')

    # Multi-level variables first
    for it in INIT_TIMES:
        for var in variables_to_use:
            fname: str = f'{config.raw_dir}/{date.strftime("%Y%m%d")}{it[:2]}_{var}.grib'

            c.retrieve(
                'cams-europe-air-quality-forecasts',
                {
                    'date': date.strftime("%Y-%m-%d/%Y-%m-%d"),
                    'type': 'forecast',
                    'format': 'netcdf',
                    'model': 'ensemble',
                    'variable': var,
                    'level': [
                        '0', '1000', '2000',
                        '250', '3000', '50',
                        '500', '5000',
                    ],
                    'leadtime_hour': [
                        '0', '1', '10',
                        '11', '12', '13',
                        '14', '15', '16',
                        '17', '18', '19',
                        '2', '20', '21',
                        '22', '23', '24',
                        '25', '26', '27',
                        '28', '29', '3',
                        '30', '31', '32',
                        '33', '34', '35',
                        '36', '37', '38',
                        '39', '4', '40',
                        '41', '42', '43',
                        '44', '45', '46',
                        '47', '48', '49',
                        '5', '50', '51',
                        '52', '53', '54',
                        '55', '56', '57',
                        '58', '59', '6',
                        '60', '61', '62',
                        '63', '64', '65',
                        '66', '67', '68',
                        '69', '7', '70',
                        '71', '72', '73',
                        '74', '75', '76',
                        '77', '78', '79',
                        '8', '80', '81',
                        '82', '83', '84',
                        '85', '86', '87',
                        '88', '89', '9',
                        '90', '91', '92',
                        '93', '94', '95',
                        '96',
                    ],
                    "time": it,
                },
                f'{config.raw_dir}/{date.strftime("%Y%m%d")}{it[:2]}_{var}.nc')

            context.log_event(
                AssetObservation(
                    asset_key="cams_eu_data",
                    metadata={
                        "path": fname,
                        "date": date.strftime("%Y-%m-%d"),
                        "variable": var,
                        "init_time": it,
                    }
                )
            )



        # Validate that all files were downloaded
        for var in variables_to_use:
            fname: str = f'{config.raw_dir}/{date.strftime("%Y%m%d")}{it[:2]}_{var}.nc'
            if not os.path.isfile(fname):
                raise FileNotFoundError(f"File {fname} was not downloaded.")
