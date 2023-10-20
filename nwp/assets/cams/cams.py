import datetime as dt

import cdsapi
import dagster
from dagster import AssetObservation

VARIABLES: list[str] = [
            'aerosol_extinction_coefficient_1064nm', 'aerosol_extinction_coefficient_355nm', 'aerosol_extinction_coefficient_532nm',
            'ammonium_aerosol_mass_mixing_ratio', 'ammonium_aerosol_optical_depth_550nm', 'anthropogenic_secondary_organic_aerosol_mass_mixing_ratio',
            'asymmetry_factor_1020nm', 'asymmetry_factor_1064nm', 'asymmetry_factor_1240nm',
            'asymmetry_factor_1640nm', 'asymmetry_factor_2130nm', 'asymmetry_factor_340nm',
            'asymmetry_factor_355nm', 'asymmetry_factor_380nm', 'asymmetry_factor_400nm',
            'asymmetry_factor_440nm', 'asymmetry_factor_469nm', 'asymmetry_factor_500nm',
            'asymmetry_factor_532nm', 'asymmetry_factor_550nm', 'asymmetry_factor_645nm',
            'asymmetry_factor_670nm', 'asymmetry_factor_800nm', 'asymmetry_factor_858nm',
            'asymmetry_factor_865nm', 'attenuated_backscatter_due_to_aerosol_1064nm_from_ground', 'attenuated_backscatter_due_to_aerosol_1064nm_from_top_of_atmosphere',
            'attenuated_backscatter_due_to_aerosol_355nm_from_ground', 'attenuated_backscatter_due_to_aerosol_355nm_from_top_of_atmosphere', 'attenuated_backscatter_due_to_aerosol_532nm_from_ground',
            'attenuated_backscatter_due_to_aerosol_532nm_from_top_of_atmosphere', 'biogenic_secondary_organic_aerosol_mass_mixing_ratio', 'black_carbon_aerosol_optical_depth_550nm',
            'carbon_monoxide', 'chlorine_monoxide', 'chlorine_nitrate',
            'dust_aerosol_0.03-0.55um_mixing_ratio', 'dust_aerosol_0.03-0.55um_optical_depth_550nm', 'dust_aerosol_0.55-0.9um_mixing_ratio',
            'dust_aerosol_0.55-9um_optical_depth_550nm', 'dust_aerosol_0.9-20um_mixing_ratio', 'dust_aerosol_9-20um_optical_depth_550nm',
            'dust_aerosol_optical_depth_550nm', 'ethane', 'formaldehyde',
            'hydrogen_chloride', 'hydrogen_cyanide', 'hydrogen_peroxide',
            'hydrophilic_black_carbon_aerosol_mixing_ratio', 'hydrophilic_black_carbon_aerosol_optical_depth_550nm', 'hydrophilic_organic_matter_aerosol_mixing_ratio',
            'hydrophilic_organic_matter_aerosol_optical_depth_550nm', 'hydrophobic_black_carbon_aerosol_mixing_ratio', 'hydrophobic_black_carbon_aerosol_optical_depth_550nm',
            'hydrophobic_organic_matter_aerosol_mixing_ratio', 'hydrophobic_organic_matter_aerosol_optical_depth_550nm', 'hydroxyl_radical',
            'isoprene', 'methane', 'nitrate_aerosol_optical_depth_550nm',
            'nitrate_coarse_mode_aerosol_mass_mixing_ratio', 'nitrate_coarse_mode_aerosol_optical_depth_550nm', 'nitrate_fine_mode_aerosol_mass_mixing_ratio',
            'nitrate_fine_mode_aerosol_optical_depth_550nm', 'nitric_acid', 'nitrogen_dioxide',
            'nitrogen_monoxide', 'organic_matter_aerosol_optical_depth_550nm', 'ozone',
            'particulate_matter_10um', 'particulate_matter_1um', 'particulate_matter_2.5um',
            'peroxyacetyl_nitrate', 'propane', 'sea_salt_aerosol_0.03-0.5um_mixing_ratio',
            'sea_salt_aerosol_0.03-0.5um_optical_depth_550nm', 'sea_salt_aerosol_0.5-5um_mixing_ratio', 'sea_salt_aerosol_0.5-5um_optical_depth_550nm',
            'sea_salt_aerosol_5-20um_mixing_ratio', 'sea_salt_aerosol_5-20um_optical_depth_550nm', 'sea_salt_aerosol_optical_depth_550nm',
            'secondary_organic_aerosol_optical_depth_550nm', 'single_scattering_albedo_1020nm', 'single_scattering_albedo_1064nm',
            'single_scattering_albedo_1240nm', 'single_scattering_albedo_1640nm', 'single_scattering_albedo_2130nm',
            'single_scattering_albedo_340nm', 'single_scattering_albedo_355nm', 'single_scattering_albedo_380nm',
            'single_scattering_albedo_400nm', 'single_scattering_albedo_440nm', 'single_scattering_albedo_469nm',
            'single_scattering_albedo_500nm', 'single_scattering_albedo_532nm', 'single_scattering_albedo_550nm',
            'single_scattering_albedo_645nm', 'single_scattering_albedo_670nm', 'single_scattering_albedo_800nm',
            'single_scattering_albedo_858nm', 'single_scattering_albedo_865nm', 'sulphate_aerosol_mixing_ratio',
            'sulphate_aerosol_optical_depth_550nm', 'sulphur_dioxide', 'total_absorption_aerosol_optical_depth_1020nm',
            'total_absorption_aerosol_optical_depth_1064nm', 'total_absorption_aerosol_optical_depth_1240nm', 'total_absorption_aerosol_optical_depth_1640nm',
            'total_absorption_aerosol_optical_depth_2130nm', 'total_absorption_aerosol_optical_depth_340nm', 'total_absorption_aerosol_optical_depth_355nm',
            'total_absorption_aerosol_optical_depth_380nm', 'total_absorption_aerosol_optical_depth_400nm', 'total_absorption_aerosol_optical_depth_440nm',
            'total_absorption_aerosol_optical_depth_469nm', 'total_absorption_aerosol_optical_depth_500nm', 'total_absorption_aerosol_optical_depth_532nm',
            'total_absorption_aerosol_optical_depth_550nm', 'total_absorption_aerosol_optical_depth_645nm', 'total_absorption_aerosol_optical_depth_670nm',
            'total_absorption_aerosol_optical_depth_800nm', 'total_absorption_aerosol_optical_depth_858nm', 'total_absorption_aerosol_optical_depth_865nm',
            'total_aerosol_optical_depth_1020nm', 'total_aerosol_optical_depth_1064nm', 'total_aerosol_optical_depth_1240nm',
            'total_aerosol_optical_depth_1640nm', 'total_aerosol_optical_depth_2130nm', 'total_aerosol_optical_depth_340nm',
            'total_aerosol_optical_depth_355nm', 'total_aerosol_optical_depth_380nm', 'total_aerosol_optical_depth_400nm',
            'total_aerosol_optical_depth_440nm', 'total_aerosol_optical_depth_469nm', 'total_aerosol_optical_depth_500nm',
            'total_aerosol_optical_depth_532nm', 'total_aerosol_optical_depth_550nm', 'total_aerosol_optical_depth_645nm',
            'total_aerosol_optical_depth_670nm', 'total_aerosol_optical_depth_800nm', 'total_aerosol_optical_depth_858nm',
            'total_aerosol_optical_depth_865nm', 'total_column_carbon_monoxide', 'total_column_chlorine_monoxide',
            'total_column_chlorine_nitrate', 'total_column_ethane', 'total_column_formaldehyde',
            'total_column_hydrogen_chloride', 'total_column_hydrogen_cyanide', 'total_column_hydrogen_peroxide',
            'total_column_hydroxyl_radical', 'total_column_isoprene', 'total_column_methane',
            'total_column_nitric_acid', 'total_column_nitrogen_dioxide', 'total_column_nitrogen_monoxide',
            'total_column_ozone', 'total_column_peroxyacetyl_nitrate', 'total_column_propane',
            'total_column_sulphur_dioxide', 'total_fine_mode_aerosol_optical_depth_1020nm', 'total_fine_mode_aerosol_optical_depth_1064nm',
            'total_fine_mode_aerosol_optical_depth_1240nm', 'total_fine_mode_aerosol_optical_depth_1640nm', 'total_fine_mode_aerosol_optical_depth_2130nm',
            'total_fine_mode_aerosol_optical_depth_340nm', 'total_fine_mode_aerosol_optical_depth_355nm', 'total_fine_mode_aerosol_optical_depth_380nm',
            'total_fine_mode_aerosol_optical_depth_400nm', 'total_fine_mode_aerosol_optical_depth_440nm', 'total_fine_mode_aerosol_optical_depth_469nm',
            'total_fine_mode_aerosol_optical_depth_500nm', 'total_fine_mode_aerosol_optical_depth_532nm', 'total_fine_mode_aerosol_optical_depth_550nm',
            'total_fine_mode_aerosol_optical_depth_645nm', 'total_fine_mode_aerosol_optical_depth_670nm', 'total_fine_mode_aerosol_optical_depth_800nm',
            'total_fine_mode_aerosol_optical_depth_858nm', 'total_fine_mode_aerosol_optical_depth_865nm',
        ]

INIT_TIMES: list[str] = [
    '00:00', '12:00',
]

class CAMSConfig(dagster.Config):
    date: str
    raw_dir: str

@dagster.op
def fetch_cams_forecast_for_day(context: dagster.OpExecutionContext, config: CAMSConfig):
    """Fetch CAMS forecast for a given day."""
    
    c = cdsapi.Client()

    date: dt.datetime = dt.datetime.strptime(config.date, "%Y-%m-%d")

    if date < dt.datetime(2015, 1, 1):
        raise ValueError('CAMS data is only available from 2015-01-01 onwards.')

    for it in INIT_TIMES:
        for var in VARIABLES:
            fname: str = f'{config.raw_dir}/{date.strftime("%Y%m%d")}{it[:2]}_{var}.grib'

            c.retrieve(
                'cams-global-atmospheric-composition-forecasts',
                {
                    'date': date.strftime("%Y-%m-%d/%Y-%m-%d"),
                    'type': 'forecast',
                    'format': 'grib',
                    'variable': var,
                    'pressure_level': [
                        '1', '2', '3',
                        '5', '7', '10',
                        '20', '30', '50',
                        '70', '100', '150',
                        '200', '250', '300',
                        '400', '500', '600',
                        '700', '800', '850',
                        '900', '925', '950',
                        '1000',
                    ],
                    'leadtime_hour': [
                        '0', '1', '10',
                        '100', '101', '102',
                        '103', '104', '105',
                        '106', '107', '108',
                        '109', '11', '110',
                        '111', '112', '113',
                        '114', '115', '116',
                        '117', '118', '119',
                        '12', '120', '13',
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
                        '96', '97', '98',
                        '99',
                    ],
                    "time": it,
                },
            f'{config.raw_dir}/{date.strftime("%Y%m%d")}{it[:2]}_{var}.grib')

            context.log_event(
                AssetObservation(
                    asset_key="cams_data",
                    metadata={
                        "path": fname,
                        "date": date.strftime("%Y-%m-%d"),
                        "variable": var,
                        "init_time": it,
                    }
                )
            )
