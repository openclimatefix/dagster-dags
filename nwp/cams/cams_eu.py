import datetime as dt
import pathlib

import cdsapi
import dagster as dg
from cdsapi.api import Result

from constants import LOCATIONS_BY_ENVIRONMENT

RAW_FOLDER = LOCATIONS_BY_ENVIRONMENT["local"].RAW_FOLDER

c = cdsapi.Client()

cams_eu_partitions = dg.MultiPartitionsDefinition({
    "date": dg.DailyPartitionsDefinition(start_date="2020-01-01"),
    "inittime": dg.StaticPartitionsDefinition(["00:00"]),
})

def map_partition_to_time(context: dg.AssetExecutionContext) -> dt.datetime:
    """Map a partition key to a datetime."""
    partkeys = context.partition_key.keys_by_dimension
    return dt.datetime.strptime(
        f"{partkeys['date']}|{partkeys['inittime']}", "%Y-%m-%d|%H:%M"
    ).replace(tzinfo=dt.UTC)

VARIABLES = [
    'alder_pollen', 'ammonia', 'birch_pollen',
    'carbon_monoxide', 'dust', 'grass_pollen',
    'nitrogen_dioxide', 'nitrogen_monoxide', 'non_methane_vocs',
    'olive_pollen', 'ozone', 'particulate_matter_10um',
    'particulate_matter_2.5um', 'peroxyacyl_nitrates', 'pm10_wildfires',
    'ragweed_pollen', 'secondary_inorganic_aerosol', 'sulphur_dioxide',
]

@dg.asset(
    name="source_archive",
    key_prefix=["nwp", "cams", "eu"],
    partitions_def=cams_eu_partitions,
    check_specs=[
        dg.AssetCheckSpec(name="nonzero_num_files", asset=["nwp", "cams", "eu", "source_archive"])
    ],
    compute_kind="network_request",
    op_tags={
        "expected_runtime": "20min",
        "MAX_RUNTIME_SECONDS_TAG": 20 * 60,
    }
)
def cams_eu_source_archive(
        context: dg.AssetExecutionContext
) -> dg.Output(list[Result]):
    """CAMS EU source archive."""
    it = map_partition_to_time(context)
    results: list[Result] = []
    for var in VARIABLES:
        result = c.retrieve(
            name='cams-europe-air-quality-forecasts',
            request={
                'date': it.strftime("%Y-%m-%d/%Y-%m-%d"),
                'type': 'forecast',
                'format': 'netcdf',
                'model': 'ensemble',
                'variable': var,
                'level': ['0', '1000', '2000', '250', '3000', '50','500', '5000'],
                'leadtime_hour': [
                    '0', '1', '10', '11', '12', '13', '14', '15', '16',
                    '17', '18', '19', '2', '20', '21', '22', '23', '24', '25', '26', '27',
                    '28', '29', '3', '30', '31', '32', '33', '34', '35', '36', '37', '38',
                    '39', '4', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49',
                    '5', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '6',
                    '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '7', '70',
                    '71', '72', '73', '74', '75', '76', '77', '78', '79', '8', '80', '81',
                    '82', '83', '84', '85', '86', '87', '88', '89', '9', '90', '91', '92',
                    '93', '94', '95', '96',
                ],
                "time": it.strftime("%H:%M"),
            },
        )
        results.append(result)

    yield dg.Output(results, metadata={
        "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
        "num_files": dg.MetadataValue.int(len(results)),
        "file_locs": dg.MetadataValue.text(str([r.location for r in results])),
    })

    yield dg.AssetCheckResult(
        check_name="nonzero_num_files",
        passed=bool(len(results) > 0),
        metadata={"num_files": dg.MetadataValue.int(len(results))},
    )

@dg.asset(
    name="raw_archive",
    key_prefix=["nwp", "cams", "eu"],
    partitions_def=cams_eu_partitions,
    compute_kind="download",
    op_tags={
        "expected_runtime": "20min",
        "MAX_RUNTIME_SECONDS_TAG": 20 * 60,
    }
)
def cams_eu_raw_archive(
    context: dg.AssetExecutionContext,
    source_archive: list[Result],
) -> list[pathlib.Path]:
    stored_paths: list[pathlib.Path] = []
    for result in source_archive:
        it = map_partition_to_time(context)
        # Store the file based on the asset key prefix and the init time of the file
        loc = "/".join(context.asset_key.path[:-1])
        fname: str = f'{RAW_FOLDER}/{loc}/{result.location}.grib'
        result.download(target=str(context.temp_path / result.location.split("/")[-1]))
        stored_paths.append(pathlib.Path(fname))

    return dg.Output(stored_paths, metadata={
        "inittime": dg.MetadataValue.text(context.asset_partition_key_for_output()),
        "num_files": dg.MetadataValue.int(len(stored_paths)),
        "file_locs": dg.MetadataValue.text(str([r.as_posix() for r in stored_paths])),
    })

