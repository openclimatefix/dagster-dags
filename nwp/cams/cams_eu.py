import dagster as dg
from cdsapi import Client
import datetime as dt

from ._factories import (
    make_asset_definitions,
    MakeAssetDefinitionsOptions,
    VariableSelection,
)


# CAMS data is only available from 3 years ago onwards
start_date: dt.datetime = dt.datetime.now(tz=dt.UTC) - dt.timedelta(days=3 * 365)
cams_eu_partitions = dg.MultiPartitionsDefinition(
    {
        "date": dg.DailyPartitionsDefinition(start_date=start_date.strftime("%Y-%m-%d")),
        "inittime": dg.StaticPartitionsDefinition(["00:00"]),
    },
)

VARIABLES = [
    "alder_pollen",
]

opts = MakeAssetDefinitionsOptions(
    area="eu",
    file_format="netcdf",
    multilevel_vars=VariableSelection(
        slow=VARIABLES, fast=VARIABLES, hours=[str(x) for x in range(0, 97)],
    ),
    multilevel_levels=['0', '1000', '2000', '250', '3000', '50', '500', '5000'],
    partitions=cams_eu_partitions,
    client=Client(),
)

cams_eu_source_archive, cams_eu_raw_archive = make_asset_definitions(opts=opts)

