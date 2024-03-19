import datetime as dt

import dagster as dg
from cdsapi import Client

from local_archives.partitions import InitTimePartitionsDefinition

from ._definitions_factory import (
    MakeDefinitionsOptions,
    MakeDefinitionsOutputs,
    VariableSelection,
    make_definitions,
)

# CAMS data is only available from 3 years ago onwards
start_date: dt.datetime = dt.datetime.now(tz=dt.UTC) - dt.timedelta(days=3 * 365)
cams_eu_partitions: dg.TimeWindowPartitionsDefinition = dg.TimeWindowPartitionsDefinition(
    start=start_date.strftime("%Y-%m-%dT%H:%M"),
    cron_schedule="0 0 * * *",  # Daily at midnight
    fmt="%Y-%m-%dT%H:%M",
)

VARIABLES = [
    "alder_pollen",
    "ammonia",
    "birch_pollen",
    "carbon_monoxide",
    "dust",
    "grass_pollen",
    "nitrogen_dioxide",
    "nitrogen_monoxide",
    "non_methane_vocs",
    "olive_pollen",
    "ozone",
    "particulate_matter_10um",
    "particulate_matter_2.5um",
    "peroxyacyl_nitrates",
    "pm10_wildfires",
    "ragweed_pollen",
    "secondary_inorganic_aerosol",
    "sulphur_dioxide",
]

opts: MakeDefinitionsOptions = MakeDefinitionsOptions(
    area="eu",
    file_format="netcdf",
    multilevel_vars=VariableSelection(
        slow={v: [v] for v in VARIABLES},
        fast={v: [v] for v in VARIABLES},
        hours=[str(x) for x in range(0, 97)],
    ),
    multilevel_levels=["0", "1000", "2000", "250", "3000", "50", "500", "5000"],
    partitions=cams_eu_partitions,
    client=Client(),
)

defs: MakeDefinitionsOutputs = make_definitions(opts=opts)

cams_eu_raw_archive = defs.raw_asset
