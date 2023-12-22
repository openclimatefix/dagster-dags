from cdsapi import Client

from local_archives.partitions import InitTimePartitionsDefinition

from ._definitions_factory import (
    MakeDefinitionsOptions,
    MakeDefinitionsOutputs,
    VariableSelection,
    make_definitions,
)

cams_global_partitions: InitTimePartitionsDefinition = InitTimePartitionsDefinition(
    start="2015-01-01",
    init_times=["00:00", "12:00"],
)

singlelevel_fast_vars: list[str] = [
    "ammonium_aerosol_optical_depth_550nm",
    "black_carbon_aerosol_optical_depth_550nm",
    "dust_aerosol_optical_depth_550nm",
    "nitrate_aerosol_optical_depth_550nm",
    "organic_matter_aerosol_optical_depth_550nm",
    "particulate_matter_10um",
    "particulate_matter_1um",
    "particulate_matter_2.5um",
    "sea_salt_aerosol_optical_depth_550nm",
    "secondary_organic_aerosol_optical_depth_550nm",
    "sulphate_aerosol_optical_depth_550nm",
    "total_aerosol_optical_depth_1240nm",
    "total_aerosol_optical_depth_469nm",
    "total_aerosol_optical_depth_550nm",
    "total_aerosol_optical_depth_670nm",
    "total_aerosol_optical_depth_865nm",
    "total_column_carbon_monoxide",
    "total_column_chlorine_monoxide",
    "total_column_chlorine_nitrate",
    "total_column_ethane",
    "total_column_formaldehyde",
    "total_column_hydrogen_chloride",
    "total_column_hydrogen_cyanide",
    "total_column_hydrogen_peroxide",
    "total_column_hydroxyl_radical",
    "total_column_isoprene",
    "total_column_methane",
    "total_column_nitric_acid",
    "total_column_nitrogen_dioxide",
    "total_column_nitrogen_monoxide",
    "total_column_ozone",
    "total_column_peroxyacetyl_nitrate",
    "total_column_propane",
    "total_column_sulphur_dioxide",
]

multilevel_fast_vars: list[str] = [
    "ammonium_aerosol_mass_mixing_ratio",
    "anthropogenic_secondary_organic_aerosol_mass_mixing_ratio",
    "biogenic_secondary_organic_aerosol_mass_mixing_ratio",
    "carbon_monoxide",
    "chlorine_monoxide",
    "chlorine_nitrate",
    "dust_aerosol_0.03-0.55um_mixing_ratio",
    "dust_aerosol_0.55-0.9um_mixing_ratio",
    "dust_aerosol_0.9-20um_mixing_ratio",
    "ethane",
    "formaldehyde",
    "hydrogen_chloride",
    "hydrogen_cyanide",
    "hydrogen_peroxide",
    "hydrophilic_black_carbon_aerosol_mixing_ratio",
    "hydrophilic_organic_matter_aerosol_mixing_ratio",
    "hydrophobic_black_carbon_aerosol_mixing_ratio",
    "hydrophobic_organic_matter_aerosol_mixing_ratio",
    "hydroxyl_radical",
    "isoprene",
    "methane",
    "nitrate_coarse_mode_aerosol_mass_mixing_ratio",
    "nitrate_fine_mode_aerosol_mass_mixing_ratio",
    "nitric_acid",
    "nitrogen_dioxide",
    "nitrogen_monoxide",
    "ozone",
    "peroxyacetyl_nitrate",
    "propane",
    "sea_salt_aerosol_0.03-0.5um_mixing_ratio",
    "sea_salt_aerosol_0.5-5um_mixing_ratio",
    "sea_salt_aerosol_5-20um_mixing_ratio",
    "specific_humidity",
    "sulphate_aerosol_mixing_ratio",
    "sulphur_dioxide",
]

multilevel_hours: list[str] = [str(x) for x in range(0, 121, 3)]

singlelevel_hours: list[str] = [str(x) for x in range(0, 121)]

# It is faster to download all variables in a group than to download them individually
# as then you are queuing fewer requests to the CDS API, for tape variables.
# Each group here have been checked in the ADS app to ensure they do not exceed
# the limit of 10000 items per request, when paired with downloading every step
# and init time for a single day.
singlelevel_slow_var_groups: dict[str, list[str]] = {
    "asymmetry_factor_340-2130nm": [
        "asymmetry_factor_340nm",
        "asymmetry_factor_355nm",
        "asymmetry_factor_380nm",
        "asymmetry_factor_400nm",
        "asymmetry_factor_440nm",
        "asymmetry_factor_469nm",
        "asymmetry_factor_500nm",
        "asymmetry_factor_532nm",
        "asymmetry_factor_550nm",
        "asymmetry_factor_645nm",
        "asymmetry_factor_670nm",
        "asymmetry_factor_800nm",
        "asymmetry_factor_858nm",
        "asymmetry_factor_865nm",
        "asymmetry_factor_1020nm",
        "asymmetry_factor_1064nm",
        "asymmetry_factor_1240nm",
        "asymmetry_factor_1640nm",
        "asymmetry_factor_2130nm",
    ],
    "single_scattering_albedo_340-2130nm": [
        "single_scattering_albedo_340nm",
        "single_scattering_albedo_355nm",
        "single_scattering_albedo_380nm",
        "single_scattering_albedo_400nm",
        "single_scattering_albedo_440nm",
        "single_scattering_albedo_469nm",
        "single_scattering_albedo_500nm",
        "single_scattering_albedo_532nm",
        "single_scattering_albedo_550nm",
        "single_scattering_albedo_645nm",
        "single_scattering_albedo_670nm",
        "single_scattering_albedo_800nm",
        "single_scattering_albedo_858nm",
        "single_scattering_albedo_865nm",
        "single_scattering_albedo_1020nm",
        "single_scattering_albedo_1064nm",
        "single_scattering_albedo_1240nm",
        "single_scattering_albedo_1640nm",
        "single_scattering_albedo_2130nm",
    ],
    "total_aerosol_optical_depth_340-2130nm": [
        "total_aerosol_optical_depth_340nm",
        "total_aerosol_optical_depth_355nm",
        "total_aerosol_optical_depth_380nm",
        "total_aerosol_optical_depth_400nm",
        "total_aerosol_optical_depth_440nm",
        "total_aerosol_optical_depth_500nm",
        "total_aerosol_optical_depth_532nm",
        "total_aerosol_optical_depth_645nm",
        "total_aerosol_optical_depth_800nm",
        "total_aerosol_optical_depth_858nm",
        "total_aerosol_optical_depth_1020nm",
        "total_aerosol_optical_depth_1064nm",
        "total_aerosol_optical_depth_1640nm",
        "total_aerosol_optical_depth_2130nm",
    ],
    "total_absorption_aerosol_optical_depth_340-2130nm": [
        "total_absorption_aerosol_optical_depth_340nm",
        "total_absorption_aerosol_optical_depth_355nm",
        "total_absorption_aerosol_optical_depth_380nm",
        "total_absorption_aerosol_optical_depth_400nm",
        "total_absorption_aerosol_optical_depth_440nm",
        "total_absorption_aerosol_optical_depth_469nm",
        "total_absorption_aerosol_optical_depth_500nm",
        "total_absorption_aerosol_optical_depth_532nm",
        "total_absorption_aerosol_optical_depth_550nm",
        "total_absorption_aerosol_optical_depth_645nm",
        "total_absorption_aerosol_optical_depth_670nm",
        "total_absorption_aerosol_optical_depth_800nm",
        "total_absorption_aerosol_optical_depth_858nm",
        "total_absorption_aerosol_optical_depth_865nm",
        "total_absorption_aerosol_optical_depth_1020nm",
        "total_absorption_aerosol_optical_depth_1064nm",
        "total_absorption_aerosol_optical_depth_1240nm",
        "total_absorption_aerosol_optical_depth_1640nm",
        "total_absorption_aerosol_optical_depth_2130nm",
    ],
    "total_fine_mode_aerosol_optical_depth_340-2130nm": [
        "total_fine_mode_aerosol_optical_depth_340nm",
        "total_fine_mode_aerosol_optical_depth_355nm",
        "total_fine_mode_aerosol_optical_depth_380nm",
        "total_fine_mode_aerosol_optical_depth_400nm",
        "total_fine_mode_aerosol_optical_depth_440nm",
        "total_fine_mode_aerosol_optical_depth_469nm",
        "total_fine_mode_aerosol_optical_depth_500nm",
        "total_fine_mode_aerosol_optical_depth_532nm",
        "total_fine_mode_aerosol_optical_depth_550nm",
        "total_fine_mode_aerosol_optical_depth_645nm",
        "total_fine_mode_aerosol_optical_depth_670nm",
        "total_fine_mode_aerosol_optical_depth_800nm",
        "total_fine_mode_aerosol_optical_depth_858nm",
        "total_fine_mode_aerosol_optical_depth_865nm",
        "total_fine_mode_aerosol_optical_depth_1020nm",
        "total_fine_mode_aerosol_optical_depth_1064nm",
        "total_fine_mode_aerosol_optical_depth_1240nm",
        "total_fine_mode_aerosol_optical_depth_1640nm",
        "total_fine_mode_aerosol_optical_depth_2130nm",
    ],
    "dust_aerosol_optical_depth_550nm_0.04-20um": [
        "dust_aerosol_0.03-0.55um_optical_depth_550nm",
        "dust_aerosol_0.55-9um_optical_depth_550nm",
        "dust_aerosol_9-20um_optical_depth_550nm",
    ],
    "sea_salt_aerosol_optical_depth_550nm_0.03-20um": [
        "sea_salt_aerosol_0.03-0.5um_optical_depth_550nm",
        "sea_salt_aerosol_0.5-5um_optical_depth_550nm",
        "sea_salt_aerosol_5-20um_optical_depth_550nm",
    ],
    "nitrate_aerosol_optical_depth_550nm_coarse-fine": [
        "nitrate_coarse_mode_aerosol_optical_depth_550nm",
        "nitrate_fine_mode_aerosol_optical_depth_550nm",
    ],
    "hydrophilic_aerosol_optical_depth_550nm_bc-om": [
        "hydrophilic_black_carbon_aerosol_optical_depth_550nm",
        "hydrophilic_organic_matter_aerosol_optical_depth_550nm",
    ],
    "hydrophobic_aerosol_optical_depth_550nm_bc-om": [
        "hydrophobic_black_carbon_aerosol_optical_depth_550nm",
        "hydrophobic_organic_matter_aerosol_optical_depth_550nm",
    ],
}

singlelevel_slow_var_groups_subset: dict[str, list[str]] = {
    "total_aerosol_optical_depth_400-645nm": [
        "total_aerosol_optical_depth_400nm",
        "total_aerosol_optical_depth_440nm",
        "total_aerosol_optical_depth_500nm",
        "total_aerosol_optical_depth_532nm",
        "total_aerosol_optical_depth_645nm",
    ],
}

# Due to pulling every pressure level, these need to be pulled one at a time
# to avoid exceeding the 10000 item limit per request.
multilevel_slow_vars: list[str] = [
    "aerosol_extinction_coefficient_1064nm",
    "aerosol_extinction_coefficient_355nm",
    "aerosol_extinction_coefficient_532nm",
    "attenuated_backscatter_due_to_aerosol_1064nm_from_ground",
    "attenuated_backscatter_due_to_aerosol_1064nm_from_top_of_atmosphere",
    "attenuated_backscatter_due_to_aerosol_355nm_from_ground",
    "attenuated_backscatter_due_to_aerosol_355nm_from_top_of_atmosphere",
    "attenuated_backscatter_due_to_aerosol_532nm_from_ground",
    "attenuated_backscatter_due_to_aerosol_532nm_from_top_of_atmosphere",
]

multilevel_slow_vars_subset = ["aerosol_extinction_coefficient_532nm"]

multilevel_levels: list[str] = [
    "1",
    "2",
    "3",
    "5",
    "7",
    "10",
    "20",
    "30",
    "50",
    "70",
    "100",
    "150",
    "200",
    "250",
    "300",
    "400",
    "500",
    "600",
    "700",
    "800",
    "850",
    "900",
    "925",
    "950",
    "1000",
]

opts: MakeDefinitionsOptions = MakeDefinitionsOptions(
    area="global",
    file_format="grib",
    multilevel_vars=VariableSelection(
        slow={d: [d] for d in multilevel_slow_vars_subset},
        fast={d: [d] for d in multilevel_fast_vars},
        hours=multilevel_hours,
    ),
    multilevel_levels=multilevel_levels,
    singlelevel_vars=VariableSelection(
        slow=singlelevel_slow_var_groups_subset,
        fast={d: [d] for d in singlelevel_fast_vars},
        hours=singlelevel_hours,
    ),
    partitions=cams_global_partitions,
    client=Client(),
)

defs: MakeDefinitionsOutputs = make_definitions(opts=opts)

cams_global_raw_archive = defs.raw_asset
