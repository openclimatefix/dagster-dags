import dagster as dg

air_assets: list[dg.AssetsDefinition] = dg.load_assets_from_package_name(
    package_name="air",
    group_name="air",
    key_prefix=["air"],
)

